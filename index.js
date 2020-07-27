const HypercoreProtocol = require('hypercore-protocol')
const Nanoresource = require('nanoresource/emitter')
const hypercore = require('hypercore')
const hypercoreCrypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')
const maybe = require('call-me-maybe')

const RefPool = require('refpool')
const deriveSeed = require('derive-key')
const derivedStorage = require('derived-key-storage')
const raf = require('random-access-file')

const MASTER_KEY_FILENAME = 'master_key'
const NAMESPACE = 'corestore'
const NAMESPACE_SEPERATOR = ':'

class InnerCorestore extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new Error('Storage should be a function or string')
    this.storage = storage

    this.opts = opts

    this._replicationStreams = []
    this.cache = new RefPool({
      maxSize: opts.cacheSize || 1000,
      close: core => {
        core.close(err => {
          if (err) this.emit('error', err)
        })
      }
    })

    // Generated in _open
    this._masterKey = opts.masterKey || null
    this._id = hypercoreCrypto.randomBytes(8)
  }

  // Nanoresource Methods

  _open (cb) {
    if (this._masterKey) return cb()
    const keyStorage = this.storage(MASTER_KEY_FILENAME)
    keyStorage.read(0, 32, (err, key) => {
      if (err) {
        this._masterKey = hypercoreCrypto.randomBytes(32)
        return keyStorage.write(0, this._masterKey, err => {
          if (err) return cb(err)
          keyStorage.close(cb)
        })
      }
      this._masterKey = key
      keyStorage.close(cb)
    })
  }

  _close (cb) {
    let error = null
    for (const { stream } of this._replicationStreams) {
      stream.destroy()
    }
    if (!this.cache.size) return process.nextTick(cb, null)
    let remaining = this.cache.size
    for (const { value: core } of this.cache.entries.values()) {
      core.close(err => {
        if (err) error = err
        if (!--remaining) {
          if (error) return cb(error)
          return cb(null)
        }
      })
    }
  }

  // Private Methods

  _checkIfExists (dkey, cb) {
    dkey = encodeKey(dkey)
    if (this.cache.has(dkey)) return process.nextTick(cb, null, true)

    const coreStorage = this.storage([dkey.slice(0, 2), dkey.slice(2, 4), dkey, 'key'].join('/'))

    coreStorage.read(0, 32, (err, key) => {
      if (err) return cb(err)
      coreStorage.close(err => {
        if (err) return cb(err)
        if (!key) return cb(null, false)
        return cb(null, true)
      })
    })
  }

  _injectIntoReplicationStreams (core) {
    for (const { stream, opts } of this._replicationStreams) {
      this._replicateCore(false, core, stream, { ...opts })
    }
  }

  _replicateCore (isInitiator, core, mainStream, opts) {
    if (!core) return
    core.ready(function (err) {
      if (err) return
      core.replicate(isInitiator, {
        ...opts,
        stream: mainStream
      })
    })
  }

  _deriveSecret (namespace, name) {
    return deriveSeed(namespace, this._masterKey, name)
  }

  _generateKeyPair (name) {
    if (typeof name === 'string') name = Buffer.from(name)
    else if (!name) name = hypercoreCrypto.randomBytes(32)

    const seed = this._deriveSecret(NAMESPACE, name)

    const keyPair = hypercoreCrypto.keyPair(seed)
    const discoveryKey = hypercoreCrypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys (coreOpts) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }

    if (coreOpts.keyPair) {
      const publicKey = coreOpts.keyPair.publicKey
      const secretKey = coreOpts.keyPair.secretKey
      return {
        publicKey,
        secretKey,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (coreOpts.key) {
      const publicKey = decodeKey(coreOpts.key)
      return {
        publicKey,
        secretKey: null,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (coreOpts.default || coreOpts.name) {
      if (!coreOpts.name) throw new Error('If the default option is set, a name must be specified.')
      return this._generateKeyPair(coreOpts.name)
    }
    if (coreOpts.discoveryKey) {
      const discoveryKey = decodeKey(coreOpts.discoveryKey)
      return {
        publicKey: null,
        secretKey: null,
        discoveryKey,
        name: null
      }
    }
    return this._generateKeyPair(null)
  }

  // Public Methods

  isLoaded (coreOpts) {
    const generatedKeys = this._generateKeys(coreOpts)
    return this.cache.has(encodeKey(generatedKeys.discoveryKey))
  }

  isExternal (coreOpts) {
    const generatedKeys = this._generateKeys(coreOpts)
    const entry = this._cache.entry(encodeKey(generatedKeys.discoveryKey))
    if (!entry) return false
    return entry.refs !== 0
  }

  get (coreOpts = {}) {
    if (!this.opened) throw new Error('Corestore.ready must be called before get.')
    const self = this

    const generatedKeys = this._generateKeys(coreOpts)
    const { publicKey, discoveryKey, secretKey } = generatedKeys
    const id = encodeKey(discoveryKey)

    const cached = this.cache.get(id)
    if (cached) return cached

    const storageRoot = [id.slice(0, 2), id.slice(2, 4), id].join('/')

    const keyStorage = derivedStorage(createStorage, (name, cb) => {
      if (name) {
        const res = this._generateKeyPair(name)
        if (discoveryKey && (!discoveryKey.equals((res.discoveryKey)))) {
          return cb(new Error('Stored an incorrect name.'))
        }
        return cb(null, res)
      }
      if (secretKey) return cb(null, generatedKeys)
      if (publicKey) return cb(null, { name: null, publicKey, secretKey: null })
      const err = new Error('Unknown key pair.')
      err.unknownKeyPair = true
      return cb(err)
    })

    const cacheOpts = { ...this.opts.cache }
    if (coreOpts.cache) {
      if (coreOpts.cache.data === false) delete cacheOpts.data
      if (coreOpts.cache.tree === false) delete cacheOpts.tree
    }
    if (cacheOpts.data) cacheOpts.data = cacheOpts.data.namespace()
    if (cacheOpts.tree) cacheOpts.tree = cacheOpts.tree.namespace()

    if (!coreOpts.onwrite && this.opts.onwrite) {
      coreOpts.onwrite = function (index, data, peer, cb) {
        self.opts.onwrite(publicKey, index, data, peer, cb)
      }
    }

    const core = hypercore(name => {
      if (name === 'key') return keyStorage.key
      if (name === 'secret_key') return keyStorage.secretKey
      return createStorage(name)
    }, publicKey, {
      ...this.opts,
      ...coreOpts,
      cache: cacheOpts,
      createIfMissing: !!publicKey
    })

    this.cache.set(id, core)
    core.ifAvailable.wait()

    var errored = false
    core.once('error', onerror)
    core.once('ready', onready)
    core.once('close', onclose)

    return core

    function onready () {
      if (errored) return
      self.emit('feed', core, coreOpts)
      core.removeListener('error', onerror)
      self._injectIntoReplicationStreams(core)
      // TODO: nexttick here needed? prob not, just legacy
      process.nextTick(() => core.ifAvailable.continue())
    }

    function onerror (err) {
      errored = true
      core.ifAvailable.continue()
      self.cache.delete(id)
      if (err.unknownKeyPair) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        // TODO: This should not throw, but should propagate somehow.
      }
    }

    function onclose () {
      self.cache.delete(id)
    }

    function createStorage (name) {
      return self.storage(storageRoot + '/' + name)
    }
  }

  replicate (isInitiator, cores, replicationOpts = {}) {
    const self = this

    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = replicationOpts.stream || new HypercoreProtocol(isInitiator, { ...finalOpts })
    var closed = false

    for (const core of cores) {
      this._replicateCore(isInitiator, core, mainStream, { ...finalOpts })
    }

    mainStream.on('discovery-key', ondiscoverykey)
    mainStream.on('finish', onclose)
    mainStream.on('end', onclose)
    mainStream.on('close', onclose)

    const streamState = { stream: mainStream, opts: finalOpts }
    this._replicationStreams.push(streamState)

    return mainStream

    function ondiscoverykey (dkey) {
      // Get will automatically add the core to all replication streams.
      self._checkIfExists(dkey, (err, exists) => {
        if (closed) return
        if (err || !exists) return mainStream.close(dkey)
        const passiveCore = self.get({ discoveryKey: dkey })
        self._replicateCore(false, passiveCore, mainStream, { ...finalOpts })
      })
    }

    function onclose () {
      if (!closed) {
        self._replicationStreams.splice(self._replicationStreams.indexOf(streamState), 1)
        closed = true
      }
    }
  }
}

class Corestore extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    this.storage = storage
    this.name = opts.name || 'default'
    this.inner = opts.inner || new InnerCorestore(storage, opts)
    this.cache = this.inner.cache
    this.store = this // Backwards-compat for NamespacedCorestore

    this._isNamespaced = !!opts.name
    this._isTopLevel = !opts.inner
    this._openedCores = new Map()

    const onfeed = feed => this.emit('feed', feed)
    const onerror = err => this.emit('error', err)
    this.inner.on('feed', onfeed)
    this.inner.on('error', onerror)
    this._unlisten = () => {
      this.inner.removeListener('feed', onfeed)
      this.inner.removeListener('error', onerror)
    }
  }

  ready (cb) {
    return maybe(cb, new Promise((resolve, reject) => {
      this.open(err => {
        if (err) return reject(err)
        return resolve()
      })
    }))
  }

  // Nanoresource Methods

  _open (cb) {
    return this.inner.open(cb)
  }

  _close (cb) {
    this._unlisten()
    if (this._isTopLevel) return this.inner.close(cb)
    for (const dkey of this._openedCores) {
      this.cache.decrement(dkey)
    }
    return process.nextTick(cb, null)
  }

  // Private Methods

  _maybeIncrement (core) {
    const id = encodeKey(core.discoveryKey)
    if (this._openedCores.has(id)) return
    this._openedCores.set(id, core)
    this.cache.increment(id)
  }

  // Public Methods

  get (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    const core = this.inner.get(coreOpts)
    this._maybeIncrement(core)
    return core
  }

  default (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    return this.get({ ...coreOpts, name: this.name })
  }

  namespace (name) {
    if (!name) name = hypercoreCrypto.randomBytes(32)
    if (Buffer.isBuffer(name)) name = name.toString('hex')
    name = this._isNamespaced ? this.name + NAMESPACE_SEPERATOR + name : name
    return new Corestore(this.storage, {
      inner: this.inner,
      name
    })
  }

  replicate (isInitiator, opts) {
    return this.inner.replicate(isInitiator, this._openedCores.values(), opts)
  }

  isLoaded (coreOpts) {
    return this.inner.isLoaded(coreOpts)
  }

  isExternal (coreOpts) {
    return this.inner.isExternal(coreOpts)
  }

  list () {
    return new Map([...this._openedCores])
  }
}

function encodeKey (key) {
  return Buffer.isBuffer(key) ? datEncoding.encode(key) : key
}

function decodeKey (key) {
  return (typeof key === 'string') ? datEncoding.decode(key) : key
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name.endsWith('/bitfield') ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, { directory: dir, lock: lock })
  }
}

module.exports = Corestore
