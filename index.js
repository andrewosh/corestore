const { EventEmitter } = require('events')

const HypercoreProtocol = require('hypercore-protocol')
const Nanoguard = require('nanoguard')
const hypercore = require('hypercore')
const hypercoreCrypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')

const thunky = require('thunky')
const LRU = require('lru')
const deriveSeed = require('derive-key')
const derivedStorage = require('derived-key-storage')
const maybe = require('call-me-maybe')
const raf = require('random-access-file')

const MASTER_KEY_FILENAME = 'master_key'
const NAMESPACE = 'corestore'

class NamespacedCorestore {
  constructor (corestore, name) {
    this.name = name
    this.store = corestore
    this._opened = new Set()
    this.ready = this.store.ready.bind(this.store)
  }

  _processCore (core) {
    if (!this._opened.has(core)) {
      this.store._incrementReference(core)
      this._opened.add(core)
      core.once('close', () => {
        this._opened.delete(core)
      })
    }
    return core
  }

  default (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    const core = this.store.default({ ...coreOpts, _name: this.name, namespaced: true })
    return this._processCore(core)
  }

  get (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    const core = this.store.get({ ...coreOpts, namespaced: true })
    return this._processCore(core)
  }

  replicate (discoveryKey, opts) {
    // TODO: This should only replicate the cores in this._opened.
    return this.store.replicate(discoveryKey, { ...opts, cores: this._opened })
  }

  close (cb) {
    const self = this
    let pending = this._opened.size + 1
    let error = null

    for (const core of this._opened) {
      const ref = this.store._decrementReference(core)
      if (ref) {
        onclose(null)
        continue
      }
      core.close(onclose)
    }
    this.store._namespaces.delete(this.name.toString('hex'))
    onclose(null)

    function onclose (err) {
      if (err) error = err
      if (!--pending) {
        self._opened.clear()
        return cb(error)
      }
    }
  }

  destroy (cb) {
    // Get a copy of current cores to not lose references
    const currentFeeds = [...this._opened.values()]
    let pending = currentFeeds.length + 1
    let error = null

    this.close(function (err) {
      if (err) return cb(err)

      for (const core of currentFeeds) {
        core.destroy(ondestroy)
      }
      ondestroy()

      function ondestroy (err) {
        if (err) error = err
        if (!--pending) cb(error)
      }
    })
  }
}

class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new Error('Storage should be a function or string')
    this.storage = storage
    this.name = opts.name || Buffer.from('default')
    this.guard = new Nanoguard()

    this.opts = opts

    this._replicationStreams = []
    this._namespaces = new Map()
    this._references = new Map()
    this._externalCores = new Map()
    this._internalCores = new LRU(opts.cacheSize || 1000)
    this._internalCores.on('evict', ({ key, value: core }) => {
      if (this._externalCores.get(key)) return
      core.close(err => {
        if (err) this.emit('error', err)
      })
    })

    // Generated in _ready
    this._masterKey = null
    this._isReady = false
    this._readyCallback = thunky(this._ready.bind(this))

    this._id = hypercoreCrypto.randomBytes(8)
  }

  _info () {
    return {
      id: this._id.toString('hex'),
      externalCoresSize: this._externalCores.size,
      internalCoresSize: this._internalCores.length,
      referencesSize: this._references.size
    }
  }

  ready (cb) {
    return maybe(cb, new Promise((resolve, reject) => {
      this._readyCallback(err => {
        if (err) return reject(err)
        this._isReady = true
        return resolve()
      })
    }))
  }

  _ready (cb) {
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

  _checkIfExists (dkey, cb) {
    const cachedCore = this._getCachedCore(dkey, false)
    if (cachedCore) return process.nextTick(cb, null, true)

    const id = datEncoding.encode(dkey)
    const coreStorage = this.storage([id.slice(0, 2), id.slice(2, 4), id, 'key'].join('/'))

    coreStorage.read(0, 32, (err, key) => {
      if (err) return cb(err)
      coreStorage.close(err => {
        if (err) return cb(err)
        if (!key) return cb(null, false)
        return cb(null, true)
      })
    })
  }

  _incrementReference (core) {
    const updated = (this._references.get(core) || 0) + 1
    this._references.set(core, updated)
    return updated
  }

  _decrementReference (core) {
    const refs = this._references.get(core)
    const updated = refs - 1
    if (updated < 0) throw new Error('References should not go negative.')
    this._references.set(core, updated)
    if (!updated) this._references.delete(core)
    return updated
  }

  _removeReferences (core) {
    this._references.delete(core)
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

  _getCacheIndex (coreOpts) {
    if (coreOpts.default) return 'default'
    else if (coreOpts.discoveryKey) return encodeKey(coreOpts.discoveryKey)
    else if (coreOpts.key) {
      const key = decodeKey(coreOpts.key)
      return hypercoreCrypto.discoveryKey(key)
    }
    return null
  }

  _getCachedCore (discoveryKey, shouldBeExternal) {
    const idx = encodeKey(discoveryKey)
    let core = this._externalCores.get(idx)

    if (core) return core

    core = this._internalCores.get(idx)

    if (shouldBeExternal && core) {
      this._externalCores.set(idx, core)
      this._injectIntoReplicationStreams(core)
    }

    return core
  }

  _cacheCore (core, discoveryKey, opts) {
    const idx = encodeKey(discoveryKey)
    if (opts && opts.external) this._externalCores.set(idx, core)
    this._internalCores.set(idx, core)
  }

  _uncacheCore (core, discoveryKey) {
    const idx = encodeKey(discoveryKey)
    const internalCached = this._internalCores.get(idx)
    if (internalCached !== core) return
    this._internalCores.remove(idx)
    this._externalCores.delete(idx)
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
    if (coreOpts.default) {
      const name = coreOpts._name || this.name
      return this._generateKeyPair(name)
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

  default (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    return this.get({
      ...coreOpts,
      default: true
    })
  }

  namespace (name) {
    if (!name) name = hypercoreCrypto.randomBytes(32)
    const ns = new NamespacedCorestore(this, name)
    this._namespaces.set(name.toString('hex'), ns)
    return ns
  }

  get (coreOpts = {}) {
    if (!this._isReady) throw new Error('Corestore.ready must be called before get.')
    const self = this

    const generatedKeys = this._generateKeys(coreOpts)
    const { publicKey, discoveryKey, secretKey } = generatedKeys
    const id = encodeKey(discoveryKey)
    const isExternal = !!publicKey

    const cached = this._getCachedCore(discoveryKey, isExternal)
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
    core.ifAvailable.depend(this.guard)

    this._cacheCore(core, discoveryKey, { external: isExternal })
    if (!coreOpts.namespaced) this._incrementReference(core)
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
      self._removeReferences(core)
      self._uncacheCore(core, discoveryKey)
      if (err.unknownKeyPair) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        // TODO: This should not throw, but should propagate somehow.
      }
    }

    function onclose () {
      self._uncacheCore(core, discoveryKey)
    }

    function createStorage (name) {
      return self.storage(storageRoot + '/' + name)
    }
  }

  replicate (isInitiator, replicationOpts = {}) {
    const self = this

    const finalOpts = { ...this.opts, ...replicationOpts, ack: true }
    const mainStream = replicationOpts.stream || new HypercoreProtocol(isInitiator, { ...finalOpts })
    var closed = false

    const cores = replicationOpts.cores || this._externalCores.values()
    for (const activeCore of cores) {
      this._replicateCore(isInitiator, activeCore, mainStream, { ...finalOpts })
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

  close (cb) {
    const self = this
    var remaining = this._externalCores.size + this._internalCores.length + 1
    var closing = false
    var error = null

    for (const { stream } of this._replicationStreams) {
      stream.destroy()
    }
    for (const core of this._externalCores.values()) {
      if (!core.closed) core.close(onclose)
      else onclose(null)
    }
    for (const idx of this._internalCores.keys) {
      const core = this._internalCores.get(idx)
      if (!core.closed) core.close(onclose)
      else onclose(null)
    }
    onclose()

    function onclose (err) {
      if (err) error = err
      if (!--remaining) return reset(error)
    }

    function reset (err) {
      if (err) error = err
      if (closing) return
      closing = true
      self._externalCores.clear()
      self._internalCores.clear()
      self._replicationStreams = []
      return cb(err)
    }
  }

  destroy (cb) {
    // TODO: Destroy master key
    const self = this
    const internalCores = this._internalCores.keys.map(function (key) {
      return self._internalCores.get(key)
    })
    const cores = [...this._externalCores.values(), ...internalCores]
    let remaining = cores.length + 1
    let error = null

    this.close(function (err) {
      if (err) return cb(err)

      for (const core of cores) {
        core.destroy(ondestroy)
      }
      ondestroy()

      function ondestroy (err) {
        if (err) error = err
        if (!--remaining) return cb(error)
      }
    })
  }

  list () {
    return new Map([...this._externalCores])
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
