const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const protocol = require('hypercore-protocol')
const datEncoding = require('dat-encoding')
const { EventEmitter } = require('events')

module.exports = (storage, opts) => new Corestore(storage, opts)

class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()
    this.storage = storage
    this.opts = opts

    this.cores = new Map()
    this.replicationStreams = []
    this.defaultCore = null

    this.storeId = crypto.randomBytes(5)
  }

  _optsToIndex (coreOpts) {
    if (coreOpts.default && !coreOpts.key && !coreOpts.secretKey) {
      var idx = 'default'
    } else if (coreOpts.name && !coreOpts.key && !coreOpts.secretKey) {
      var idx = coreOpts.name
    } else {
      idx = coreOpts.key || coreOpts.discoveryKey
      if (!idx) {
        // If no key was specified, then we generate a new keypair or use the one that was passed in.
        var { publicKey, secretKey } = coreOpts.keyPair || crypto.keyPair()
        idx = crypto.discoveryKey(publicKey)
      } else {
        if (!(idx instanceof Buffer)) idx = datEncoding.decode(idx)
        if (coreOpts.key) {
          idx = crypto.discoveryKey(idx)
        }
      }
    }
    return { idx, key: coreOpts.key || publicKey, secretKey }
  }

  _replicateCore (core, mainStream, opts) {
    const self = this

    core.ready(function (err) {
      if (err) return
      // if (mainStream.has(core.key)) return
      for (const feed of mainStream.feeds) { // TODO: expose mainStream.has(key) instead
        if (feed.peer.feed === core) return
      }
      core.replicate({
        ...opts,
        stream: mainStream
      })
    })
  }

  isDefaultSet () {
    return !!this.defaultCore
  }

  getInfo () {
    return {
      id: this.storeId,
      defaultKey: this.defaultCore && this.defaultCore.key,
      discoveryKey: this.defaultCore && this.defaultCore.discoveryKey,
      replicationId: this.defaultCore.id,
      cores: this.cores
    }
  }

  default (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    if (this.defaultCore) return this.defaultCore

    if (!coreOpts.keyPair && !coreOpts.key) coreOpts.default = true

    this.defaultCore = this.get(coreOpts)
    return this.defaultCore
  }

  get (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    const self = this

    const { idx, key, secretKey } = this._optsToIndex(coreOpts)

    const idxString = (idx instanceof Buffer) ? datEncoding.encode(idx) : idx
    const existing = this.cores.get(idxString)
    if (existing) {
      injectIntoReplicationStreams(existing)
      return existing
    }

    var storageRoot = idxString
    if (idxString !== 'default' && !coreOpts.name) {
      storageRoot = [storageRoot.slice(0, 2), storageRoot.slice(2, 4), storageRoot].join('/')
    }

    var core = hypercore(filename => this.storage(storageRoot + '/' + filename), key, {
      ...this.opts,
      ...coreOpts,
      createIfMissing: !coreOpts.discoveryKey,
      secretKey: coreOpts.secretKey || secretKey
    })

    var errored = false
    const errorListener = err => {
      if (coreOpts.discoveryKey) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        errored = true
        return
      }
    }
    core.once('error', errorListener)
    core.once('ready', () => {
      if (errored) return
      this.emit('feed', core, coreOpts)
      core.removeListener('error', errorListener)
      this.cores.set(datEncoding.encode(core.key), core)
      this.cores.set(datEncoding.encode(core.discoveryKey), core)
      injectIntoReplicationStreams(core)
    })

    return core

    function injectIntoReplicationStreams (core) {
      for (let { stream, opts }  of self.replicationStreams) {
        self._replicateCore(core, stream, { ...opts })
      }
    }
  }

  replicate (discoveryKey, replicationOpts) {
    if (discoveryKey && (!(discoveryKey instanceof Buffer) && (typeof discoveryKey !== 'string'))) return this.replicate(null, discoveryKey)
    const self = this

    if (!this.defaultCore && replicationOpts.encrypt) throw new Error('A main core must be specified before replication.')

    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = this.defaultCore
      ? this.defaultCore.replicate({ ...finalOpts })
      : protocol({ ...finalOpts })

    var closed = false

    for (let [_, core] of this.cores) {
      this._replicateCore(core, mainStream, { ...finalOpts })
    }

    mainStream.on('feed', dkey => {
      // Get will automatically add the core to all replication streams.
      this.get({ discoveryKey: dkey })
    })

    mainStream.on('finish', onclose)
    mainStream.on('end', onclose)
    mainStream.on('close', onclose)

    let streamState = { stream: mainStream, opts: finalOpts }
    this.replicationStreams.push(streamState)

    return mainStream

    function onclose () {
      if (!closed) {
        self.replicationStreams.splice(self.replicationStreams.indexOf(streamState), 1)
        closed = true
      }
    }
  }

  close (cb) {
    const self = this
    var remaining = this.cores.size

    for (let { stream } of this.replicationStreams) {
      stream.destroy()
    }
    for (let [, core] of this.cores) {
      if (!core.closed) core.close(onclose)
    }

    function onclose (err) {
      if (err) return reset(err)
      if (!--remaining) return reset(null)
    }

    function reset (err) {
      self.cores = new Map()
      self.replicationStreams = []
      return cb(err)
    }
  }

  list () {
    return new Map([...this.cores])
  }
}

module.exports.Corestore = Corestore
