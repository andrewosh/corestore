const p = require('path')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const datEncoding = require('dat-encoding')
const test = require('tape')

const Corestore = require('..')
const {
  runAll,
  validateCore,
  cleanup
} = require('./helpers')

test('ram-based corestore, different get options', async t => {
  const store1 = await create(ram)
  const core1 = store1.default()
  var core2, core3, core4, core5

  await runAll([
    cb => core1.ready(cb),
    cb => core1.append('hello', cb),
    cb => {
      // Buffer arg
      core2 = store1.get(core1.key)
      return core2.ready(cb)
    },
    cb => {
      // Object arg
      core3 = store1.get({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      // Discovery key option
      core4 = store1.get({ discoveryKey: core1.discoveryKey })
      return core4.ready(cb)
    },
    cb => {
      // String option
      core5 = store1.get({ key: datEncoding.encode(core1.key) })
      return core5.ready(cb)
    }
  ])

  t.same(core1, core2)
  t.same(core1, core3)
  t.same(core1, core4)
  t.same(core1, core5)
  t.end()
})

test('ram-based corestore, simple replication', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default(core1.key)
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      const stream = store1.replicate(true)
      stream.pipe(store2.replicate(false)).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])

  t.end()
})

test('ram-based corestore, replicating with different default keys', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default()
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core1.key })
      return core4.ready(cb)
    },
    cb => core1.append('cat', cb),
    cb => core1.append('dog', cb),
    cb => {
      const stream = store1.replicate(true)
      stream.pipe(store2.replicate(false)).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('ram-based corestore, sparse replication', async t => {
  const store1 = await create(ram, { sparse: true })
  const store2 = await create(ram, { sparse: true })
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      t.same(core2.sparse, true)
      t.same(core1.sparse, true)
      return process.nextTick(cb, null)
    },
    cb => {
      core3 = store2.default(core1.key)
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => {
      const stream = store1.replicate(true, { live: true })
      stream.pipe(store2.replicate(false, { live: true })).pipe(stream)
      return process.nextTick(cb, null)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      t.same(core3.length, 0)
      t.same(core4.length, 0)
      return process.nextTick(cb, null)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('ram-based corestore, sparse replication with different default keys', async t => {
  const store1 = await create(ram, { sparse: true })
  const store2 = await create(ram, { sparse: true })
  const core1 = store1.default()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => {
      core3 = store2.default()
      return core3.ready(cb)
    },
    cb => {
      const s1 = store1.replicate(true, { live: true })
      const s2 = store2.replicate(false, { live: true })
      s1.pipe(s2).pipe(s1)
      return process.nextTick(cb, null)
    },
    cb => core1.append('cat', cb),
    cb => core1.append('dog', cb),
    cb => {
      core4 = store2.get({ key: core1.key })
      return core4.ready(cb)
    },
    cb => {
      t.same(core4.length, 0)
      t.same(core1.length, 2)
      return process.nextTick(cb, null)
    }
  ])

  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('raf-based corestore, simple replication', async t => {
  const store1 = await create(path => raf(p.join('store1', path)))
  const store2 = await create(path => raf(p.join('store2', path)))
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      setImmediate(() => {
        const stream = store1.replicate(true)
        stream.pipe(store2.replicate(false)).pipe(stream)
        stream.on('end', cb)
      })
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  await cleanup(['store1', 'store2'])
  t.end()
})

test('raf-based corestore, close and reopen', async t => {
  var store = await create('test-store')
  var firstCore = store.default()
  var reopenedCore = null

  await runAll([
    cb => firstCore.ready(cb),
    cb => firstCore.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(firstCore.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      create('test-store').then(store => {
        reopenedCore = store.default()
        return reopenedCore.ready(cb)
      })
    }
  ])

  await validateCore(t, reopenedCore, [Buffer.from('hello')])
  await cleanup(['test-store'])
  t.end()
})

test('live replication with an additional core', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)

  const core1 = store1.default()
  var core2 = null
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => {
      core3 = store2.default({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      const stream = store1.replicate(true, { live: true })
      stream.pipe(store2.replicate(false, { live: true })).pipe(stream)
      return cb(null)
    },
    cb => {
      core2 = store1.get()
      return core2.ready(cb)
    },
    cb => {
      core4 = store2.get(core2.key)
      return core4.ready(cb)
    },
    cb => core2.append('hello', cb),
    cb => core2.append('world', cb)
  ])

  await validateCore(t, core4, [Buffer.from('hello'), Buffer.from('world')])
  t.end()
})

test('namespaced corestores use separate default keys', async t => {
  const store1 = await create(ram)
  const store2 = store1.namespace('store2')
  const store3 = store1.namespace('store3')

  await store2.ready()
  await store3.ready()

  const feed1 = store2.default()
  const feed2 = store3.default()

  t.true(!feed1.key.equals(feed2.key))

  t.end()
})

test('namespaced corestores will not increment reference multiple times', async t => {
  const store1 = await create(ram)
  const store2 = store1.namespace('store2')
  const store3 = store1.namespace('store3')

  await store2.ready()
  await store3.ready()

  const feed1 = store2.default()
  await feed1.ready()
  const feed3 = store3.get({ key: feed1.key })
  const feed4 = store3.get({ key: feed1.key })
  const feed5 = store3.get({ key: feed1.key })

  t.same(feed1, feed3)
  t.same(feed1, feed4)
  t.same(feed1, feed5)
  t.same(store1._references.get(feed1), 2)

  t.end()
})

test('caching works correctly when reopening by discovery key', async t => {
  var store = await create('test-store')
  var firstCore = store.default()
  var discoveryKey = null
  var reopenedCore = null

  await runAll([
    cb => firstCore.ready(cb),
    cb => {
      discoveryKey = firstCore.discoveryKey
      return cb(null)
    },
    cb => firstCore.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(firstCore.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      create('test-store').then(reopenedStore => {
        store = reopenedStore
        reopenedCore = store.get({ discoveryKey })
        return reopenedCore.ready(cb)
      })
    },
    cb => {
      const idx = discoveryKey.toString('hex')
      t.true(store._internalCores.get(idx))
      return cb(null)
    }
  ])

  await validateCore(t, reopenedCore, [Buffer.from('hello')])
  await cleanup(['test-store'])
  t.end()
})

test('destroying a store deletes its data', async t => {
  const store = await create('test-store')
  const firstCore = store.default()
  let defaultKey = null

  await runAll([
    cb => firstCore.append('Hello World', cb),
    cb => {
      defaultKey = firstCore.key
      t.same(firstCore.length, 1, 'core has data')
      return process.nextTick(cb, null)
    },
    cb => store.destroy(cb)
  ])

  const reopenedStore = await create('test-store')
  const reopenedCore = reopenedStore.default()

  await runAll([
    cb => reopenedCore.ready(cb)
  ])

  t.same(reopenedCore.key, defaultKey, 'regenerated same default core')
  t.same(reopenedCore.length, 0, 'core is now empty')

  await cleanup(['test-store'])
  t.end()
})

test('destroying a namespaced store deletes its data', async t => {
  const store = await create('test-store')
  const store2 = store.namespace('store2')

  await store.ready()
  await store2.ready()

  const firstCore = store2.default()
  let defaultKey = null

  await runAll([
    cb => firstCore.append('Hello world', cb),
    cb => {
      defaultKey = firstCore.key
      t.same(firstCore.length, 1, 'core has data')
      return process.nextTick(cb, null)
    },
    cb => store2.destroy(cb)
  ])

  const reopenedStore = store.namespace('store2')

  await store2.ready()

  const reopenedCore = reopenedStore.default()

  await runAll([
    cb => reopenedCore.ready(cb)
  ])

  t.same(reopenedCore.key, defaultKey, 'regenerated same default core')
  t.same(reopenedCore.length, 0, 'core is now empty')

  await cleanup(['test-store'])
  t.end()
})

async function create (storage, opts) {
  const store = new Corestore(storage, opts)
  await store.ready()
  return store
}
