# corestore
[![Build Status](https://travis-ci.com/andrewosh/corestore.svg?token=WgJmQm3Kc6qzq1pzYrkx&branch=master)](https://travis-ci.com/andrewosh/corestore)

__Note: These APIs are still in progress and are subject to change prior to the Hyperdrive v10 release.__

This module is the canonical implementation of the "corestore" interface, which exposes a Hypercore factory and a set of associated functions for managing generated Hypercores.

A corestore is designed to efficiently store and replicate multiple sets of interlinked Hypercores, such as those used by [Hyperdrive](https://github.com/mafintosh/hyperdrive) and [mountable-hypertrie](https://github.com/andrewosh/hypertrie), removing the responsibility of managing custom storage/replication code from these higher-level modules.

In order to do this, corestore provides:
1. __Key derivation__ - all writable Hypercore keys are derived from a single master key.
2. __Caching__ - Two separate caches are used for passively replicating cores (those requested by peers) and active cores (those requested by the owner of the corestore).
3. __Storage bootstrapping__ - You can create a `default` Hypercore that will be loaded when a key is not specified, which is useful when you don't want to reload a previously-created Hypercore by key.
4. __Namespacing__ - If you want to create multiple compound data structures backed by a single corestore, you can create namespaced corestores such that each data structure's `default` feed is separate.

### Installation
`npm i corestore --save`

### Usage
A corestore instance can be constructed with a random-access-storage module, a function that returns a random-access-storage module given a path, or a string. If a string is specified, it will be assumed to be a path to a local storage directory:
```js
const Corestore = require('corestore')
const ram = require('random-access-memory')
const store = new Corestore(ram)
await store.ready()
```

Hypercores can be generated with both the `get` and `default` methods. If the first writable core is created with `default`, it will be used for storage bootstrapping. We can always reload this bootstrapping core off disk without your having to store its public key externally. Keys for other hypercores should either be stored externally, or referenced from within the default core:
```js
const core1 = store1.default()
```
_Note: You do not have to create a default feed before creating additional ones unless you'd like to bootstrap your corestore from disk the next time it's instantiated._

Additional hypercores can be created by key, using the `get` method. In most scenarios, these additional keys can be extracted from the default (bootstrapping) core. If that's not the case, keys will have to be stored externally:
```js
const core2 = store1.get({ key: Buffer(...) })
```
All hypercores are indexed by their discovery keys, so that they can be dynamically injected into replication streams when requested.

Two corestores can be replicated with the `replicate` function, which accepts hypercore's `replicate` options:
```js
const store1 = new Corestore(ram)
const store2 = new Corestore(ram)
await Promise.all([store1.ready(), store2.ready()]

const core1 = store2.get()
const core2 = store2.get({ key: core1.key })
const stream = store1.replicate(true, { live: true })
stream.pipe(store2.replicate(false, { live: true })).pipe(stream) // This will replicate all common cores.
```

### API
#### `const store = corestore(storage, [opts])`
Create a new corestore instance. `storage` can be either a random-access-storage module, or a function that takes a path and returns a random-access-storage instance.

Opts is an optional object which can contain any Hypercore constructor options, plus the following:
```js
{
  cacheSize: 1000 // The size of the LRU cache for passively-replicating cores.
}
```

#### `store.default(opts)`
Create a new default hypercore, which is used for bootstrapping the creation of subsequent hypercores. Options match those in `get`.

#### `store.get(opts)`
Create a new hypercore. Options can be one of the following:
```js
{
  key: 0x1232..., // A Buffer representing a hypercore key
  discoveryKey: 0x1232..., // A Buffer representing a hypercore discovery key (must have been previously created by key)
  ...opts // All other options accepted by the hypercore constructor
}
```

If `opts` is a Buffer, it will be interpreted as a hypercore key.

#### `store.on('feed', feed, options)`

Emitted everytime a feed is loaded internally (ie, the first time get(key) is called).
Options will be the full options map passed to .get.

#### `store.replicate(isInitiator, [opts])`
Create a replication stream that will replicate all cores currently in memory in the corestore instance.

When piped to another corestore's replication stream, only those cores that are shared between the two corestores will be successfully replicated.

#### `store.list()`
Returns a Map of all cores currently cached in memory. For each core in memory, the map will contain the following entries:
```
{
  discoveryKey => core,
  ...
}
```

#### `const namespacedStore = store.namespace('some-name')`
Create a "namespaced" corestore that uses the same underlying storage as its parent, and mirrors the complete corestore API. 

`namespacedStore.default` returns a different default core, using the namespace as part of key generation, which makes it easier to bootstrap multiple data structures from the same corestore. The general pattern is for all data structures to bootstrap themselves from their corestore's default feed:
```js
const store = new Corestore(ram)
const drive1 = new Hyperdrive(store.namespace('drive1'))
const drive2 = new Hyperdrive(store.namespace('drive2'))
```

Namespaces currently need to be saved separately outside of corestore (as a mapping from key to namespace), so that data structures remain writable across restarts. Extending the above code, this might look like:
```js
async function getDrive (opts = {}) {
  let namespace = opts.key ? await lookupNamespace(opts.key) : await createNamespace()
  const namespacedCorestore = store.namespace(namespace)
  const drive = new Hyperdrive(namespacedCorestore)
  await saveNamespace(drive.key, namespace)
}
```

#### `store.close(cb)`
Close all hypercores previously generated by the corestore.

#### `store.destroy(cb)`
Close the store and destroy the data for all cores.

### License
MIT
