const rimraf = require('rimraf')

function runAll (ops) {
  return new Promise((resolve, reject) => {
    runNext(ops.shift())
    function runNext (op) {
      op(err => {
        if (err) return reject(err)
        const next = ops.shift()
        if (!next) return resolve()
        return runNext(next)
      })
    }
  })
}

function validateCore (t, core, values) {
  const ops = values.map((v, idx) => cb => {
    core.get(idx, (err, value) => {
      t.error(err, 'no error')
      t.same(value, values[idx])
      return cb(null)
    })
  })
  return runAll(ops)
}

async function cleanup (dirs) {
  return Promise.all(dirs.map(dir => new Promise((resolve, reject) => {
    rimraf(dir, err => {
      if (err) return reject(err)
      return resolve()
    })
  })))
}

function delay (ms, cb) {
  return new Promise(resolve => {
    setTimeout(() => {
      if (cb) cb()
      resolve()
    }, ms)
  })
}

module.exports = {
  delay,
  cleanup,
  validateCore,
  runAll
}
