
const { Writable } = require('stream')
const { createGzip } = require('zlib')
const tempPath = require('temp-path')
const { v4 } = require('uuid')
const path = require('path')
const fs = require('fs')

exports = module.exports = options => {
  return new RedshiftLoader(options)
}

exports.RedshiftLoader = RedshiftLoader

class RedshiftLoader extends Writable {
  constructor(options) {
    this.options = options || {}
    this.i = 0

    super({
      objectMode: true
    })
  }

  _validateOptions() {
    const { options } = this

    if (typeof options.transform === 'function') {
      this._transform = options.transform
    }

    const files = this.files = options.files || 1
    const filenames = this.filenames = []
    for (let i = 0; i < files.length; i++) {
      filenames.push(tempPath())
    }

    this.streams = filenames.map(filename => {
      const gzip = createGzip()
      const stream = fs.createWriteStream(filename)
      gzip.pipe(stream)
      return {
        gzip,
        stream
      }
    })
  }

  transform (doc) {
    const transform = this._transform
    if (!transform) return Promise.resolve(doc)
    return new Promise(resolve => resolve(transform(doc)))
  }

  // select which write stream to write to
  selectWriteStream () {
    this.i = ++this.i % this.files
    return this.streams[this.i].gzip
  }

  _write(doc, null, cb) {
    this.transform(doc).then(doc => {
      this.selectWriteStream().write(JSON.stringify(doc) + '\n')
    }).then(cb, cb)
  }

  _promise () {
    new Promise((resolve, reject) => {
      // wait until the file write streams are finished
      resolve(Promise.all(this.streams.map(({ stream }) => (
        new Promise((resolve2, reject2) => {
          stream.once('error', reject)
          stream.once('finish', resolve)
        })
      ))))

      this.once('error', reject)
      this.once('finish', () => {
        // once this stream ended, end all the gzip streams
        this.streams.map(({ gzip }) => gzip.end())
      })
      // end this stream
      this.end()
    })
  }

  then (resolve, reject) {
    this.promise = this.promise || this._promise()
    return this.promise(resolve, reject)
  }
}
