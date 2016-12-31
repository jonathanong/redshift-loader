
const { Writable } = require('stream')
const { createGzip } = require('zlib')
const tempPath = require('temp-path')
const moment = require('moment')
const { v4 } = require('uuid')
const pg = require('pg-then')
const fs = require('fs')

exports = module.exports = options => {
  return new RedshiftLoader(options)
}

exports.RedshiftLoader = RedshiftLoader

exports.s3_format = prefix => prefix + moment.utc().format('YYYY/MM/DD/HH/mm/') + v4()

exports.onError = err => {
  if (!err) return

  console.error(err.stack || err)
}

class RedshiftLoader extends Writable {
  constructor (options) {
    super({
      objectMode: true
    })

    this.options = options || {}
    this.i = 0
  }

  _validateOptions () {
    const { options } = this

    this.s3 = options.s3
    this.prefix = options.prefix || ''

    if (typeof options.transform === 'function') {
      this._transform = options.transform
    }

    const files = this.files = options.files || 1
    const filenames = this.filenames = []
    const keys = this.keys = []
    const name = exports.s3_format(this.prefix)
    for (let i = 0; i < files.length; i++) {
      filenames.push(tempPath())
      keys.push(`${name}/${i}.json.gz`)
    }

    this.manifestKey = `${name}/manifest.json`

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

  // TODO: backpressure
  _write (doc, NULL, cb) {
    this.transform(doc).then(doc => {
      this.selectWriteStream().write(JSON.stringify(doc) + '\n')
    }).then(cb, cb)
  }

  uploadFile (Key, Body, gzipped) {
    const { s3_bucket } = this.options
    const Params = {
      Key,
      Body,
      Bucket: s3_bucket,
      ContentType: `application/json`
    }
    if (gzipped) Params.ContentEncoding = `gzip`
    return this.s3.putObject(Params).promise()
  }

  uploadFiles () {
    return Promise.all(this.filenames.map((filename, i) => {
      const Key = this.keys[i]
      return this.uploadFile(Key, fs.createReadStream(filename), true)
    }))
  }

  // TODO: can I upload gzipped?
  uploadManifest () {
    const { s3_bucket } = this.options
    const manifest = {
      entries: this.keys.map(Key => ({
        url: `s3://${s3_bucket}/${Key}`,
        mandatory: true
      }))
    }

    return this.uploadFile(this.manifestKey, JSON.stringify(manifest, null, 2))
  }

  copy () {
    const { options } = this
    const client = pg.Client(this.options.redshift_url)
    const TABLE = this.options.redshift_table
    const TEMP = `${TABLE}_temp_${v4().replace(/-/g, `_`)}`

    const rsQuery = `
      BEGIN TRANSACTION;
      CREATE TEMP TABLE ${TEMP} (LIKE ${TABLE});
      COPY ${TEMP}
      FROM 's3://${options.s3_bucket}/${this.manifestKey}'
      CREDENTIALS 'aws_access_key_id=${options.accessKeyId};aws_secret_access_key=${options.secretAccessKey}'
      MANIFEST
      FORMAT AS JSON 'auto'
      GZIP
      TRUNCATECOLUMNS
      MAXERROR 100000;
      DELETE FROM ${TABLE}
      WHERE id IN (SELECT id FROM ${TEMP});
      INSERT INTO ${TABLE}
      SELECT * FROM ${TEMP};
      DROP TABLE ${TEMP};
      END TRANSACTION;
    `

    return client.query(rsQuery)
    .then(() => client.end())
    .catch(err => (
      client.query(`ROLLBACK;`).catch(exports.onError).then(() => {
        client.end()
        throw err
      })
    ))
  }

  _promise () {
    return new Promise((resolve, reject) => {
      // wait until the file write streams are finished
      resolve(Promise.all(this.streams.map(({ stream }) => (
        new Promise((resolve, reject) => {
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
