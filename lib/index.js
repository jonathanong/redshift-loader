
const { Writable } = require('stream')
const { createGzip } = require('zlib')
const tempPath = require('temp-path')
const assert = require('assert')
const moment = require('moment')
const { v4 } = require('uuid')
const AWS = require('aws-sdk')
const pg = require('pg-then')
const fs = require('fs')

exports = module.exports = options => {
  return new RedshiftLoader(options)
}

exports.RedshiftLoader = RedshiftLoader

exports.s3format = prefix => prefix + moment.utc().format('YYYY/MM/DD/HH/mm/') + v4()

exports.onError = err => {
  if (!err) return

  console.error(err.stack || err)
}

class RedshiftLoader extends Writable {
  constructor (options) {
    super({
      objectMode: true
    })

    this.options = options
    // which stream to write to
    this.i = 0

    this._validateOptions()
  }

  _validateOptions () {
    const { options } = this
    assert(options, 'Options are required!')

    const { s3 } = options
    assert(s3, '.s3 options are required!')

    this.s3 = new AWS.S3(Object.assign({
      params: {
        Bucket: options.bucket
      }
    }, s3))
    this.s3format = typeof s3.format === 'function'
      ? s3.format
      : exports.s3format

    this.prefix = options.prefix || ''
    if (typeof options.transform === 'function') {
      this._transform = options.transform
    }

    const files = this.files = options.files || 1
    const filenames = this.filenames = []
    const keys = this.s3keys = []
    const name = this.s3Format(this.prefix)
    for (let i = 0; i < files.length; i++) {
      filenames.push(tempPath())
      keys.push(`${name}/${i}.json.gz`)
    }

    this.s3manifestKey = `${name}/manifest.json`

    this.streams = filenames.map(filename => {
      const gzip = createGzip()
      const stream = fs.createWriteStream(filename)
      gzip.pipe(stream)
      return {
        gzip,
        stream
      }
    })

    this.onError = options.onError || exports.onError
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
  // TODO: write in parallel?
  _write (doc, NULL, cb) {
    this.transform(doc).then(doc => {
      this.selectWriteStream().write(JSON.stringify(doc) + '\n')
    }).then(cb, cb)
  }

  uploadFile (Key, Body, gzipped) {
    const Params = {
      Key,
      Body,
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
    const { bucket } = this.options.s3
    const manifest = {
      entries: this.s3keys.map(Key => ({
        url: `s3://${bucket}/${Key}`,
        mandatory: true
      }))
    }

    return this.uploadFile(this.s3manifestKey, JSON.stringify(manifest, null, 2))
  }

  copy () {
    const { options } = this
    const { redshift, s3 } = options
    const client = pg.Client(redshift.url)
    const TABLE = redshift.table
    const TEMP = `${TABLE}_temp_${v4().replace(/-/g, `_`)}`

    const rsQuery = `
      BEGIN TRANSACTION;
      CREATE TEMP TABLE ${TEMP} (LIKE ${TABLE});
      COPY ${TEMP}
      FROM 's3://${s3.bucket}/${this.s3manifestKey}'
      CREDENTIALS 'aws_access_key_id=${s3.accessKeyId};aws_secret_access_key=${s3.secretAccessKey}'
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
      client.query(`ROLLBACK;`).catch(this.onError).then(() => {
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
