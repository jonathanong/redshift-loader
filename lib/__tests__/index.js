
/* eslint-env jest, jasmine */

const moment = require('moment')
const assert = require('assert')
const { v4 } = require('uuid')
const pg = require('pg-then')
const path = require('path')
const fs = require('fs')

const RedshiftLoader = require('..')

jasmine.DEFAULT_TIMEOUT_INTERVAL = 30000

const options = {
  transform (doc) {
    doc.id = v4()
    doc.received_at = moment.utc().format('YYYY-MM-DD HH:mm:ss')
    return doc
  },

  s3: {
    bucket: process.env.REDSHIFT_LOADER_S3_BUCKET,
    prefix: process.env.REDSHIFT_LOADER_S3_PREFIX || 'events/',
    accessKeyId: process.env.REDSHIFT_LOADER_S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.REDSHIFT_LOADER_S3_SECRET_ACCESS_KEY
  },

  redshift: {
    url: process.env.REDSHIFT_LOADER_REDSHIFT_URL,
    table: process.env.REDSHIFT_LOADER_TABLE || 'test_events'
  }
}

const pool = pg.Pool(options.redshift.url)

beforeAll(() => pool.query(fs.readFileSync(path.join(__dirname, 'table.sql'), 'utf8')))

describe('upload and check', () => {
  const event = Math.random().toString(36)

  it('it should upload data', () => {
    const loader = RedshiftLoader(options)

    loader.write({
      event
    })

    return loader
  })

  it('should have downloaded the data', () => {
    return pool.query(`
      SELECT COUNT(*) AS count
      FROM ${options.redshift.table}
      WHERE event = $1
    `, [
      event
    ]).then(result => {
      assert(~~result.rows[0].count > 0)
    })
  })
})
