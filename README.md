# Redshift Loader

[![NPM version][npm-image]][npm-url]
5	[![Build status][travis-image]][travis-url]
6	[![Test coverage][codecov-image]][codecov-url]
7	[![Dependency Status][david-image]][david-url]
8	[![License][license-image]][license-url]
9	[![Downloads][downloads-image]][downloads-url]

A module to help load data from an object stream directly to Redshift.

The flow is as follows:

- Write objects to the write stream
- Convert the objects to JSON, gzip it, then write it to files round-robin
- Upload files to S3
- Upload a manifest to S3
- Create a temporary table in Redshift
- Load the files into the temporary table using the manifest
- Load the data from the temporary table to the main table
- Delete the temporary table

## API

Required permissions for both IAM and Redshift user role:

- Load data into S3
- Load data into Redshift
- Create and delete temporary tables in Redshift

### const writer = RedshiftLoader(options)

Creates a new write stream in `ObjectMode`.
You should be writing only objects as this module handles `JSON.stringify()`.

Options are:

- `files = 1` - the number of files to load data from.
  Redshift loads data faster if it loads data from multiple files in parallel.
  You really only need to use this if you are loading a lot of data.
- `transform <AsyncFunction>` - optional transform function for every object. Useful if you want to add `.id` or something to every object.
- `s3 <Object> [required]` - s3 options
  - `region <String>` - s3 bucket region
  - `bucket <String> [required]` - s3 bucket to store dat
  - `prefix <String>` - a prefix to save the data. It should end with a `/`.
  - `format <Function>` - how the files are formatted, defaulting to `YYYY/MM/DD/HH/MM/UUIDV4`
  - `accessKeyId <String> [required]`
  - `secretAccessKey <String> [required]`
- `redshift <Object> [required]`
  - `url <String> [required]` - URL for the redshift server
  - `table <String> [required]` - table to load into

### writer.write({})

### writer.then(() => {})

[npm-image]: https://img.shields.io/npm/v/redshift-loader.svg?style=flat-square
[npm-url]: https://npmjs.org/package/redshift-loader
[travis-image]: https://img.shields.io/travis/jonathanong/redshift-loader/master.svg?style=flat-square
[travis-url]: https://travis-ci.org/jonathanong/redshift-loader
[codecov-image]: https://img.shields.io/codecov/c/github/jonathanong/redshift-loader/master.svg?style=flat-square
[codecov-url]: https://codecov.io/github/jonathanong/redshift-loader
[david-image]: http://img.shields.io/david/jonathanong/redshift-loader.svg?style=flat-square
[david-url]: https://david-dm.org/jonathanong/redshift-loader
[license-image]: http://img.shields.io/npm/l/redshift-loader.svg?style=flat-square
[license-url]: LICENSE
[downloads-image]: http://img.shields.io/npm/dm/redshift-loader.svg?style=flat-square
[downloads-url]: https://npmjs.org/package/redshift-loader
