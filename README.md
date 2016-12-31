# Redshift Loader

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
- `s3 [required]` - an instance of `AWS.S3()`
- `s3_bucket <String>` - S3 bucket to use. Only required if you don't set it in your `AWS.S3()` options.
- `s3_prefix <String> [required]` - a prefix to save the data. It should end with a `/`.
- `s3_format <Function>` - how the files are formatted, defaulting to `YYYY/MM/DD/HH/MM/SS-UUIDV4.gz`
- `redshift_url <String> [required]` - URL for the Redshift server
- `redshift_table <String> [required]` - table to load data into
- `transform <AsyncFunction>` - optional transform function for every object. Useful if you want to add `.id` or something to every object.
