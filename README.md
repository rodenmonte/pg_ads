# Using
## Install postgres 15
The `CREATE OR REPLACE` trigger syntax doesn't work with older versions of Postgres.

## Create alerts
Either create an alert config file based on your dataset, or use `test.yaml` with our test dataset defined in `test.sql`.

When using our test dataset, run `test.sql` in a `psql` console. `psql` backslash commands must be copy pasted, nesting of backslash commands is not supported).

## Benchmarking
`bench/runner.rb` can be used to produce many timings