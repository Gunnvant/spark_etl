## Spark Data Lake

### Datawarehouse Structure

This repo demonstrates data dump on `s3`. The source tables are on `s3`.

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

From these two tables following fact and dimension tables are created:

1. `songplays_fact`
2. `user_dim`
3. `songs_dim`
4. `artist_dim`
5. `time_dim`

### Project Structure

Use the `dwh.cfg` to setup credentials.

Run `etl.py`. The scripts runs on a local cluster, pulls data from s3, does necessary transforms and pushes data to another s3 bucket.

The script can be modified to be run using `spark-submit`.  Hardcode the aws credentials, source and destination paths in the `etl.py` file as `spark-submit` will not be able to access the config file.