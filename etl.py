import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf as udf
from pyspark.sql import types as t
import logging

logging.basicConfig(
    format="""%(asctime)s,%(msecs)d %(levelname)-8s[%(filename)s:%(funcName)s:%(lineno)d] %(message)s""",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)

config = configparser.ConfigParser()
config.read("dwh.cfg")


os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET')

def create_session_aws():
    '''
    Create spark session to read in dataframes
    from s3 buckets
    '''
    spark = SparkSession.\
                        builder.\
                        appName('sparkify_data').\
                        config('"spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"').\
                        getOrCreate()
    return spark 

def create_session_local():
    '''
    Create spark session to read in dataframes
    '''
    spark = SparkSession.\
                        builder.\
                        appName('sparkify_data').\
                        getOrCreate()
    return spark

def etl_song_data(spark,song_data_path,dest_base_path):
    '''
    Extracts songs data from source and creates: 
    1. song
    2. artist tables. Registers songs table for further 
    processing
    '''
    path = os.path.join(song_data_path,"*","*","*","*.json")
    logging.info("Reading songs metedata")
    df = spark.read.json(path)
    logging.info("Read complete")
    song_table_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    artist_table_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    songs_table = df.select(song_table_cols).drop_duplicates()
    artist_table = df.select(artist_table_cols).drop_duplicates()
    logging.info("Writing songs table")
    songs_table.write.partitionBy(['year','artist_id']).parquet(os.path.join(dest_base_path,"songs_table"))
    logging.info("Writing artist table")
    artist_table.write.parquet(os.path.join(dest_base_path,"artist_table"))
    df.createOrReplaceTempView("song_df_table")
    logging.info("Songs table temp view created")


def etl_log_data(spark,log_data_path,dest_base_path):
    '''
    Performs etl for log data, following tables are generated:
    1. Time table
    2. User table
    3. Songplays table
    '''
    path = os.path.join(log_data_path,"*","*","*.json")
    df = spark.read.json(path)

    user_table_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_table=df.select(user_table_cols).drop_duplicates()

    @udf(t.FloatType())
    def norm_ts(v):
        return v/1000

    logs = df.withColumn("start_time",f.from_unixtime(norm_ts("ts")))
    time_table = logs.\
            withColumn('hour',f.hour('start_time')).\
            withColumn("day",f.dayofmonth("start_time")).\
            withColumn("week",f.dayofweek("start_time")).\
            withColumn("month",f.month("start_time")).\
            withColumn("year",f.month("start_time")).distinct()

    songs = spark.sql("Select * from song_df_table")
    cols = ['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    song_plays = songs.\
            join(logs.where("page=='NextSong'"),songs.artist_name==logs.artist,'inner').\
            select(cols).distinct().\
            withColumn("songplay_id",f.monotonically_increasing_id()).\
            withColumn("year",f.year("start_time")).\
            withColumn("month",f.month("start_time")).distinct()

    logging.info('Writing user table')
    user_table.write.parquet(os.path.join(dest_base_path,"user_table"))
    
    logging.info('Writing time table')
    time_table.write.partitionBy('year','month').parquet(os.path.join(dest_base_path,"time_table"))
    
    logging.info('Writing songplays table')
    song_plays.write.partitionBy('year','month').parquet(os.path.join(dest_base_path,'songplays_table'))



if __name__=='__main__':
    spark = create_session_aws()
    song_data_path = config.get("S3","SONG_DATA")
    log_data_path = config.get("S3","LOG_DATA")
    dest_base_path = config.get("S3","DEST_PATH")
    logging.info("Starting etl for songs data")
    etl_song_data(spark,song_data_path,dest_base_path)
    logging.info("Starting etl for log data")
    etl_log_data(spark,log_data_path,dest_base_path)
