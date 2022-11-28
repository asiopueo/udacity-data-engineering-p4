import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import IntegerType

import logging
logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    print("Creating Spark session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function takes the song_data-files and creates two tables:
            * the songs_table (dim)
            * the artists_table (dim)
        Finally, these two tables are saved as paquet-files.
    """
    
    logging.info("Processing song data")
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.option("recursiveFileLookup", "true").json(song_data)

    # extract columns to create songs table
    # We require the following columns: song_id, title, artist_id, year, duration
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table.write.partitionBy('year', 'artist_id').save(output_data + 'songs_table_csv', format='csv', header=True)
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs_table_parquet')

    # extract columns to create artists table
    # We require the following columns: artist_id, name, location, latitude, longitude
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    artists_table = artists_table.toDF('artist_id', 'name', 'location', 'latitude', 'longitude')
    
    # write artists table to parquet files
    #artists_table.write.save(output_data + 'artists_table_csv', format='csv', header=True)
    artists_table.write.parquet(output_data + 'artists_table_parquet')

def process_log_data(spark, input_data, output_data):
    """
        This function takes the song_data-files and creates two tables:
            * the users_table (dim)
            * the time_table (dim)
            * and finally the songplays_table (fact)
        Finally, these two tables are saved as paquet-files.
    """
    logging.info("Processing log data")
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter( log_df['page'] == 'NextSong' )

    ## extract columns for users table   
    # We require the following columns: user_id, first_name, last_name, gender, level
    users_table = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    users_table = users_table.toDF('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write users table to parquet files
    #users_table.write.save(output_data + 'users_table_csv', format='csv', header=True)
    users_table.write.parquet(output_data + 'users_table_parquet')

    # create timestamp column from original timestamp column
    # Epoch time is measured in seconds, and the build-in function from_unixtime
    # Hence, the timestamp values need to be converted from milliseconds to seconds first
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))

    log_df = log_df.withColumn("timestamp", get_timestamp('ts'))
   
    
    ## extract columns to create time table
    # We require the following columns: start_time, hour, day, week, month, year, weekday

    get_starttime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%H:%M:%S'))

    time_table = log_df.select(
        get_starttime('ts').alias('start_time'),
        hour('timestamp').alias('hour'),
        dayofmonth('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'),
        dayofweek('timestamp').alias('weekday')
    )
    

    # write time table to parquet files partitioned by year and month
    #time_table.write.partitionBy('year', 'month').save(output_data + 'time_table_csv', format='csv', header=True)
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table_parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    ## extract columns from joined song and log datasets to create songplays table
    # We require the following columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    # Enrich data with a primary key
    log_df = log_df.withColumn("songplay_id", monotonically_increasing_id())
    
    songplays_table = log_df.join(song_df, log_df.artist == song_df.artist_name , "inner")\
            .select(
            col('songplay_id'),
            col('timestamp').alias('start_time'), 
            year('timestamp').alias('year'),
            month('timestamp').alias('month'),
            col('userId').alias('user_id'), 
            col('level'), 
            col('song_id'), 
            col('artist_id'), 
            col('sessionId').alias('session_id'), 
            col('artist_location').alias('location'), 
            col('userAgent').alias('user_agent')
    )
    
    # write songplays table to parquet files partitioned by year and month
    #songplays_table.write.partitionBy('year', 'month').save(output_data + 'songplays_table_csv', format='csv', header=True)
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays_table_parquet')


def main():
    spark = create_spark_session()

    input_s3_bucket = 's3a://udacity-dend/' 
    output_s3_bucket = 's3a://sparkifyllc-analytics/'
    local_dir = '/home/workspace/'

    # Flag to select either local storage on client or S3-bucket for data in- and output
    use_s3 = False

    if use_s3:
        input_path = input_s3_bucket
        output_path = os.path.join(output_s3_bucket, 'output/')
    else:
        input_path = os.path.join(local_dir, 'data/')
        output_path = os.path.join(local_dir, 'output/')

    process_song_data(spark, input_path, output_path)    
    process_log_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
