import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df[['artist_id', 'name', 'location', 'lattitude', 'longitude']]
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, '/log_data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table    
    artists_table = df[['artist_id', 'name', 'location', 'lattitude', 'longitude']]
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df = df.withColumn('start_timestamp', get_timestamp('ts').cast(TimestampType()))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('start_datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select(col('start_timestamp').alias('start_time'),
                           hour('start_timestamp').alias('hour'),
                           dayofmonth('start_timestamp').alias('day'),
                           weekofyear('start_timestamp').alias('week'),
                           month('start_timestamp').alias('month'),
                           year('start_timestamp').alias('year'),
                           dayofweek('start_timestamp').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_data.join(song_df, log_data['song']==song_df['title'], how='left').join('time_table', datetime.fromtimestamp(log_data['ts']/1000)==time_table['start_time'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "data/"
    output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
