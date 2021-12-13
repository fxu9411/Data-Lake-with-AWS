import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song data from base S3 location 
    and extracts JSON data and stores as Parquet
    partitioned by year and artist_id.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the song data.
    output_data : str
        The path prefix for the output data.
    
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']].dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select(df.artist_id, df.artist_name.alias('name'), df.artist_location.alias('location'),
                              df.artist_latitude.alias('latitude'),
                              df.artist_longitude.alias('longitude')).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    Loads log event data from base S3 location,
    extracts JSON data.
    Creates users table and stores as Parquet.
    Creates time table and stores as Parquet
    partitioned by year and month.
    Creates songplays table and stores as Parquet
    partitioned by year and month.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the song data.
    output_data : str
        The path prefix for the output data.
    
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']
    df.createOrReplaceTempView('log')

    # extract columns for users table
    user_table = df.select(df.userId.alias('user_id'), df.firstName.alias("first_name"), df.lastName.alias("last_name"),
                           df.gender, df.level).dropDuplicates()

    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_timestamp', get_timestamp('ts').cast(TimestampType()))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d'))
    df = df.withColumn('start_datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select(col('start_timestamp').alias('start_time'),
                           hour('start_timestamp').alias('hour'),
                           dayofmonth('start_timestamp').alias('day'),
                           weekofyear('start_timestamp').alias('week'),
                           month('start_timestamp').alias('month'),
                           year('start_timestamp').alias('year'),
                           dayofweek('start_timestamp').alias('weekday'),
                           col('ts'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'time'))
    time_table.createOrReplaceTempView('time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))
    song_df.createOrReplaceTempView('songs')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
    SELECT DISTINCT c.start_time, a.userid as user_id, a.level, b.song_id, b.artist_id, a.sessionid as session_id, a.location, a.useragent as user_agent
    FROM log as a 
    LEFT JOIN songs as b ON a.song = b.title
    LEFT JOIN time as c ON a.ts = c.ts
    """)

    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
