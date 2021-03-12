import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

"""
Reads in credentials from config file
"""

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the songs json files and create tables for songs and artists. The tables are then written to S3
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    print("song_data read")
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

        
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist").parquet(output_data + "songs/", mode="overwrite")
    print("songs_table written to parquet file")

    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")
    
    print("artists_table written to parquet file")

def process_log_data(spark, input_data, output_data):
    """
    Process the event log files and create tables for users, time and songplays from the data. The tables are then written to S3.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """     
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    print("log_data read")
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates() 
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")
    print("users_table written to parquet file")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datatime.fromtimestamp(int(x)/1000), TimestampType()) 
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datatime.fromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time"))\
                    .withColumn("day", dayofmonth("start_time"))\
                    .withColumn("week", weekofyear("start_time"))\
                    .withColumn("month", month("start_time"))\
                    .withColumn("year", year("start_time"))\
                    .withColumn("weekday", dayofweek("start_time"))\
                    .select("start_time", "hour", "day", "week", "month", "year", "weekday").dropDuplicates()
   
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table/", mode="overwrite")
    print("time_table written to parquet file")
    
    # read in song data to use for songplays table (is this PARQUET FILE?)
    song_df = spark.read.parquet(output_data + "songs/")
    print("song_data read")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how="inner")\
                        .select(monotonically_increasing_id().alias("songplay_id"),\
                                "start_time",\
                                col("userId").alias("user_id"),\
                                "level",\
                                "song_id",\
                                "artist_id",\
                                col("sessionId").alias("session_id"),\
                                "location",\
                                col("userAgent").alias("user_agent"),\
                                "year")\
                                .withColumn("month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partionBy("year", "month").parquet(output_data + "songplays/", mode="overwrite")
    print("songplays_table written to parquet file")

def main():
    """
    Main function that runs the above methods
    """
    spark = create_spark_session()
    print("spark session created")
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://lr53-data-lakes"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
