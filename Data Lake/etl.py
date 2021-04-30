import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

# Function to create Spark Session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description:
    Function to read and transform the song data and load the data into the songs and artists dimension tables into S3.
    
    :param spark: Reference to the spark session
    :param input_data: Base path for input data in S3
    :param output_data: Base path for output data in S3
    
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration")).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy(["year", "artist_id"]).parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name").alias("name"), col("artist_location").alias("location"), col("artist_latitude").alias("lattitude"), col("artist_longitude").alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """
    Description:
    Function to read and transform the log data and populate the users and time dimension tables and songplays fact table into S3.
    
    :param spark: Reference to the spark session
    :param input_data: Base path for input data in S3
    :param output_data: Base path for output data in S3
    
    """
    
    # get filepath to log data file
    log_data = input_data + "log-data/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), col("gender"), col("level")).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
  
    # extract columns to create time table
    time_table = df.select(
                        col("start_time").alias("start_time"), 
                        hour(col("start_time")).alias("hour"), 
                        dayofmonth(col("start_time")).alias("day"), 
                        weekofyear(col("start_time")).alias("week"), 
                        month(col("start_time")).alias("month"), 
                        year(col("start_time")).alias("year"), 
                        dayofweek(col("start_time")).alias("weekday")
                    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy(["year","month"]).parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")
    
    # read in time data to use for songplays table
    time_df = spark.read.parquet(output_data + "time/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title), how='inner') \
                        .join(time_df, df.start_time == time_df.start_time, how='inner') \
                        .select(monotonically_increasing_id().alias("songplay_id"),
                                time_df.start_time.alias("start_time"), 
                                df.userId.alias("user_id"), 
                                df.level.alias("level"), 
                                song_df.song_id.alias("song_id"), 
                                song_df.artist_id.alias("artist_id"), 
                                df.sessionId.alias("session_id"), 
                                df.location.alias("location"), 
                                df.userAgent.alias("user_agent"),
                                time_df.year.alias("year"),
                                time_df.month.alias("month"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy(["year", "month"]).parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-data-lake-project-bucket/input_data/"
    output_data = "s3a://udacity-data-lake-project-bucket/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
