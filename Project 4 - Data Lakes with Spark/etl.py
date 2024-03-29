import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour,\
weekofyear, date_format
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField, DoubleType 
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    
    """
    This function creates the Spark Session with the packages required.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        This function loads song_data from S3 and then processes the songs and the artist tables and then loads them back to S3
        
        Parameters:
            spark       = Spark Session
            input_data  = location of the song_data where the file is loaded to process
            output_data = location of the results stored
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
        
    # read song data file
    songs_df = spark.read.json(song_data, schema=song_schema).dropDuplicates()

    # extract columns to create songs table
    songs_table = songs_df.select("title", "artist_id", "year", "duration").dropDuplicates(["artist_id"]) \
                  .withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", 
                              partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = songs_df.select("artist_id", "artist_name", "artist_location", 
                                    "artist_latitude", "artist_longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data +  "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    
    """
    This function processes all log data JSON files from the location in the input folder 
    and stores them in parquet format in the output folder
    
    Parameters:
            spark       = Spark Session
            input_data  = location of the song_data where the file is loaded to process
            output_data = location of the results stored
    
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", 
                            "level").dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/"), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time")) \
                   .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday") \
                   .drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), \
                             mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read \
                .format("parquet") \
                .option("basePath", os.path.join(output_data, "songs/")) \
                .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner') \
                        .select(monotonically_increasing_id().alias("songplay_id"), 
                                col("start_time"), 
                                col("userId").alias("user_id"), 
                                "level","song_id","artist_id", 
                                col("sessionId").alias("session_id"), 
                                "location", 
                                col("userAgent").alias("user_agent"))
    
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner") \
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", 
                        "artist_id", "session_id", "location", "user_agent", "year", "month").drop_duplicates(["user_id"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/"),
                                  mode="overwrite", partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
