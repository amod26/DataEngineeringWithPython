import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import DateType,TimestampType
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates spark session and initializes Spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads the file, extract columns and write inside Parquet file,
    Creates Songs, Artists table and writes inside Parquet File
    """
    # get filepath to song data file
    song_data = 'data/song-data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.option("header", "true").json(song_data)

     # extract columns to create songs table
    songs_table = df.select(['song_id','title','duration','year','artist_id']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data+"songs_table/",  mode="overwrite")

    
    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']).distinct()
    
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists_table/",  mode="overwrite")



def process_log_data(spark, input_data, output_data):
    """
    Reads log files, filters by songs , extract columns and writes to Parquet file
    Creates timestamps, extract columns,create time table, writes to the time table and then to Parquet file
    Reads songs_table,extracts columns,creates songplay table and writes to Parquet file
    """
    # get filepath to log data file
    log_data = 'data/log-data/*.json'


    # read log data file
    df = spark.read.option("header", "true").json(log_data)
    
    # filter by actions for song plays
    log_data = df.filter(df.page=='NextSong')
    df = log_data

    # extract columns for users table    
    artists_table = df.select(['userId','firstName','lastName','gender','level']).distinct()
    
    # write users table to parquet files
    artists_table.write.parquet(output_data+"users_table/",  mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.fromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))
    
   
    # extract columns to create time table
    df = df.withColumn('hour', hour('start_time')) \
                    .withColumn('day', dayofmonth('start_time')) \
                    .withColumn('week', weekofyear('start_time')) \
                    .withColumn('month', month('start_time')) \
                    .withColumn('years', year('start_time')) \
                    .withColumn('weekday', dayofweek('start_time'))
        
    time_table = df.select('ts','start_time','hour', 'day', 'week', 'month', 'years', 'weekday').drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("years","month").parquet(output_data+"time/",  mode="overwrite")


    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.withColumnRenamed(existing='title',new='song')\
                        .join(df, on=['song'],how='right') \
                        .select(['userId','years', 'month', 'start_time','level','song_id','artist_id','sessionId','location','userAgent']) \
                        .withColumn("songplay_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("years","month").parquet(output_data+"songplays_table/",  mode="overwrite")



def main():
    spark = create_spark_session()
    input_data = "data"
    output_data = "Output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
