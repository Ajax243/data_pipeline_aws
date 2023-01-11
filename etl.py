import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import  pyspark.sql.functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "")\
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data , 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,'songs'),partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)
    df = df.withColumn('user_id',df.userId.cast(IntegerType()))

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(['userId','firstName','lastName','gender','level'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn('timestamp',F.to_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn('timestamp', get_timestamp('ts').cast(TimestampType()))
    
    # extract columns to create time table
    time_table = df.select('timestamp')
    time_table =time_table.withColumn('hour', F.hour('timestamp'))
    time_table = time_table.withColumn('day', F.dayofmonth('timestamp'))
    time_table = time_table.withColumn('week', F.weekofyear('timestamp'))
    time_table = time_table.withColumn('month', F.month('timestamp'))
    time_table = time_table.withColumn('year', F.year('timestamp'))
    time_table = time_table.withColumn('weekday', F.dayofweek('timestamp'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,'time'), partitionBy=['year','month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data,'song_data','*','*','*'))

    # extract columns from joined song and log datasets to create songplays table 
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('logs')

    songplays_table = spark.sql("""
    SELECT 
        l.songplay_id,
        l.start_time,
        l.user_id,
        l.level,
        s.song_id,
        s.artist_id,
        l.sessionId,
        l.location,
        l.userAgent,
        year(l.start_time) as year,
        month(l.start_time) as month
    FROM logs l
    LEFT JOIN songs s 
    ON  l.song=s.title AND
        l.artist=s.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3a://output-for-dl/'

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
