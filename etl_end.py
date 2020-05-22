import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType
from pyspark.sql.functions import expr,from_unixtime,row_number,dayofweek,year,month,dayofmonth,hour,date_format,desc,col,dense_rank,rank,weekofyear,monotonically_increasing_id
from pyspark.sql.types import StructField,StructType,StringType,LongType,DoubleType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

spark=create_spark_session()

def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data = input_data
 
    # read song data file
    df = spark.read.format('json').option('mode','permissive').load(input_data)
    df.createOrReplaceTempView("songs_table_DF")
    # extract columns to create songs table
    songs_table = songs_table=spark.sql('select distinct song_id,title,artist_id,year,cast(duration as float) duration from songs_table_DF')
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table.write.format('csv').mode('overwrite').save(output_data)
    songs_table.write.format('parquet').mode('overwrite').save(output_data+'/songs/')

    # extract columns to create artists table
    artists_table=spark.sql('''
      select artist_id,name,location,latitude,longitude from 
    (
    select 
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude,
        row_number() over (partition by artist_id order by artist_id desc) as row_id
    from songs_table_DF
    )
    where row_id=1
    ''')
    
    # write artists table to parquet files
    artists_table.write.format('parquet').mode('overwrite').save(output_data+'/artists/')


def process_log_data(spark, input_data, output_data):
   
    # get filepath to log data file
    log_data = 'log_data/*/*/*.json'
    # read log data file
    df = spark.read.json(input_data + log_data)
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')
    # rename fields
    fields = [("artist", "artist"),
          ("auth", "auth"),
          ("firstName", "first_name"),
          ("gender", "gender"),
          ("itemInSession", "itemInSession"),
          ("lastName", "last_name"),
          ("length", "length"),
          ("level", "level"),
          ("location", "location"),
          ("method", "method"),
          ("page", "page"),
          ("registration", "registration"),
          ("sessionId", "session_id"),
          ("song", "song"),
          ("status", "status"),
          ("ts", "ts"),
          ("userAgent", "user_agent"),
          ("userId", "user_id")
          ]
    exprs = [ "{} as {}".format(field[0],field[1]) for field in fields]
    df = df.selectExpr(*exprs)

    # extract columns for users table
    user_fields = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    users_table = df.select(user_fields).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + '/users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, Dbl())
    df = df.withColumn('ts2', get_timestamp('ts'))

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', from_unixtime('ts2').cast(dataType=TimestampType()))

    # extract columns to create time table
    time_table = df.select('start_time')\
                        .dropDuplicates()\
                        .withColumn('hour', hour(col('start_time')))\
                        .withColumn('day', dayofmonth(col('start_time')))\
                        .withColumn('week', weekofyear(col('start_time')))\
                        .withColumn('month', month(col('start_time')))\
                        .withColumn('year', year(col('start_time')))\
                        .withColumn('weekday', date_format(col('start_time'), 'E'))

    # write time table to parquet files partitioned by year and month
    time_table.write.format('parquet').mode('overwrite').save(output_data+'/time/')

    # read in song data to use for songplays table
    song_df=spark.read.parquet(output_data + '/songs/*/*/*.parquet')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =  df.withColumn('songplay_id',monotonically_increasing_id())\
                             .join(song_df,df.song == song_df.title)\
                                .select('songplay_id',
                                        'start_time',
                                        col('userId').alias('user_id'),
                                        'level',
                                        col('song').alias('song_id'),
                                        col('artist').alias('artist_id'),
                                        col('sessionId').alias('session_id'),
                                        'location',
                                        col('userAgent').alias('user_agent'),
                                        year('start_time').alias('year'),
                                        month('start_time').alias('month'))


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkfy"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
