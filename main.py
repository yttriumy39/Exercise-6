from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, FloatType, DateType
from pyspark.sql.functions import mean, date_trunc, count, col, max as sparkmax, date_add
import zipfile
import io
import re
import os
from datetime import datetime

#function to stream zip data with unzipping the files
def zip_extract(x):
    in_memory_data = io.BytesIO(x[1]) #this is an in memory stream
    file_obj = zipfile.ZipFile(in_memory_data, "r") #this reads the zipped stream file
    files = [i for i in file_obj.namelist()] #returns a list/RDD of file objects/pipelined RDD
    return dict(zip(files, [file_obj.open(file).read() for file in files])) #

#decoding bytes data into strings and doing some cleaning
def decode_bytes(a):
    keylist = list(a.keys())
    bytes = a[keylist[0]]
    decoded_string = bytes.decode()
    cleaned_string = re.sub(',"[0-9]', '', decoded_string)
    cleaned_string = re.sub('.0"', '.0', decoded_string)
    split_string_list = cleaned_string.splitlines()
    return split_string_list

#splitting the string data into individual strings for each element 
def list_split(b):
    list1 = []
    list2 = []
    for i in b:
        list2 = i.split(",")
        list1.append(list2)
    return list1

def main():
    spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()
    # your code here
    spark.sparkContext.setLogLevel("WARN")

    zips = spark.sparkContext.binaryFiles("/app/data/*.zip") #this is an RDD object
    files_data = zips.map(zip_extract).collect() #we have a list of two dictionaries (for two files), each

    #there are two files, I kept them separate throughout and only queried one. Files have differen schemas, could be brought together if needed
    #apply above functions to create data for loading into DataFrames
    list_data1 = list_split(decode_bytes(files_data[0]))
    list_data2 = list_split(decode_bytes(files_data[1]))

    #removing the header from the data
    dfdata1 = list_data1[1:]
    dfdata2 = list_data2[1:]

    #this file needed extra data cleaning, ultimately 44 rows get dropped for having the incorrect row length
    finaldfdata1 = []
    for i in dfdata1:
        if len(i)==12:
            finaldfdata1.append(i)

    #conversion to correct data types
    for i in finaldfdata1:
        i[0] = int(i[0])
        i[1] = datetime.strptime(i[1], '%Y-%m-%d %H:%M:%S').date()
        i[2] = datetime.strptime(i[2], '%Y-%m-%d %H:%M:%S').date()
        i[3] = int(i[3])
        i[4] = float(i[4])
        i[5] = int(i[5])
        i[7] = int(i[7])
        if i[11] == '':
            i[11] = int(0.0)
        else:
            i[11] = int(i[11])

    for i in dfdata2:
        i[2] = datetime.strptime(i[2], '%Y-%m-%d %H:%M:%S').date()
        i[3] = datetime.strptime(i[3], '%Y-%m-%d %H:%M:%S').date()
        i[5] = int(i[5])
        if i[7] == '':
            i[7] = int(0.0)
        else:
            i[7] = int(i[7])
        if i[8] == '':
            i[8] = float(0.0)
        else:
            i[8] = float(i[8])
        if i[9] == '':
            i[9] = float(0.0)
        else:
            i[9] = float(i[9])
        if i[10] == '':
            i[10] = float(0.0)
        else:
            i[10] = float(i[10])
        if i[11] == '':
            i[11] = float(0.0)
        else:
            i[11] = float(i[11])

    #writing exact schemas so that data is more usable in the DataFrame
    column_schema1 = StructType([
        StructField('trip_id', IntegerType(), True),
        StructField('start_time', DateType(), True),
        StructField('end_time', DateType(), True),
        StructField('bikeid', IntegerType(), True),
        StructField('tripduration', FloatType(), True),
        StructField('from_station_id', IntegerType(), True),
        StructField('from_station_name', StringType(), True),
        StructField('to_station_id', IntegerType(), True),
        StructField('to_station_name', StringType(), True),
        StructField('usertype', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('birthyear', IntegerType(), True)
        ])

    column_schema2 = StructType([
        StructField('ride_id', StringType(), True),
        StructField('rideabletype', StringType(), True),
        StructField('started_at', DateType(), True),
        StructField('ended at', DateType(), True),
        StructField('start_station_name', StringType(), True),
        StructField('start_station_id', IntegerType(), True),
        StructField('end_station_name', StringType(), True),
        StructField('end_station_id', IntegerType(), True),
        StructField('start_lat', FloatType(), True),
        StructField('start_lng', FloatType(), True),
        StructField('end_lat', FloatType(), True),
        StructField('end_lng', FloatType(), True),
        StructField('member_casual', StringType(), True),
        ])

    #create two dataframes
    df1 = spark.createDataFrame(finaldfdata1,column_schema1)
    df2 = spark.createDataFrame(dfdata2,column_schema2)
 
    #requested to save data down into reports folder
    directory =  os.getcwd() + "/reports"
    if not os.path.exists(directory):
        os.makedirs(directory)

    #average trip duration per day
    avtripdur = (df1.groupBy(date_trunc('day', df1.start_time).alias('date'))
    .agg(mean("tripduration").alias("average_trip_duration")))

    #count the number of trips per day
    nooftrips = (df1.groupBy(date_trunc('day', df1.start_time).alias('date'))
    .agg(count("trip_id").alias("average_no_trips")))

    #count the most coomonly used stations per month
    mostpopstation = (df1.groupBy(date_trunc('month', df1.start_time).alias('month'))
    .agg(count("from_station_id").alias("average_no_trips")))


    #top 3 stations in the past two weeks
    x1 = df1.select(date_add(sparkmax('start_time'),-14)).collect()[0][0]
    lasttwoweeks = df1.filter(date_trunc('day', df1.start_time)>=x1)       
    top3stations = (lasttwoweeks.groupBy(date_trunc('day', df1.start_time).alias('date'), col("from_station_name"))
                     .agg(count("from_station_name").alias("station_count"))
                     .orderBy(col("station_count").asc()))
    
    #show the average time by gender, this will include male, female and blank
    gendertripdur = (df1.groupBy(col("gender").alias('gender'))
    .mean("tripduration").alias("average_no_trips"))

    #top 10 ages of those who take the longest trips and the shortest. First calculate the mean of the trips
    x2 = df1.select(mean('tripduration')).collect()[0][0]
    q6p1df = df1.filter(col("tripduration")>x2)
    q6p2df = df1.filter(col("tripduration")<x2)
    q6p1 = q6p1df.orderBy(col("birthyear").asc()).limit(5)
    q6p2 = q6p2df.orderBy(col("birthyear").asc()).limit(5)

    #write to csv file. The coalesce function slows down the write but means one file per output
    avtripdur.coalesce(1).write.format("csv").save(directory + "/q1")
    nooftrips.coalesce(1).write.format("csv").save(directory + "/q2")
    mostpopstation.coalesce(1).write.format("csv").save(directory + "/q3")
    top3stations.coalesce(1).write.format("csv").save(directory + "/q4")
    gendertripdur.coalesce(1).write.format("csv").save(directory + "/q5")
    q6p1.coalesce(1).write.format("csv").save(directory + "/q6p1")
    q6p2.coalesce(1).write.format("csv").save(directory + "/q6p2")



if __name__ == '__main__':
    main()
