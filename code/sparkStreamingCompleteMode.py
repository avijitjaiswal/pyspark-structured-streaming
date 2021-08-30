import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                                .appName("spaarkStreamingCompleteMode")\
                                .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("lsoa_code",StringType(),True),\
                        StructField("borough",StringType(),True),\
                        StructField("major_category",StringType(),True),\
                        StructField("minor_category",StringType(),True), \
                        StructField("value", StringType(), True),\
                        StructField("year",StringType(),True), \
                        StructField("month", StringType(), True)])

    fileStreamDF = sparkSession.readStream\
                                .option("header","true")\
                                .option("maxFilesPerTrigger",1)\
                                .schema(schema)\
                                .csv("../datasets/droplocation")

    print(" ")
    print("Is the stream ready?")
    print(fileStreamDF.isStreaming)

    print(" ")
    print("Schema of the incoming stream: ")
    print(fileStreamDF.printSchema())

    recordsPerBorough = fileStreamDF.groupBy("borough")\
                                    .count()\
                                    .orderBy("count",ascending=False)

    query = recordsPerBorough.writeStream\
                        .outputMode("complete")\
                        .format("console")\
                        .option("trucate","false")\
                        .option("numRows","30")\
                        .start()\
                        .awaitTermination()
