import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                                .appName("spaarkStreamingAppendMode")\
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
                                .schema(schema)\
                                .csv("../datasets/droplocation")

    print(" ")
    print("Is the stream ready?")
    print(fileStreamDF.isStreaming)

    print(" ")
    print("Schema of the incoming stream: ")
    print(fileStreamDF.printSchema())

    trimmedDF = fileStreamDF.select(fileStreamDF.borough,
                                    fileStreamDF.year,
                                    fileStreamDF.month,
                                    fileStreamDF.value)\
                            .withColumnRenamed(
                                "value",
                                "convictions"
                                )

    query = trimmedDF.writeStream\
                        .outputMode("append")\
                        .format("console")\
                        .option("trucate","false")\
                        .option("numRows","30")\
                        .start()\
                        .awaitTermination()
