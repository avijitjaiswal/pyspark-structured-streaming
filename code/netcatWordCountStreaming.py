import findspark
findspark.init()
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv)!=3:
        print("Usage: spark-submit netcatWordCountStreaming.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession\
        .builder\
        .appName("NetCatWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    lines = spark\
        .readStream\
        .format("socket")\
        .option("host",host)\
        .option("port", port)\
        .load()

    words = lines.select(
        explode(
            split(lines.value," ")
        ).alias("word")
    )

    wordCounts = words.groupBy("word")\
                    .count()

    query = wordCounts.writeStream\
                    .outputMode("complete")\
                    .format("console")\
                    .start()

    query.awaitTermination()