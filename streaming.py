import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

lines = spark.readStream.format('socket').option('host', 'localhost').option('port', 9999).load()
words = lines.select(
    F.explode(
        F.split(lines.value, ' ')
    ).alias('word')
)

wordCounts = words.groupBy('word').count()

query = wordCounts.writeStream.outputMode('complete').format('console').start()

query.awaitTermination()
