import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.format('csv').option("header", True).option('inferSchema', True).load('predictive_maintenance.csv')

df.show(n=5)


df.printSchema()


def add_air_index(df):
    air_mean = df.agg(F.mean(df['air_temp_kelvin']).alias('air_mean')).collect()[0]['air_mean']
    df = df.withColumn('air_index', df['air_temp_kelvin'] - air_mean)
    return df

df = add_air_index(df)

df.rdd.map(lambda x: x.type.lower()).collect()

df.filter(df['air_index'] > 0).sample(fraction=0.1).show(n=5)

df.select(F.max(df['air_index'])).show(n=5)


df.count()

df.select('failure_type').distinct().show()

df.filter(df.)
