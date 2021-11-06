import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import seaborn as sns

spark = SparkSession.builder.getOrCreate()


df = spark.read.format('csv').option("header", True).option('inferSchema', True).load('predictive_maintenance.csv')

df.dtypes

df.count()

df.first()
df.select(F.max('air_temp_kelvin')).show()

df.show(n=5)

df.select('failure_type').distinct().show()

df.groupby('failure_type').count().show()
X = df.select('air_temp_kelvin').toPandas().air_temp_kelvin

sns.displot(X)
