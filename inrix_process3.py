from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, expr, when
from pyspark.sql.functions import date_format
spark = SparkSession.builder.appName("inrix_process").getOrCreate()

schema = StructType([
StructField("Segment", IntegerType()),
StructField("C_value", StringType()),
StructField("segco", IntegerType()),
StructField("Score", IntegerType()),
StructField("Speed", IntegerType()),
StructField("Average", StringType()),
StructField("Ref", DoubleType()),
StructField("Travel", StringType()),
StructField("time", StringType()),
StructField("ct", DateType()),
])

#df = spark.read.format("csv").option("header", "false").load("/reactor/INRIX_old/2018/")
df=spark.read.csv("hdfs://rpm01.intrans.iastate.edu:8020/reactor/inrix_partitioned_2018/2018/",header='false',schema=schema)
#df = spark.read.format("csv").option("header", "false").load("/reactor/INRIX_old/2018/*")
from pyspark.sql import Window
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, udf, to_timestamp
date_format = "yyyy-MM-dd HH:mm:ss"
df_date=df.withColumn("timestamp", to_timestamp("time", date_format))
from pyspark.sql import functions as f
df_date=df_date.withColumn('D', f.when(f.col('Speed') < 35, 1).otherwise(0))
from pyspark.sql import Window as Window
w = Window.partitionBy("Segment").orderBy("timestamp")
from pyspark.sql import functions as func
df_try = df_date.withColumn('timestamp_long',df_date.timestamp.astype('Timestamp').cast("long"))
w2 = Window.partitionBy('Segment').orderBy('timestamp_long').rangeBetween(-60*15,0)
#w2.printSchema()
df_try = df_try.withColumn('occurrences_in_15_min',f.sum('D').over(w2))
from pyspark.sql.types import *
add_bool = udf(lambda col: 1 if col>14 else False, BooleanType())
df_try = df_try.withColumn('already_occured',add_bool('occurrences_in_15_min'))
add_bool_1 = udf(lambda col: 1 if col>15 else 0, IntegerType())
df_try = df_try.withColumn('indicator',add_bool_1('occurrences_in_15_min'))
from pyspark.sql import functions as f
w4 = Window.partitionBy('Segment').orderBy('timestamp')
from pyspark.sql.functions import col, expr, when
df_try_2=df_try.withColumn("p",when((df_try.indicator==1),f.lag(df_try.timestamp,15).over(w4)).otherwise("null"))
df_congestion = df_try_2.filter(df_try_2.indicator==1)
#df_congestion.count()
#print(df_congestion.count())
df_congestion.repartition(1).write.format("csv").save("/reactor/inrix-process5/congestion12/", header=True)

from pyspark.sql.functions import date_format
data=df_try_2.withColumn("date",date_format('timestamp', 'yyyy-MM-dd')).withColumn("time_separated",date_format('timestamp', 'HH:mm:ss'))
time_partition= Window.partitionBy('Segment', 'time_separated')
data1=data.withColumn('occurance', f.sum('indicator').over(time_partition))
#data1.printSchema()
#time_partition= Window.partitionBy('time_separated')
data1=data1.withColumn('occurance_all', f.count('date').over(time_partition))


data2=data1.withColumn('occurance_p',f.col('occurance')/f.col('occurance_all'))
w2 = Window.partitionBy('Segment').orderBy('timestamp_long').rangeBetween(-60*15,0)
data3 = data2.withColumn('prob_in_15_min',f.sum('occurance_p').over(w2))
data4 = data3.withColumn('RC_indicator', f.when((data3.occurrences_in_15_min >15)& (data3.prob_in_15_min>3),1).otherwise(0) )
#print("RC")
#print(data4.count())
w4 = Window.partitionBy('Segment').orderBy('timestamp')
data5=data4.withColumn("RC_start_time",when((data4.RC_indicator==1),f.lag(data4.timestamp,15).over(w4)).otherwise("null"))
#print("additional RC")
#print(data5.count())
#data5.repartition(1).write.format("csv").save("/reactor/inrix-process5/RC_test1/", header=True)
data6=data5.filter(data5.RC_indicator==1)
#print("filetring RC")
#print(data6.count())
data6.repartition(1).write.format("csv").save("/reactor/inrix-process5/RC_final12/", header=True)
