from pyspark.sql import SparkSession


spark=SparkSession.builder.master("local").appName("hive table").enableHiveSupport().getOrCreate()
print(spark)
spark.sql("create database if not exists spadlm")
spark.sql("use spadlm")
ss=spark.catalog.listDatabases()
print(ss)
df1=spark.read.csv("file:/home/hduser/sparkdata/nyse_noheader.csv",sep="~",header="false",inferSchema="true").toDF("ex","st","ra")
df1.show()
df1.write.mode("overwrite").saveAsTable("spadlm.tb1")
df1_h=spark.read.table("spadlm.tb1")
df1_h.show()

df_jd=spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/custpayments").option("dbtable","customers").option("user","root").option("password","Root123$").load()

df_jd.show(3)
url_1="jdbc:mysql://localhost:3306/custpayment?user=root&password=Root123$"


