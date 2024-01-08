# Databricks notebook source
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------

spark = SparkSession.builder \
    .appName('AzureDataPipeline') \
    .config('soark.jars','/path/to/sqljdbs42.jar') \
    .getOrCreate()       

# COMMAND ----------

azure_sql_url = 'jdbc:sqlserver://your-sql-server.database.windows.net:1433;database:your_database'

# COMMAND ----------

azure_sql_properties = {
    'user': spark.conf.get('your_secret_scope_name') + '/your_secret_name',
    'password': spark.conf.get('your_secret_scope_name') + '/your_secret_name',
    'driver': 'com.microsoft.sqlserver.jdbc.SqlServerDriver' 
}

# COMMAND ----------

synapse_analytics_url = 'jdbc:sqlserver://your_synapse-analytics.database.windows.net:1433;database=your_datawarehouse'

# COMMAND ----------

synapse_analytics_properties = {
    "user": spark.conf.get('your_secret_scope_name') + '/your_secret_name',
    "password": spark.conf.get('your_secret_scope_name') + '/your_secret_name' ,
    'driver' : "com.microsoft.sqlserver.jdbc.SqlServerDriver"
}

# COMMAND ----------

for key , value in synapse_analytics_properties.items():
    synapse_analytics_properties[key] = spark.conf.get(value)

# COMMAND ----------

for key , value in synapse_sql_properties.items():
    azure_sql_properties[key] = spark.conf.get(value)

# COMMAND ----------

spark.conf.set('spark.sql.isolationLevel','Read_COMMITED')

# COMMAND ----------

query = '(select * from your_table) as data'
df = spark.read.jdbc(url = azure_sql_url , table = query , properties = azure_sql_properties , column = 'your_partition_column' , lowerBound = 1 , upperBound = 100 , numPartition = 10 )

# COMMAND ----------

df_transformed = df.select('col1','col2',col('col3').cast(IntegerType())).filter('col1' > 0 )

# COMMAND ----------

df_transformed.show()

# COMMAND ----------

try: 
    df_transformed.write \
        .format('com.databricks.spark.sqldw') \
        .option('url' , synapse_analytics_url) \
        .option('dbtable' , 'your_destination_table') \
        .option('forward_spark_azure_storage_credentials' , 'True') \
        .mode('overwrite') \ 
        .save()

    print('Transaction commited successfully')
expect Exception as e:

     print(f"Error:"{str(e)})
     print("Rolling back the transaction")                      

# COMMAND ----------


