from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Calculating is_return
def get_is_return(type):
	is_return=0
    if type=="RETURN":
        is_return = 1
	return is_return

# Calculating is_order
def get_is_order(type):
	is_order = 0
	if type=="ORDER":
		is_order = 1
	return is_order 

# Calculating total cost in single order
def get_total_cost(items,type):
	total_cost=0
    for item in items:
        total_cost = total_cost + (item['quantity'] * item['unit_price'])
	if type=="ORDER":
		return total_cost
	else:
		return -total_cost   

# Calculating total number of items present in single order
def get_total_item_count(items):
	total_count=0
    for item in items:
        total_count = total_count + item['quantity']
	return total_count  

# Biuld Spark Session
spark = SparkSession.builder.appName("RetailDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
# Read data from Kafka
lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("startingOffsets","earliest").option("subscribe","real-time-project").load()

#Converting the binary values to String using cast function
kafkaDF = lines.selectExpr("cast(key as string)","cast(value as string)")	


#Defining schema for a single order
jsonSchema = StructType() \
	.add("invoice_no",LongType()) \
	.add("country",StringType()) \
	.add("timestamp", TimestampType()) \
	.add("type",StringType()) \
	.add("items", ArrayType(StructType([StructField("SKU",StringType()),StructField("title",StringType()),StructField("unit_price",DoubleType()),StructField("quantity",IntegerType())])))

#Now, extract the value which is in JSON String to DataFrame and convert to DataFrame columns using jsonSchema.
orderStreamDF = kafkaDF.select(from_json(col("value"),jsonSchema).alias("data")).select("data.*")

# Define the UDFs with the utility functions.
add_total_item_count = udf(get_total_item_count, IntegerType())
add_total_cost = udf(get_total_cost, DoubleType())
add_is_order = udf(get_is_order,IntegerType())
add_is_return = udf(get_is_return,IntegerType())

# Calculate additional columns
expandedOrderStreamDF = orderStreamDF \
	.withColumn("total_items",add_total_item_count(orderStreamDF.items)) \
	.withColumn("total_cost",add_total_cost(orderStreamDF.items,orderStreamDF.type)) \
	.withColumn("is_order",add_is_order(orderStreamDF.type)) \
	.withColumn("is_return",add_is_return(orderStreamDF.type))

# Calculate time-country based KPIs
aggStreamByTimeCountry = expandedOrderStreamDF \
		.withWatermark("timestamp", "1 minute") \
		.groupBy(window("timestamp" , "1 minute" , "1 minute"), "country") \
		.agg(sum("total_cost").alias("total_sales_volume"),count("invoice_no").alias("OPM"), avg("is_return").alias("rate_of_return")) \
		.select("window", "country" ,"OPM", "total_sales_volume", "rate_of_return")

#Time-Country based KPI. Saving output files in JSON format.
query = aggStreamByTimeCountry.writeStream \
	.outputMode("append") \
	.format("json") \
	.option("truncate","False") \
	.option("path","/user/root/op2") \
	.option("checkpointLocation","/user/root/cp2") \
	.trigger(processingTime="1 minute") \
	.start()

# Calculate time based KPIs
aggStreamByTime = expandedOrderStreamDF \
		.withWatermark("timestamp", "1 minute") \
		.groupBy(window("timestamp" , "1 minute" , "1 minute"), "country") \
		.agg(sum("total_cost").alias("total_sales_volume"),count("invoice_no").alias("OPM"), avg("is_return").alias("rate_of_return"),(sum("total_cost")/count("type")).alias("avg_transaction_size")) \
		.select("window" , "OPM", "total_sales_volume", "rate_of_return", "avg_transaction_size")

#Time based KPI. Saving output im JSON format.
queryTime = aggStreamByTime.writeStream \
	.outputMode("append") \
	.format("json") \
	.option("truncate","false") \
	.option("path","/user/root/op1") \
	.option("checkpointLocation","/user/root/cp1") \
	.trigger(processingTime="1 minute") \
	.start()

# Printing on Console
query = expandedOrderStreamDF  \
	.select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return") \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.option("truncate","False")  \
	.start()
	
query.awaitTermination()
