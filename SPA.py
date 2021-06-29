# Importing Required Libraries
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode, split, col, from_json ,expr ,when 
from pyspark.sql.functions import acos, cos, sin, lit, toRadians
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType ,TimestampType
import pandas as pd 

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("sampleCustomers").getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    # Read the data from kafka topic Customers
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "Customers") \
        .option("startingOffsets", "earliest") \
        .option("fetchOffset.retryIntervalMs" ,"4000") \
        .load() \
        .selectExpr("CAST(topic AS STRING)", "CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp as STRING)");

    string_df = df.selectExpr("CAST(value AS STRING)")
    
    # Create a schema for the stream data 
    inputStreamSchema = StructType([
        StructField("Customer", StringType()),
        StructField("Clat",  DecimalType(7,4)),
        StructField("Clong", DecimalType(7,4)),
        StructField("Mlat",  DecimalType(7,4)),
        StructField("Mlong", DecimalType(7,4))
        ])
    
    # Select the data present in the column value and apply the schema on it
    dfStream = string_df.withColumn("jsonData", from_json(col("value"), inputStreamSchema)).select("jsondata.*")

    #  Load Mallcustomer.csv to panda data frame from location "/Users/sanrawat/repo/pyspark/Mallcustomer.csv"
    pdf = pd.read_csv( "/Users/sanrawat/repo/pyspark/Mallcustomer.csv" ,header='infer')

    # Convert panda data frame to spark data frame
    sdf = spark.createDataFrame(pdf)

    # Joined streaming data to Customer domain data
    dfJoined = dfStream.join(sdf, expr("""Customer ==  CustomerID"""),  "leftOuter" )
	
    #  Function to calculate the distance between 2 given coordinates
    def distenceCustomer(long_x, lat_x, long_y, lat_y):
        return acos(sin(toRadians(lat_x)) * sin(toRadians(lat_y)) + cos(toRadians(lat_x)) * cos(toRadians(lat_y)) * cos(toRadians(long_x) - toRadians(long_y))) * lit(6371000)
    
    # Distance of Customer from the Mall
    dfDistence = dfJoined.withColumn("mall_distance", distenceCustomer("Clong", "Clat", "Mlong", "Mlat").alias("mall_distance"))

    dfOffers =dfDistence.withColumn("Offer",
                when(( (col("Age")> 19)  & (col("Spending Score (1-100)")>84)) ,"High brands Offer") \
                .when(( (col("Age")> 26)  & (col("Gender") == "Male" )) ,"Pub Offers") \
                .when(( (col("Age")> 19)  & (col("Gender") == "Female" )) ,"Female Offers ") \
                .otherwise("No Offer"))
  
    dfCustomersOfers = dfOffers.select("Customer" , "Offer").where (col("mall_distance") < 100)
    
    # Writing back to kafka 
    dfCustomersOfers.selectExpr("Customer AS key", "to_json(struct(*)) AS value")\
            .writeStream\
            .format("kafka")\
            .outputMode("append")\
            .option("kafka.bootstrap.servers", "localhost:9092")\
            .option("topic", "CustomersOffers")\
            .option("checkpointLocation", "/Users/sanrawat/repo/pyspark/Mallcustomer1")\
            .start()\
            .awaitTermination(5)

    
