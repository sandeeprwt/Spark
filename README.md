# Spark
This was college project to simulate streaming

   In the real-world scenario, the app installed in the customer’s mobile phone will capture the customer’s GPS co-ordinates and will pass it to the Kafka.
 Kafka Topic, which is used to store and publish records, has been created to store the customer ID, and GPS co-ordinates (Longitude, and Latitude).
 As the topics are created, for this assignment, on the local server, the replication factor and partition has been set to 1


Setup 
crate kfka queue 

    kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Customers

    kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic CustomersOffers

Running spark job 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 SPA.py

******************************
SAMPLE STREAM DATA

Sample date: {&#39;Customer&#39;: &#39;66&#39;, &#39;Clong&#39;: &#39;28.5285&#39;, &#39;Clat&#39;: &#39;77.2197&#39;, &#39;Mlong&#39;: 28.5289, &#39;Mlat&#39;:
77.2195}
Customer – Customer ID
Clong – Customer’s Location – Longitude field
Clong – Customer’s Location – Latitude field

**********************

Spark Python file: SPA.py

Stream  Kafka “Customers” topic.

Create stream data frame

Using pandas create data frame “mallcustomer.csv”

Convert panda data frame to spark dataframe

Define utility function distance_difference_calculator that take customer and mall

coordinates and return distance in meter

Join both the data frames (stream and panda)on customer

Apply the utility function distance_difference_calculator for getting distance of

customer from mall

Using when created case statement create offer

Filter result-set by distance

Write final processed result(stream) to Kafka topic CustomersOffers

Assumptions:

 Random GPS coordinates considered for Mall location
 
 Aerial distance has been considered to calculate the distance between 2coordainates
