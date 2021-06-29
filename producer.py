# Importing Required Libraries
from time import sleep
from json import dumps
from kafka import KafkaProducer
from random import randint , uniform
import time

# Kafka producer instance - Connected to the Local Server
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

# Longitude & Latitude (location of the Mall)
Mlong =28.5289 
Mlat=77.2195

while True :
    Customer = str(randint(1, 201)) #Randomly generated Customers for the feed
    Clong =str(uniform(28.5200, 28.5392))[:7] #Customer's location Longitude - Randomly generated for the feed
    Clat =str(uniform(77.2100 ,77.2300 ))[:7] #Customer's location Longitude - Randomly generated for the feed
    data = {"Customer": Customer, "Clong": Clong , "Clat": Clat  ,"Mlong" : Mlong , "Mlat" : Mlat} #Data format
    producer.send("Customers", value=data) #Sending Customer data to Kafka Topic Customers
    time.sleep(.5)
