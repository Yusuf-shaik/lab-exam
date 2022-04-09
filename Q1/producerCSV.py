#TODO: you may need to install some python libraries 
from kafka import KafkaProducer;        # pip install kafka-python
import json;
import time
import io;
from avro.io import DatumWriter, BinaryEncoder
import avro.schema          # pip install avro
import pandas as pd         # pip install pandas
import numpy as np          # pip install numpy

#TODO: make sure that time-series-19-covid-combined.csv, schema.avsc
#      and  cred.json are in the same folder

# read the CSV file 
df = pd.read_csv('./time-series-19-covid-combined.csv')
df=df.dropna();
df=df.astype({'Recovered':'int64'})
df = df.reset_index();
print(df)

    
#get the credentials and necessary data for Kafka
data=json.load(open('cred.json'))   #TODO: fill the missing values within cred.json
bootstrap_servers=data['bootstrap_servers'];
sasl_plain_username=data['Api key'];
sasl_plain_password=data['Api secret'];
topicName='newToMySQL'                        #TODO: fill by topic name
schemaID=100007;                         #TODO: set the schema ID.


#initalize the Serializer
schema = avro.schema.parse(open("./schema.avsc").read())
writer = DatumWriter(schema)

#serialize function
def encode(value):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return schemaID.to_bytes(5, 'big')+bytes_writer.getvalue()

#create a producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,value_serializer=lambda m: encode(m))

#produce each record to the topic
for i in range(df.shape[0]):
    d={};
    for c in df.columns:
        if type(df[c][i]) is np.int64:
            d[c]=int(df[c][i])
        else:
            d[c]=df[c][i]
    print(d);
    producer.send(topicName, d);
    time.sleep(0.01);

#cleaning up resources
producer.close();
