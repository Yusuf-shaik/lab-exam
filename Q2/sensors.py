#TODO: you may need to install some python libraries 
from kafka import KafkaProducer;        # pip install kafka-python
import numpy as np                      # pip install numpy
import json;
import time
import random

data=json.load(open('cred.json'))   #TODO: fill the missing values within cred.json
bootstrap_servers=data['bootstrap_servers'];
sasl_plain_username=data['Api key'];
sasl_plain_password=data['Api secret'];
topicName='Sensors'                        #TODO: fill by topic name

#device distribution profile used to generate random data
DEVICE_PROFILES = {
	"boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091) },
	"denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341) },
	"losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201) },
}
profileNames=["boston","denver","losang"];

#serialize and deserialize functions
def serialize(value):
    return json.dumps(value).encode('utf-8')

def deserialize(value):
    return str(json.loads(value))
    
#create a producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,value_serializer=lambda m: serialize(m))

#generate random data
count = 1
while True:     # stop using Ctrl+C
    #select random profile
    profile_name = profileNames[random.randint(0, 2)];
    profile = DEVICE_PROFILES[profile_name]
    # get random values within a normal distribution of the value
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))
	
    # create dictionary
    msg={"time": time.time(), "profile_name": profile_name, "temperature": temp,"humidity": humd, "pressure":pres};
    
    #randomly eliminate some measurements
    for i in range(3):
        if(random.randrange(0,10)<1):
            choice=random.randrange(0,3)
            if(choice==0):
                msg['temperature']=None;
            elif (choice==1):
                msg['humidity']=None;
            else:
                msg['pressure']=None;
	
    #produce the message into Kafka
    producer.send(topicName, msg);
    
    print(f'sending data to kafka, #{count} ,\tmsg: {msg}')
    count += 1
    time.sleep(.1)
    
producer.close();





