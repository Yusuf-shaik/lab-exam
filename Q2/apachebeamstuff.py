# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import json
import logging
import os
import math
import apache_beam as beam
import tensorflow as tf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from kafka import KafkaProducer


def singleton(cls):
  instances = {}
  def getinstance(*args, **kwargs):
    if cls not in instances:
      instances[cls] = cls(*args, **kwargs)
    return instances[cls]
  return getinstance



@singleton
class Model():

  def __init__(self, checkpoint):
    with tf.Graph().as_default() as graph:
      sess = tf.compat.v1.InteractiveSession()
      saver = tf.compat.v1.train.import_meta_graph(os.path.join(checkpoint, 'export.meta'))
      saver.restore(sess, os.path.join(checkpoint, 'export'))

      inputs = json.loads(tf.compat.v1.get_collection('inputs')[0])
      outputs = json.loads(tf.compat.v1.get_collection('outputs')[0])

      self.x = graph.get_tensor_by_name(inputs['image'])
      self.p = graph.get_tensor_by_name(outputs['scores'])
      self.input_key = graph.get_tensor_by_name(inputs['key'])
      self.output_key = graph.get_tensor_by_name(outputs['key'])
      self.sess = sess

class FilterNones(beam.DoFn):
    def process(self, element):
        for k,v in element.items():
            if v is not None:
                yield element
            else:
                return


class Conversions(beam.DoFn):
    def process(self, element):
        temp = element["temp"]
        humd = element["humd"]
        pres = element["pres"]

        elements = {}
        elements["temp"] = temp*1.8+32
        elements["pres"] = pres/6.895
        temp = elements["temp"]
        pres = element["pres"]

        if humd is not None and pres is not None and temp is not None:
            if humd>90:
                element["risk"] = 1
            if pres>=0.2 and humd<=90:
                element["risk"] = 1-math.pow( 2.71828, 0.2-pres)
            if pres < 0.2 and humd <=90:
                element["risk"] = min(1, temp/300)
        return element

        
        


        
        



def riskEstimation(humidity, psi, F):
    if humidity is not None and psi is not None and F is not None:
        if humidity>90:
            return 1
        if psi>=0.2 and humidity<=90:
            return 1-math.pow( 2.71828, 0.2-psi)
        if psi < 0.2 and humidity <=90:
            return min(1, F/300)

def _to_dictionary(line):
    result = {}
    result['key'], result['image'] = line.split(':')
    return result

class ProduceKafkaMessage(beam.DoFn):

    def __init__(self, topic, servers, *args, **kwargs):
        beam.DoFn.__init__(self, *args, **kwargs)
        self.topic=topic;
        self.servers=servers;

    def start_bundle(self):
        self._producer = KafkaProducer(**self.servers)

    def finish_bundle(self):
        self._producer.close()

    def process(self, element):
        try:
            self._producer.send(self.topic, element[1], key=element[0])
            yield element
        except Exception as e:
            raise
            
def run(argv=None):
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--input', dest='input', required=True,
                      help='Input file to process.')
  parser.add_argument('--output', dest='output', required=True,
                      help='Output file to write results to.')
  parser.add_argument('--model', dest='model', required=True,
                      help='Checkpoint file of the model.')
  parser.add_argument('--source', dest='source', required=True,
                      help='Data source location (text|mysql|kafka).')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True;
  with beam.Pipeline(options=pipeline_options) as p:
    
    if known_args.source == 'kafka':
        from beam_nuggets.io import kafkaio
        consumer_config = {"topic": 'Sensors','bootstrap_servers':'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',\
            'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN','sasl_plain_username':'O3SSBJ7WWBGBKIQD',\
            'sasl_plain_password':"EOQtUuMrmDXpL5siyWGTTXwb5m+7lcijyBBuyiWa7Q2n8DJAYYywLpGpoW2ccSqQ",\
                'auto_offset_reset':'latest'}
        server_config = {'bootstrap_servers':'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',\
            'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN','sasl_plain_username':'O3SSBJ7WWBGBKIQD',\
            'sasl_plain_password':"EOQtUuMrmDXpL5siyWGTTXwb5m+7lcijyBBuyiWa7Q2n8DJAYYywLpGpoW2ccSqQ"}
        sensorData = (p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(
            consumer_config=consumer_config,value_decoder=bytes.decode) 
            | 'Deserializing' >> beam.Map(lambda x : json.loads(x[1])))
        filteredValues = (sensorData | "RemoveNones") >> beam.ParDo(FilterNones())
        newValues = (filteredValues |"UpdateValues") >> beam.ParDo(Conversions())
        
        # predictions = (sensorData | 'Prediction' >> beam.ParDo(PredictDoFn(), known_args.model)
        #     | "Serializing" >> beam.Map(lambda x: (None,json.dumps(x).encode('utf8'))));
        predictions |'To kafka' >> beam.ParDo(ProduceKafkaMessage(known_args.output,server_config))
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
