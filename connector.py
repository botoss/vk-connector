#!/usr/bin/env python3
import threading, logging, time
import multiprocessing
import requests
import random
import json
import re
import uuid

from configparser import ConfigParser
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer

API_VERSION = '5.65'

class VkListener(threading.Thread):
    daemon = True

    def run(self):

        ts, server, key = self.getTsServerKey()

        while True:
            try:
                url = "http://%s?act=a_check&key=%s&ts=%s&wait=25&mode=2&version=0" %(server, key, ts)
                res = requests.get(url)
                data = json.loads(res.text)
                
                if 'error' in data: 
                  print(data)
                  
                ts=data['ts']
                
                for update in data['updates']:
                  if(update[0]==4):
                    if(update[6]!=''):                        
                      if(update[6][0]=='!' or update[6][0]=='/'):
                        t = threading.Thread(target=self.handler, args=(update))
                        t.start()

            except ValueError:
                logging.error('ValueError')
                pass
            except KeyError:
                logging.error('KeyError')
                print("KeyError")
                ts, server, key = self.getTsServerKey()
                pass 
            except requests.exceptions.ConnectionError as e:
                print(datetime.now())
                print(e)
                logging.error('ConnectionError')
                time.sleep(10)
                ts, server, key = self.getTsServerKey()
                pass
            except Exception as e:
                print("exception")
                logging.exception("Exception")
                pass

    def getTsServerKey(self):
        print("getTsServerKey")
        url = u"https://api.vk.com/method/messages.getLongPollServer?use_ssl=1&v=5.44&need_pts=0&access_token="+ token
        try:
            res = requests.get(url)
            data = json.loads( res.text )
            server=data['response']['server'] 
            ts=data['response']['ts']
            key=data['response']['key']
            print(ts)
            return ts, server, key
      
        except requests.exceptions.ConnectionError as e:
            print(datetime.now())
            print(e)
            logging.error('Another ConnectionError')
            time.sleep(20)
            ts, server, key = self.getTsServerKey()
            return ts, server, key
      
        except Exception as e:
            print(datetime.now())
            print(e)
            logging.exception("Exception in getTsServerKey")
            ts, server, key = self.getTsServerKey()
            return ts, server, key

    def handler(self, *args):
        
        code = args[0] 
        message_id = args[1]
        flags = args[2]
        from_id = args[3]
        timestamp = args[4]
        subject = args[5]
        text = args[6]
        attachments = args[7]

        if('from' in attachments):
            user_id = int(attachments['from'])
        else:
            user_id = from_id
        
        space = text.find(' ')
        if space == -1:
            keyword = text[1:]
        else:
            keyword = text[1:space]
        args = text.split()[1:]

        message_json = {}
        message_json['connector-id'] = 'vk'
        message_json['command'] = keyword
        message_json['params'] = args

        producer = KafkaProducer(bootstrap_servers=kafka_address+':'+kafka_port)
        key = str.encode(str(uuid.uuid4()))
        producer.send('to-module', str.encode(json.dumps(message_json)), key=key)

        global messages_dict

        messages_dict[key] = from_id

        time.sleep(20)


class VkSender(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=kafka_address+':'+kafka_port, group_id='vk-connector')
        consumer.subscribe(['to-connector'])

        for message in consumer:
            key = message.key
            message_string = message.value.decode("utf-8") 
            message_json = json.loads(message_string)

            global messages_dict
            response = json.loads(message.value.decode('utf-8'))['text']

            try:
                send_to = messages_dict[key]
                self.sendMessage(send_to, response)
                del(messages_dict[key])
            except:
                pass
            
            
    def sendMessage(self, user_id, message):
        guid = random.randint(1, 1000)
          
        url = "https://api.vk.com/method/messages.send"
        if(user_id<2000000000):
            params = {
                      'user_id': user_id,
                      'message': message,
                      'access_token': token,
                      'v': API_VERSION,
                      }
        else:
            user_id=user_id-2000000000
            params = {
                      'chat_id': user_id,
                      'message': message,
                      'access_token': token,
                      'v': API_VERSION,
                      }
          
        res = requests.post(url, params=params)
        data = json.loads(res.text)
        response = data['response']
            
        if 'error' in data: 
            print(data)
        return response


def main():

    tasks = [
        VkListener(),
        VkSender()
    ]

    for t in tasks:
        t.start()

    while True:
        time.sleep(10)
        

if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARNING
        )

    config = ConfigParser()
    configName = "/etc/botoss/vk.conf"
    config.read(configName)
    token = config['main']['token']

    messages_dict = {}

    kafka_address = config['main']['kafka-address']
    kafka_port = config['main']['kafka-port']

    main()
