import pika
import sys
import yaml
import json

config = yaml.load(open('config.yaml', 'r').read())

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=config['hostname']))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
def send(message):
    channel.basic_publish(exchange='',
                          routing_key='task_queue',
                          body=json.dumps(message),
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent,
                             content_type='application/json'
                          ))
