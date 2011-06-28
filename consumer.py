#!/usr/bin/env python
import pika
import time
import yaml
import json
import functools
import logging
import json
import uuid
import yaml
import copy
import mq_send

from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.websocket import WebSocketHandler



config = yaml.load(open('config.yaml', 'r').read())
logging.basicConfig(level=logging.INFO,
format='%(asctime)s - %(message)s',
datefmt='%Y-%m-%d %H:%M:%S')

HOSTS = {}


class Offering(object):
    def __init__(self, **kwargs):
        [setattr(self, k, v)
            for (k, v) in kwargs.items()]

    def __str__(self):
        return str(self.name)

    def __hash__(self):
        return hash(str(self))

    def __cmp__(self, other):
        return cmp(str(self), str(other))


offerings = [Offering(**i)
    for i in config['offerings']]
offerings_lookup = dict([(str(i), i) for i in offerings])


class Host(object):
    def __init__(self, id):
        self.id = id
        self.vms = []
        self.numcpus = config['host']['numcpus']
        self.mem = config['host']['mem']

    def __iter__(self):
        return iter(self.vms)

    def append(self, item):
        o = offerings_lookup[item['so']]
        self.numcpus -= o.numcpus
        self.mem -= o.mem
        if self.numcpus < 0 or self.mem < 0:
            send_message({'action': 'capacity',
                'key': self.id})
            self.numcpus += o.numcpus
            self.mem += o.mem
            return False
        else:
            self.vms.append(item)
            return True


def send_message(data):
    logging.debug('sending message to listeners: %s' % repr(data))
    msg = unicode(json.dumps(data))
    for i in LISTENERS:
        i.write_message(msg)
    #TODO: send to rabbitmq


def status_message(_key):
    for k, v in HOSTS.items():
        send_message({
            'action': 'stats',
            'key': k,
            'numcpus': (config['host']['numcpus'] - v.numcpus),
            'mem': (float(v.mem) / float(config['host']['mem']))*100 })


def create_host():
    logging.info('adding host')
    _key = str(uuid.uuid4())
    HOSTS.update({_key: Host(_key)})
    send_message({'action': 'new_host', 'key': _key})

    cb = functools.partial(send_message,
        {'action': 'hb', 'key': _key})
    PeriodicCallback(cb, 3 * 1000, ioloop).start()
    status = functools.partial(status_message, _key)
    PeriodicCallback(status, 3 * 1000, ioloop).start()

def delete_host(_key):
    logging.info('deleting host')
    HOSTS.pop(_key)

def create_vm(_key, so):
    vm = {'so': so, 'key': str(uuid.uuid4())}
    if HOSTS[_key].append(vm):
        logging.info('added vm')
        send_message({'key': _key,
            'vm': vm, 'action': 'vm'})
    else:
        logging.info('insufficient capacity on host '+_key+" for offering "+so)

def delete_vm(_key,_vm_key):
    if HOSTS.get(_key):
        for virtual_machine in HOSTS[_key]:
            if virtual_machine['key']==_vm_key:
                so=virtual_machine['so']
                o = offerings_lookup[so]
                HOSTS[_key].vms.remove(virtual_machine)
                HOSTS[_key].numcpus+=o.numcpus
                HOSTS[_key].mem+=o.mem
                logging.info('deleted vm')
                break




connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=config['hostname']))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print 'Consumer ready for work'

def callback(ch, method, properties, body):
    try:
        message=json.loads(body)
        message_type=message['type']
        logging.info("Received message of 'type' %s"  % (message_type,))
        if message_type=='create_vm':
            print "Trying to create VM!"
            try:
                create_vm(message['host'],message['offering'])
            except:
                logging.info("Failed to create vm")
        elif message_type=='create_host':
            print "Trying to start host!"
            try:
                create_host()
            except:
                logging.info("Failed to create host")
    except:
        logging.info("Received message without 'type' ")

    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()
