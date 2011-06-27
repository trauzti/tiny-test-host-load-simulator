#!/usr/bin/env python
# -*- coding: utf-8 -

import functools
import logging
import json
import uuid
import yaml
import copy
import time

from tornado.web import authenticated
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.web import RequestHandler, Application
from tornado.websocket import WebSocketHandler


logging.basicConfig(level=logging.INFO,
format='%(asctime)s - %(message)s',
datefmt='%Y-%m-%d %H:%M:%S')


LISTENERS = []
HOSTS = {}
ioloop = IOLoop.instance()

config = yaml.load(open('config.yaml', 'r').read())


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

class BaseHandler(RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("user")

class MainHandler(BaseHandler):
    def get(self):
        if not self.current_user=="godzilla":
            self.redirect("/login")
            return
        self.render("index.html",
            title="Host simulator",
            path='localhost:8888',
            offerings=offerings)
    def post(self):
        if not self.current_user=="gogo":
            self.redirect("/login")
            return
        action = self.request.arguments['action'][0]

        if action == 'host':
            create_host()
        if action == 'logout':
            self.clear_all_cookies()
            self.redirect("/login")
            return
        elif action == 'delete_host':
            try:
                delete_host(self.request.arguments['host'][0])
            except:
                pass
        elif action == 'vm':
            host = self.request.arguments['host'][0]
            so = self.request.arguments['so'][0]
            create_vm(host, so)
        elif action == 'delete_vm':
            host = self.request.arguments['host'][0]
            vmid = self.request.arguments['vmid'][0]
            try:
                delete_vm(host, vmid)
            except:
                pass


class LoginHandler(BaseHandler):
    def get(self):
        self.write('<html><body><form action="/login" method="post">'
                   'Password: <input type="text" name="name">'
                   '<input type="submit" value="Sign in">'
                   '</form></body></html>')

    def post(self):
        self.set_secure_cookie("user", self.get_argument("name"))
        self.redirect("/")


class RealTimeHandler(WebSocketHandler):
    def open(self):
        LISTENERS.append(self)

        def init_stack():
            for k, v in HOSTS.items():
                yield {'action': 'new_host', 'key': k}
                for vm in v:
                    yield {'key': k, 'action': 'vm', 'vm': vm}

        for data in init_stack():
            self.write_message(unicode(json.dumps(data)))

    def on_message(self, message):
        pass

    def on_close(self):
        LISTENERS.remove(self)

def start_hosts(number):
    for i in range(number):
        create_host()

def start_vms(number):
    for m in range(number):
        host_number_id=copy.copy(HOSTS.keys()[number-1-m])
        for i in range(5):
            create_vm(host_number_id,'m1.xlarge')

application = Application([
    (r"/", MainHandler),
    (r"/actions/", RealTimeHandler),
    (r"/login", LoginHandler),
], cookie_secret="61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=")

run_me=True
#run_me=False

if __name__ == "__main__":
    application.listen(8888)
    start_hosts(8)
    start_vms(8)
    if run_me:
        ioloop.start()
