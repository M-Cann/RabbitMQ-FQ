import os
import subprocess
import pika
import requests
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queues = []

def queue_list1(user='guest', password='guest', host='localhost', port=15672, virtual_host=None):
    start = time.time()
    url = 'http://%s:%s/api/queues/%s' % (host, port, virtual_host or '')
    response = requests.get(url, auth=(user, password))
    global queues
    queues = [q['name'] for q in response.json()]  
    finish = time.time()
    print(queues)
    print(finish-start)
    queues.clear()
    
def queue_list2():
    start = time.time()
    path = 'C:\\Program Files\\RabbitMQ Server\\rabbitmq_server-3.8.14\sbin'
    os.chdir(path)
    a = os.popen("rabbitmqctl list_queues")
    writing = a.read()
    line = writing.splitlines()
    for i in range (3, len(line)):
        queue_split = line[i].split("\t")
        queues.append(queue_split[0])
    finish = time.time()
    print(queues)
    print(finish-start)
    queues.clear()

queue_list1()
queue_list2()