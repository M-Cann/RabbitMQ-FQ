import threading 
import pika
import os 
import json
import time
from openpyxl import Workbook,load_workbook

channel = None
queues = []
consumers = []
times = {}

def queue_list():
    path = 'C:\\Program Files\\RabbitMQ Server\\rabbitmq_server-3.8.14\sbin'
    os.chdir(path)
    a = os.popen("rabbitmqctl list_queues")
    writing = a.read()
    line = writing.splitlines()
    global queues
    for i in range (3, len(line)):
        queue_split = line[i].split("\t")
        if not (queue_split[0] in queues):
            queues.append(queue_split[0])

def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)    

def on_channel_open(a):
    global channel
    channel = connection.channel(on_open_callback=queue_declare)

def queue_declare(a):
    global channel
    channel.queue_declare(queue='fair_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    start_consuming()

def start_consuming():
    global channel
    queue_list()
    for queue_name in queues:
        if not (queue_name in consumers) and not(queue_name=='fair_queue'):
            channel.basic_consume(queue_name, on_message)
            consumers.append(queue_name)

def on_message(ch, method, properties, body):
    received_data = json.loads(body)
    user = received_data['User']
    time1 = received_data['Time']
    x = received_data['x']
    message = {'User': user,
    'Time': time1,
    'x': x}
    json_str = json.dumps(message)
    if times.get(user)==None:
        times[user] = time.time()-float(time1)
    channel.basic_publish(
        exchange='', 
        routing_key='fair_queue', 
        body=json_str, 
        properties=pika.BasicProperties(
                delivery_mode=2,
    ))
    channel.basic_ack(delivery_tag=method.delivery_tag)

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)                                 

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    connection.close()
    connection.ioloop.start()
