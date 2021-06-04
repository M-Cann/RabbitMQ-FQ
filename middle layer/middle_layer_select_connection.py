import pika
import os 
import json

channel = None
queues = []
consumers = []
message_count = 0

def queue_list():
    global message_count
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
    for j in range (3, len(line)):
        count_split = line[j].split("\t")
        if not(count_split[0]=='fair_queue') and not(count_split[0]=='normal_queue'):
            message_count = message_count + int(count_split[1])

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
    global message_count
    global channel
    queue_list()
    if message_count==0:
        start_consuming()
    for queue_name in queues:
        if not(queue_name=='fair_queue') and not(queue_name=='normal_queue'):
            channel.basic_consume(queue_name, on_message)
            consumers.append(queue_name)

def on_message(ch, method, properties, body):
    global message_count
    channel.basic_publish(
        exchange='', 
        routing_key='fair_queue', 
        body=body, 
        properties=pika.BasicProperties(
                delivery_mode=2,
    ))
    message_count-=1
    channel.basic_ack(delivery_tag=method.delivery_tag)
    if message_count==0:
        start_consuming()

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)                                 

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    connection.close()
    connection.ioloop.start()