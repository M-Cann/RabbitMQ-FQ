import os
import pika
import json
import time
import requests    
import threading 
import subprocess
import functools

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queues = []
consumers = []
times = {}

count=0

def queue_list(user='guest', password='guest', host='localhost', port=15672, virtual_host=None):
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

channel.queue_declare(queue='fair_queue', durable=True)
channel.basic_qos(prefetch_count=1)

def start_consume():
    while True:
        time.sleep(0.5)
        channel.start_consuming()

def basic_consume():
    while True:
        global consumers
        global count
        queue_list()
        def callback(ch, method, properties, body):
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
            ch.basic_ack(delivery_tag=method.delivery_tag)
        count = count + 1
        for queue_name in queues:  
            if not(queue_name=='normal_queue') and not(queue_name=='fair_queue'):
                consumer_tag=str(queue_name)+str(count)
                channel.basic_consume(queue=queue_name, on_message_callback=callback, consumer_tag=consumer_tag)                 
                consumers.append(consumer_tag)

def statistics():
    while True:
        inpt = input()
        if inpt == 'p':
            queue_list()
            for queue in queues:
                if not(queue=='normal_queue') and not(queue=='fair_queue'):
                    channel.queue_delete(queue=queue)
            consumers.clear()

t1 = threading.Thread(target=basic_consume)
t1.start()
t2 = threading.Thread(target=start_consume)
t2.start()
t3 = threading.Thread(target=statistics)
t3.start()

#Kodun tamamını tekrar çalıştırılabilir yap.
#"Channle is closed." hatısı yüzünden çalışmıyor.
#channel.add_on_cancel_callback komutunu dene