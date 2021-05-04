import os
import pika
import json
import time
import requests    
import threading 

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

consumers = []
times = {}

channel.queue_declare(queue='fair_queue', durable=True)

def queue_list(user='guest', password='guest', host='localhost', port=15672, virtual_host=None):
    url = 'http://%s:%s/api/queues/%s' % (host, port, virtual_host or '')
    response = requests.get(url, auth=(user, password))
    global queues
    queues = [q['name'] for q in response.json()]

channel.basic_qos(prefetch_count=1)

def start_consume():
    while True:
        time.sleep(0.5)
        channel.start_consuming()

def basic_consume():
    while True:
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
        for queue_name in queues:
            global consumers
            if not(queue_name in consumers) and not(queue_name=='normal_queue') and not(queue_name=='fair_queue'):
                channel.basic_consume(queue=queue_name, on_message_callback=callback, consumer_tag=queue_name) 
                consumers.append(queue_name)

def statistics():
    while True:
        inpt = input()
        if inpt == 'a':
            print(times)

t1 = threading.Thread(target=basic_consume)
t1.start()
t2 = threading.Thread(target=start_consume)
t2.start()
t3 = threading.Thread(target=statistics)
t3.start()