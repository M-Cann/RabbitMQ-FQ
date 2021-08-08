from re import T
import pika
import time
import json  
import threading  

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
connection2 = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel2 = connection2.channel()
channel.queue_declare(queue='queue_list', durable=True)
channel2.queue_declare(queue='fair_queue', durable=True)
channel.basic_qos(prefetch_count=1)
channel2.basic_qos(prefetch_count=1)

queues = []
old_queues = []
new_queues = []

def basic_consume():
    def callback(ch, method, properties, body): 
        received_data = json.loads(body)
        queue_name = received_data['Queue']
        queues.append(queue_name)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    channel.basic_consume(queue='queue_list', on_message_callback=callback) 
    channel.start_consuming()

def queue_array():
    while True:
        global old_queues
        for i in queues:
            if not(i in old_queues):
                old_queues.append(i)
                new_queues.append(i)

def consume_and_remove():
    while True:
        global new_queues
        for queue_name in new_queues:
            new_queues.remove(queue_name)
            t4 = threading.Thread(target=consume, args=(queue_name))
            t4.start()
            
def consume(queue_name):
    channel2 = connection2.channel()
    channel2.basic_qos(prefetch_count=1)
    def callback(ch, method, properties, body):  
        channel2.basic_publish(
            exchange='', 
            routing_key='fair_queue', 
            body=body, 
            properties=pika.BasicProperties(
                delivery_mode=2,)
        )   
        ch.basic_ack(delivery_tag=method.delivery_tag) 
    channel2.queue_declare(queue=queue_name, durable=True)
    channel2.basic_consume(queue=queue_name, on_message_callback=callback)
    channel2.start_consuming()

t1 = threading.Thread(target=basic_consume)
t1.start()
t2 = threading.Thread(target=queue_array)
t2.start()
t3 = threading.Thread(target=consume_and_remove)
t3.start()

'''
HERYER THREAD OLACAK HERŞEY VE HERYER 
THREAD 
THREAD 
THREAD
'''