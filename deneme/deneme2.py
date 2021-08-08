import pika
import time
import json  
import threading   
'''
connection1 = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel1 = connection1.channel()
channel1.queue_declare(queue='queue_list', durable=True)
channel1.basic_qos(prefetch_count=1)

def basic_consume():
    def callback(ch, method, properties, body): 
        received_data = json.loads(body)
        queue_name = received_data['Queue']
        queues.append(queue_name)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    channel1.basic_consume(queue='queue_list', on_message_callback=callback) 
    channel1.start_consuming()
'''
queues = []
old_queues = []
new_queues = []
channel=None

def queue_array():
    while True:
        global old_queues
        for i in queues:
            if not(i in old_queues):
                old_queues.append(i)
                new_queues.append(i)

#t1 = threading.Thread(target=basic_consume)
#t1.start()
t2 = threading.Thread(target=queue_array)
t2.start()

def on_open(connection):
    print('On Open')
    connection.channel(on_open_callback=on_channel_open)    

def on_channel_open(a):
    global channel
    print('On Cannel Open')
    channel = connection.channel(on_open_callback=queue_declare)

def queue_declare(a):
    global channel
    print('Queue Declare')
    start_consuming()

def start_consuming():
    print('Start Consuming')
    print(new_queues)
    time.sleep(0.2)
    global channel
    channel.basic_consume('queue_list', callback) 
    for queue_name in queues:
        if not(queue_name=='fair_queue') and not(queue_name=='normal_queue'):
            print(queue_name)
            channel.basic_consume(queue_name, on_message)
            new_queues.remove(queue_name)
    #start_consuming()

def callback(ch, method, properties, body): 
    received_data = json.loads(body)
    queue_name = received_data['Queue']
    print(queue_name)
    queues.append(queue_name)
    channel.basic_ack(delivery_tag=method.delivery_tag)

def on_message(ch, method, properties, body):
    global message_count
    channel.basic_publish(
        exchange='', 
        routing_key='fair_queue', 
        body=body, 
        properties=pika.BasicProperties(
                delivery_mode=2,
    ))
    channel.basic_ack(delivery_tag=method.delivery_tag)
    if len(new_queues)>0:
        start_consuming()

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)                                 

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    connection.close()
    connection.ioloop.start()