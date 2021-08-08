import os
import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='queue_list', durable=True)

def queues_list_publish(i):
    os.chdir("..")
    path = "queues list/queue list "+str(i)+".txt"
    f = open(path, "r")
    for queue in f:
        queue_name=queue.split('\n')
        message = {'Queue': queue_name[0],}
        json_str = json.dumps(message)
        channel.basic_publish(
            exchange='', 
            routing_key='queue_list', 
            body=json_str, 
            properties=pika.BasicProperties(
                delivery_mode=2,)
        )
    print("Queues list "+str(i)+" mesajları gönderildi.")

queues_list_publish(1)
channel.close()    