import os
import pika
import json
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

def scenarios_publish(i):
    os.chdir("..")
    path = "scenarios/scenarios"+str(i)+".txt"
    f = open(path, "r")
    for x in f:
        a = x.split('-')
        user = a[0]
        piece = int(a[1])
        channel.queue_declare(queue=user, durable=True)
        for x in range(1, piece+1):
            message = {'User': user,
            'Time': time.time(),
            'x':x}
            json_str = json.dumps(message)
            channel.basic_publish(
                exchange='', 
                routing_key=user, 
                body=json_str, 
                properties=pika.BasicProperties(
                    delivery_mode=2,)
            )
    print("Seneryo "+str(i)+" mesajları gönderildi.")

scenarios_publish(0)
channel.close()      