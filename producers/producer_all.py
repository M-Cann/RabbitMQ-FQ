import os
import pika
import json
import time
from openpyxl import Workbook,load_workbook

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

os.chdir("..")
path = "scenarios/scenarios0.txt"
f = open(path, "r")

channel.queue_declare(queue='normal_queue', durable=True)

wb = load_workbook("statistics.xlsx")
ws = wb.active
ws.append([path[10:25]])
wb.save("statistics.xlsx")
for y in f:
    a = y.split('-')
    user = a[0]
    piece = int(a[1])
    channel.queue_declare(queue=user, durable=True)
    for x in range(1, piece+1):
        message = {'User': user,
        'Time': str(time.time()),
        'x':x}
        json_str = json.dumps(message)
        channel.basic_publish(
            exchange='', 
            routing_key=user, 
            body=json_str, 
            properties=pika.BasicProperties(
                delivery_mode=2,
        ))
        channel.basic_publish(
            exchange='', 
            routing_key='normal_queue', 
            body=json_str, 
            properties=pika.BasicProperties(
                delivery_mode=2,
        ))
print("Mesajlar gönderildi")

connection.close()