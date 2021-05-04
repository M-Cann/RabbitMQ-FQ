import os
import pika
import json
import time
from openpyxl import Workbook,load_workbook

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='normal_queue', durable=True)

os.chdir("..")
path = "scenarios/scenarios1.txt"
f = open(path, "r")

wb = load_workbook("statistics.xlsx")
ws = wb.active
ws.append([path[10:20]])
wb.save("statistics.xlsx")
for y in f:
    a = y.split('-')
    user = a[0]
    piece = int(a[1])
    for x in range(1, piece+1):
        message = {'User': user,
        'Time': str(time.time()),
        'x':x}
        json_str = json.dumps(message)
        channel.basic_publish(
            exchange='', 
            routing_key='normal_queue', 
            body=json_str, 
            properties=pika.BasicProperties(
                delivery_mode=2,
        ))
        print(" [x] Sent %r" % message)

connection.close()