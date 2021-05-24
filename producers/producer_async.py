import pika
import os 
import json
import time
from openpyxl import Workbook,load_workbook

os.chdir("..")
path = "scenarios/scenarios0.txt"
f = open(path, "r")

wb = load_workbook("statistics.xlsx")
ws = wb.active
ws.append([path[10:25]])
wb.save("statistics.xlsx")

channel = None

def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)

def on_channel_open(a):
    global channel
    channel = connection.channel(on_open_callback=queue_declare)

def queue_declare(a):
    global channel
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
    print("Mesajlar gönderildi")
    connection.close()

parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)                                 

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    connection.close()
    connection.ioloop.start()
