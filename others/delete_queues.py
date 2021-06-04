import os 
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queues=[]

def queue_list():
    global queues
    path = 'C:\\Program Files\\RabbitMQ Server\\rabbitmq_server-3.8.14\sbin'
    os.chdir(path)
    a = os.popen("rabbitmqctl list_queues")
    writing = a.read()
    line = writing.splitlines()
    for i in range (3, len(line)):
        queue_split = line[i].split("\t")
        if not (queue_split[0] in queues):
            queues.append(queue_split[0])

queue_list()

for queue_name in queues:
    if not(queue_name=='normal_queue') and not(queue_name=='fair_queue'):
        channel.queue_delete(queue_name)