import os 
import pika
import json  
import threading

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='fair_queue', durable=True)
channel.basic_qos(prefetch_count=1)

queues = []
message_count=0

def queue_list():
    global message_count
    path = 'C:\\Program Files\\RabbitMQ Server\\rabbitmq_server-3.8.17\sbin'
    os.chdir(path)
    a = os.popen("rabbitmqctl list_queues")
    writing = a.read()
    line = writing.splitlines()
    global queues
    for i in range (3, len(line)):
        queue_split = line[i].split("\t")
        if not (queue_split[0] in queues):
            queues.append(queue_split[0])
    for j in range (3, len(line)):
        count_split = line[j].split("\t")
        if not(count_split[0]=='fair_queue') and not(count_split[0]=='normal_queue'):
            message_count += int(count_split[1])
    
def basic_consume():
    while True:
        queue_list()
        def callback(ch, method, properties, body):
            global message_count            
            channel.basic_publish(
                    exchange='', 
                    routing_key='fair_queue', 
                    body=body, 
                    properties=pika.BasicProperties(
                            delivery_mode=2,)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            message_count-=1
            if message_count==0:
                channel.stop_consuming()
                return basic_consume
        for queue_name in queues:
            if not(queue_name=='fair_queue') and not(queue_name=='normal_queue'):
                channel.basic_consume(queue=queue_name, on_message_callback=callback)
        channel.start_consuming()

basic_consume()

'''
Gelen kuyruktaki kuyruklara bakıp olmayanları tespit edicek
Bulunan kuyruğa consumer atayabilecek
'''

"""
İstatistik tablosunu doldur
Farklı istatistik tablosuna diğer istatistikleri yazdır 
İstatistik tablosunu renklendir 
Farklı çözümleri kontrol et
Kodu temizle
Ortalama hesaplamasını değiştirdim
"""

"""
Seleryde yüksek bellekli conusumerlar çalıştırıldığında consumer rabbitMQ dan düşüyor ancak çalışmaya devam ediyor
Kopan consumerların tekrar çalıştırılması
RabbitMQ üzerinden consumer kontrolü 
Kopan kuyruktaki IP ile consumer bulunması
"""