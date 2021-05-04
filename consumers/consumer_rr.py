import os 
import pika
import time
import json
import requests    
import threading   
from openpyxl import Workbook,load_workbook

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
os.chdir("..")

start_times = {}
start_times2 = {}
receive_times = {}
finish_times = {}
statistics_dic = {}
queues = []
consumers = []
process_times = []
proc_time = 0.0
toplam = 0

excel_receive_times = []
excel_start_times = []
excel_first_messages = {}

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
            receive_time = time.time()  
            received_data = json.loads(body)
            user = received_data['User']
            start_time = received_data['Time']
            excel_receive_times.append(receive_time)
            excel_start_times.append(start_time)
            if excel_first_messages.get(user)==None:
                excel_first_messages[user]=receive_time-float(start_time)
            if statistics_dic.get(user)==None:
                statistics_dic[user] = 1
            else:
                statistics_dic[user] = statistics_dic.get(user) + 1
            if start_times.get(user)==None:
                receive_times[user] = receive_time
                start_times[user] = start_time
                start_times2[user] = start_time
            print(" [x] Received", body, "receive time:", receive_time)
            time.sleep(proc_time)     
            print(" [x] Done")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            finish_time = time.time()
            finish_times[user] = finish_time
            procces_time = finish_time-(receive_time+proc_time)
            process_times.append(procces_time)
        for queue_name in queues:
            global consumers
            if not (queue_name in consumers) and not (queue_name=='normal_queue'):
                channel.basic_consume(queue=queue_name, on_message_callback=callback, consumer_tag=queue_name) 
                consumers.append(queue_name)

def get_key(val):
    for key, value in excel_first_messages.items():
         if val == value:
             return key

def excel_statics():
    while True:
        inpt = input()
        if inpt == 'p':
            wb = load_workbook("statistics.xlsx")
            ws = wb.active
            toplam = 0
            ilk_mesaj = excel_receive_times[0]-float(excel_start_times[0])
            geçen_süre = excel_receive_times[len(excel_receive_times)-1]-float(excel_start_times[0])
            print(excel_first_messages)
            count = 1
            for row in ws:
                if row[2].value==None:
                    cell2 = 'C'+str(count)
                    cell4 = 'E'+str(count)
                    cell7 = 'H'+str(count)
                    cell8 = 'I'+str(count)
                    ws[cell2] = geçen_süre/len(excel_receive_times)
                    ws[cell4] = ilk_mesaj
                    ws[cell7] = get_key(max(excel_first_messages.values()))
                    ws[cell8] = max(excel_first_messages.values())
                count = count + 1
            wb.save("statistics.xlsx")
            excel_first_messages.clear()
            excel_receive_times.clear()
            excel_start_times.clear()

def statistics():
    while True:
        inpt = input()
        if inpt == 'w':
            for i in receive_times:
                print(i, " consumera ulaşma süresi: ", receive_times[i]-float(start_times2[i]), sep='')
            start_times2.clear()
            receive_times.clear()
        if inpt == 'e':
            for i in statistics_dic:
                print(i, ", ", statistics_dic[i], " işlem için bekleme süresi: ", finish_times[i]-float(start_times[i]), sep='')
            print()
            for i in statistics_dic:
                print(i, "ortalaması:", (finish_times[i]-float(start_times[i]))/statistics_dic[i])
            start_times.clear()
            finish_times.clear()
            statistics_dic.clear()
        if inpt == 'r':
            print(process_times)
            print()
            global toplam
            for i in process_times:          
                toplam = toplam + i
            print("İşlem başına programın çalışma ortalaması:", toplam/len(process_times))
            toplam=0
        if inpt == 'c':
            print(consumers)

t1 = threading.Thread(target=basic_consume)
t1.start()
t2 = threading.Thread(target=start_consume)
t2.start()
t3 = threading.Thread(target=excel_statics)
t3.start()

# 1. çözüm ortalama işlenme süresi  - 2. çözüm ortalama işlenme süresi - 1. çözüm ilk mesaj işlenme süresi - 2. çözüm ilk mesaj işlenme süresi - 1. çözüm için en yüksek ilk bekleme süresi ve kullanıcısı  - 2. çözüm için en yüksek bekleme süresi ve kullanıcısı 
#senaryo1
#senaryo2

#1000 kullanıcılı senaryolar üret (senaryoları kod ile üret)
#senaryo sayısını arttır
#sonuçları excel dosyası olarak yazdır.


#Program durduğunda tekrar çalıştırılabilir yap