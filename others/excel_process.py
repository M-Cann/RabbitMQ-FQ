import os 
from openpyxl import Workbook,load_workbook
from openpyxl.utils import get_column_letter

#os.chdir("..")
wb = Workbook("statistics.xlsx")
wb.save("statistics.xlsx")
#wb = load_workbook("statistics.xlsx")
#ws = wb.active
queues_names=['A','B','C','D','E','F','G','H','I','M']

def scenarios_count():
    path = "scenarios"
    count = os.listdir(path)
    return(len(count))

def excel_clear():
    wb = load_workbook("statistics.xlsx")
    ws = wb.active
    for row in ws:
        for cell in row:
            cell.value = None
    sc_count = scenarios_count()
    j=0
    for i in range(2,2+(sc_count*6), 6):
        ws.merge_cells(start_column=i, end_column=i+5, start_row=1, end_row=1)
        cell=str(get_column_letter(i))+str(1)
        ws[cell] = 'Seneryo ' + str(j)
        j=j+1
    for j in range(2,2+(sc_count*6), 6):
        ws.merge_cells(start_column=j, end_column=j+1, start_row=2, end_row=2)
        ws.merge_cells(start_column=j+2, end_column=j+3, start_row=2, end_row=2)
        ws.merge_cells(start_column=j+4, end_column=j+5, start_row=2, end_row=2)
        cell1=str(get_column_letter(j))+str(2)
        cell2=str(get_column_letter(j+2))+str(2)
        cell3=str(get_column_letter(j+4))+str(2)
        ws[cell1] = 'Çözümsüz'
        ws[cell2] = 'Block Connect'
        ws[cell3] = 'Select Connect'
    for k in range(2,2+(sc_count*6), 2):
        cell1=str(get_column_letter(k))+str(3)
        cell2=str(get_column_letter(k+1))+str(3)
        ws[cell1] = 'İlk mesajın süresi'
        ws[cell2] = 'Ortalama mesaj işleme süresi'
    for l in range(0,10):
        cell='A'+str(l+4)
        ws[cell]=queues_names[l]
    wb.save("statistics.xlsx")

def excel_read():
    wb = load_workbook("statistics.xlsx")
    ws = wb.active
    for row in ws:
        for cell in row:
            print(cell.value, " ", end='')
        print()

while True:
    inpt = input()
    if inpt == 'c':
        excel_clear()
    if inpt == 'r':
        excel_read()