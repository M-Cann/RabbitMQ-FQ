import os
import random
import string

os.chdir("..")

for i in range (1,26):
    path = "scenarios/scenarios%s.txt" %i
    f = open(path, "w")
    for j in range (1,random.randint(5,50)):
        letter = random.choice(string.ascii_uppercase[0:9])
        number = random.randint(10,500)
        let_num = "%s-%s\n" %(letter, number)
        f.write(let_num)