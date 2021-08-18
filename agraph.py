"""
        York U EECS4415 
        jiajun Zhou 215231517
        Xianzun Meng 215183858

"""

import matplotlib; matplotlib.use('Agg')
import numpy as np
import matplotlib.pyplot as plt
import time
i=1
while True:
    print("This is graph"+ str(i))
    f = open("aresult.txt", 'r')
    lines  = f.readlines()
    tags = []
    counts =[]
    for line in lines:
        words = line.split("\t")
        tags.append(words[0])

        counts.append(int(words[1].rstrip()))
    tags.reverse()
    counts.reverse()

    y_pos = np.arange(len(tags))
    plt.figure()
    plt.bar(y_pos, counts, align = 'center',alpha = 0.5)
    plt.xticks(y_pos, tags)

    plt.ylabel("occurance of #")
    plt.xlabel("name of #")
    plt.title("Graph")
    plt.bar(tags, counts)

    plt.savefig("aplot.png")
    #same delay time as in the spark
    time.sleep(5)

    i+=1







#
