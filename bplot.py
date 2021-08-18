"""
        York U EECS4415
        jiajun Zhou 215231517
        Xianzun Meng 215183858

"""

import time

# function of identify the topic
def iswhat(line):
    if "redbull" in line:
        return "RedBull"
    elif "ferrari" in line:
        return "Ferrari"
    elif "astonmartin" in line:
        return "AstonMartin"
    elif "mercedes" in line:
        return "Mercedes"
    else :
        return "Alpine"

i=1
while True:
    print("**********************")
    print("This is plot "+ str(i))
    print("**********************\n")


    #store all 5 data as [pos, neutral, neg]
    # data = {"RedBull":[0,0,0],
    # "Ferrari":[0,0,0],
    # "AstonMartin":[0,0,0],
    # "Mercedes":[0,0,0],
    # "Alpine" : [0,0,0]
    # }

    #open file
    f = open("bresult.txt", 'r')
    lines  = f.readlines()
    index =1
    for line in lines:
        words = line.split("\t")
        position = (index%3) - 1
        #update the data
        data[iswhat(line)][position] = int(words[1])
        index +=1

    for key, value in data.items():
        # analize computation
        total_tweets = value[0] + value[1] + value[2]
        sum = value[0] - value[2]
        average = 0
        if total_tweets > 0:
            average = (sum/total_tweets)
        else:
            average = 0
        #print our results
        print(key)
        print("%dx positive(+1)" %(value[0]))
        print("%dx neutral(0)" %(value[1]))
        print("%dx negative(-1)\n" %(value[2]))
        print("Total tweets: %d" %(total_tweets))
        print("Sum = %d" %(sum))
        print("Average_Sentiment = %f\n" %(average))

    time.sleep(30)
    i+=1
