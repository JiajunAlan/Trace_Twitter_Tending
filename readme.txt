This is readme.txt for    jiajun Zhou 215231517  &  Xianzun Meng 215183858

For Part A:
Our file for part A are atwitter.py, aspark.py, agraph.py, aresult.txt and aplot.png
put them under your project folder.

1: lunch two docker. first one:
docker run -it -v $PWD:/app --name twitter -w /app python bash

second one :
docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

in twitter image, on bash run:    python3 atwitter.py
in eecs4415 image, on bash run:   spark-submit aspark.py
in your operating system's terminal at your project folder, run:   python3 agraph.py

atwitter will get all suitable Twitter and send them to spark through the port.
aspark will deal with the input stream and get results to file aresult.txt.
agraph will read aresult.txt and give dynamic result plots saved as aplot.png


For Part B:
Our file for part B are btwitter.py, bspark.py, bplot.py, bresult.txt
put them under your project folder.
1: lunch two docker. first one:
docker run -it -v $PWD:/app --name twitter -w /app python bash

second one :
docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

in twitter image, on bash run:    python3 btwitter.py
in eecs4415 image, on bash run:   spark-submit bspark.py
in your operating system's terminal at your project folder, run:   python3 bplot.py you will see the plot on the terminal.



#end of readme
