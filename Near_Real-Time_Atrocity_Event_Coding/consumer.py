from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
import os
from subprocess import Popen, PIPE
import uuid

base_dir = "/Users/manishagalla/Documents/UTD/Spring17/BigDataManagement/Project"
input_files_path=os.path.join(base_dir,'scraper-master')
#input_files_list=os.listdir(input_files_path)
def es_index(rdd):
    for x in rdd.collect():
        sentences=x.split(". ")
        print(sentences)
        filename=str(uuid.uuid4())
        #test_input_path=os.path.join(base_dir,"test_input/"+filename)
        with open(filename,'a') as f:
            for sentence in sentences:
                f.write(sentence)
                f.write('\n')
            try:
                input_path = os.path.join(base_dir,filename)
                print(input_path)
                output_path = os.path.join(base_dir,"output_test_files")
                print(output_path)
                cmd = 'semafor/bin/runSemafor.sh '+input_path + ' ' + output_path + '/' + filename + ' 10'
                print(cmd)
                p = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
                out, err = p.communicate()
                print("Return code: ", p.returncode)
            except:
                print("cannot parse file: ")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: consume.py <host> <topic>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    ssc = StreamingContext(sc, 1)
    host, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, host, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1].encode("ascii","ignore"))
    lines.foreachRDD(es_index)
    ssc.start()
    ssc.awaitTermination()
