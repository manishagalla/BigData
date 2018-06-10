import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def es_index():
    for x in rdd.collect():
        print x
    

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: rss_process.py <host> <topic>")
        exit(-1)
    sc = SparkContext()
    ssc = StreamingContext(sc, 1)
    host, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, host, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1].encode("ascii","ignore"))
    lines.foreachRDD(es_index)
    ssc.start()
    ssc.awaitTermination()