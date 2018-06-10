
Twitter Streaming::
	-first Start kafka
	 use the commands::
		to start zookeeper
			- bin/zookeeper-server-start.sh config/zookeeper.properties
	 	to start kafka:
			- bin/kafka-server-start.sh config/server.properties
		
	
	- now start elastic search and kibana	
	- Create a Mapping for sentiment index for elastic search:

	curl -XPUT http://localhost:9200/twitterstream -d '
	{
	 "mappings" : {
	  "test-type" : {
	   "properties" : {
	    "text" : {"type": "string", "index" : "not_analyzed" },
	    "sentiment" : {"type": "string", "index" : "not_analyzed" },
		"location" : {"type": "geo_point"},
		"timestamp" : {"type": "date"},
		"positive" : {"type" : "long"},
		"neutral" : {"type" : "long"},
		"negative" : {"type" : "long"}
	   }
	  }
	 }
	}
	';

	Execute on terminal:
	 -python producer.py
	 -spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 consumer.py localhost:2181 twitterstream
	
	- Open in browser:
		to open kibana
	 	- https://localhost:5601
		- Create an index pattern in kibana
		- In visualize, select SUM for fields positive, negative and neutral.
		- Save visualization
		- In dashboard, create a dashboard and set auto-refresh time for 1 minute.
	