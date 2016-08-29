package com.bigdata.spark.network;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


public class NetworkStreaming {
	public static void main(String[] args) {
		
		SparkConf config = new SparkConf();
		config.setMaster("local[2]").setAppName("NetworkStreaming");
		
		JavaSparkContext sparkContext = new JavaSparkContext(config);
		
		JavaStreamingContext streamContext = new JavaStreamingContext(sparkContext, new Duration(10000l));
		
		JavaReceiverInputDStream<String> inputDstream = streamContext.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER_2());
		
		JavaPairDStream<String, Integer> pairs = inputDstream.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairDStream<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		counts.print();
		
		streamContext.start();
		streamContext.awaitTermination();
		
	}
}
