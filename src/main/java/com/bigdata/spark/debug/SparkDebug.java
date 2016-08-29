package com.bigdata.spark.debug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SparkDebug {

	public static void main(String[] args) {
		
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName("SparkDebug");
		
		List<String> contents = new ArrayList<String>();
		contents.add("i am wang hai sheng");
		contents.add("i am 28 years old");
		contents.add("thanks");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		//parallelize产生ParrallelCollectionRDD,每个Partition包含一行文字
		//JavaRDD<String> javaRdd = javaSparkContext.parallelize(contents);
		
		JavaRDD<String> javaRdd = javaSparkContext.textFile("hdfs://10.249.73.142:9000/data/bigdata/test-001.nb");
		
		/*
		javaRdd.foreachPartition(new VoidFunction<Iterator<String>>() {
			private static final long serialVersionUID = -4724362517197890348L;
			public void call(Iterator<String> iter) throws Exception {
				long threadId = Thread.currentThread().getId();
				while(iter.hasNext()) {
					System.out.println(threadId + " : " + iter.next());
				}
			}
		});
		*/
		
		long firstRddCount = javaRdd.count();
		//Driver输出
		System.out.println("firstRddCount : " + firstRddCount); //3
		
		final Integer oneCount = 1;
		JavaRDD<String> words = javaRdd.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(String content) throws Exception {
				//return Arrays.asList(content.split(" "));
				return Arrays.asList(content.split("	"));
			}
		});
		
		long secondRddCount = words.count();
		//Driver输出
		System.out.println("secondRddCount : " + secondRddCount); //11
		
		JavaPairRDD<String, Integer> wordWithOneCount = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, oneCount);
			}
		});
		
		long thirdRddCount = wordWithOneCount.count();
		//Driver输出
		System.out.println("thirdRddCount : " + thirdRddCount); //11
		
		JavaPairRDD<String, Integer> wordCount = wordWithOneCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		long fourthRddCount = wordCount.count();
		//Driver输出
		System.out.println("fourthRddCount : " + fourthRddCount); //9
		
		//Worker输出
		wordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("foreach ,word : " + tuple._1 + " ,count : " + tuple._2);
			}
		});
		
		//Worker输出
		wordCount.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
			private static final long serialVersionUID = 1L;
			public void call(Iterator<Tuple2<String, Integer>> iter)
					throws Exception {
				while(iter.hasNext()) {
					Tuple2<String, Integer> tuple = iter.next();
					System.out.println("foreachPartition ,word : " + tuple._1 + " ,count : " + tuple._2);
				}
			}
		});
		
		//Driver端输出
		Map<String, Integer> javaMapWordCount = wordCount.collectAsMap();
		for (Entry<String, Integer> entry : javaMapWordCount.entrySet()) {
			System.out.println("collectAsMap ,word : " + entry.getKey() + " ,count : " + entry.getValue());
		}
		
		String rddDebugString = wordCount.toDebugString();
		System.out.println(rddDebugString);
		/*
		(2) ShuffledRDD[3] at reduceByKey at SparkDebug.java:50 []
 		+-(2) MapPartitionsRDD[2] at mapToPair at SparkDebug.java:43 []
    		|  MapPartitionsRDD[1] at flatMap at SparkDebug.java:36 []
    		|  ParallelCollectionRDD[0] at parallelize at SparkDebug.java:32 []
		*/
		javaSparkContext.close();
	}

}
