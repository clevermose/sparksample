package com.bigdata.spark.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

public class SparkKafka {
	
	private static final String topic = "KAFKA_1";
	private static final String groupId = "kafka-spark-1";
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName("SparkKafka").setMaster("local[8]");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
		JavaStreamingContext streamContext = new JavaStreamingContext(sparkContext, new Duration(10000l));
		
		/******************创建kafka的cluster****************/
		Map<String, String> oKafkaParams = new HashMap<String, String>();
		oKafkaParams.put("metadata.broker.list", "10.249.73.142:9092,10.249.73.143:9092,10.249.73.144:9092");
		//oKafkaParams.put("metadata.broker.list", "topgun-spark1-8367.lvs01.dev.ebayc3.com:9092,topgun-spark2-8247.lvs01.dev.ebayc3.com:9092,topgun-spark3-8268.lvs01.dev.ebayc3.com:9092");
		oKafkaParams.put("group.id", groupId);
		
		//Java Map转scala的mutable map
		scala.collection.mutable.Map<String, String> mutableKafkaParams = JavaConversions.mapAsScalaMap(oKafkaParams);
		//scala的mutable map 转 scala的immutable map
		scala.collection.immutable.Map<String, String> immutableKafkaParams = mutableKafkaParams.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, String> apply(Tuple2<String, String> v) {
				return v;
			}
		});
		
		final KafkaCluster kafkaCluster = new KafkaCluster(immutableKafkaParams);
		
		/*******************获取offset信息*******************/
		
		Map<TopicAndPartition, Long> topicAndPartitionOffsetMap = new HashMap<TopicAndPartition, Long>();
		
		//topic信息,转换成scala类型
		Set<String> javaTopicSet = new HashSet<String>();
		javaTopicSet.add(topic);
		scala.collection.mutable.Set<String> scalaTopicMutableSet = JavaConversions.asScalaSet(javaTopicSet);
		scala.collection.immutable.Set<String> scalaTopicImmutableSet = scalaTopicMutableSet.toSet();
		
		//通过Topic信息获取到Partition的信息
		scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet = kafkaCluster.getPartitions(scalaTopicImmutableSet).right().get();
		
		//如果isLeft()为true表示获取异常了,认为是第一次消费
		boolean isFirstTime = kafkaCluster.getConsumerOffsets(groupId, scalaTopicAndPartitionSet).isLeft();
		if(isFirstTime) {
			//将scala的Partition信息转成Java的set
			Set<TopicAndPartition> topicAndPartitionSet = JavaConversions.asJavaSet(scalaTopicAndPartitionSet);
			//获取每个Partition的leader的offset信息
			scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> earliestOffset = kafkaCluster.getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
			//Partition和对应的leader的offset的映射信息
			Map<TopicAndPartition, LeaderOffset> earliestOffsetMap = JavaConversions.asJavaMap(earliestOffset);
			
			for (TopicAndPartition topicAndPartition : topicAndPartitionSet) {
				Long leaderOffset = earliestOffsetMap.get(topicAndPartition).offset();
				Long offset = 0L;
				if(offset < leaderOffset) {
					offset = leaderOffset;
				}
				topicAndPartitionOffsetMap.put(topicAndPartition, offset);
			}
		} else { //不是第一次
			//获取Consumer的Partition与Leader offset的映射信息
			scala.collection.immutable.Map<TopicAndPartition, Object> scalaTopicAndPartitionOffsetMap = kafkaCluster.getConsumerOffsets(groupId, scalaTopicAndPartitionSet).right().get();
			Map<TopicAndPartition, Object> topicAndPartitionOffsetMapTmp = JavaConversions.mapAsJavaMap(scalaTopicAndPartitionOffsetMap);
			//获取Partition的Earliest leader offset信息
			scala.collection.immutable.Map<TopicAndPartition, LeaderOffset> scalaEarliestOffset = kafkaCluster.getEarliestLeaderOffsets(scalaTopicAndPartitionSet).right().get();
			Map<TopicAndPartition, LeaderOffset> earliestOffsetMap = JavaConversions.asJavaMap(scalaEarliestOffset);
			Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions.setAsJavaSet(scalaTopicAndPartitionSet);
			for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
				//Consumer的partition的leader offset信息
				Long offset = (Long)topicAndPartitionOffsetMapTmp.get(topicAndPartition);
				//partition的earliest leader offset信息
				Long leaderOffset = earliestOffsetMap.get(topicAndPartition).offset();
				if(offset < leaderOffset) {
					offset = leaderOffset;
				}
				topicAndPartitionOffsetMap.put(topicAndPartition, offset);
			}
		}
		
		/*******************创建于kafka之间的流通道******************/
		Class keyClazz = String.class;
		Class valClazz = String.class;
		Class keyDecoderClazz = StringDecoder.class;
		Class valDecoderClazz = StringDecoder.class;
		Class recordClazz = String.class;
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "10.249.73.142:9092,10.249.73.143:9092,10.249.73.144:9092");
		//kafkaParams.put("metadata.broker.list", "topgun-spark1-8367.lvs01.dev.ebayc3.com:2181,topgun-spark2-8247.lvs01.dev.ebayc3.com:2181,topgun-spark3-8268.lvs01.dev.ebayc3.com:2181/kafka");
		/*
		Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
		TopicAndPartition topicAndPartition_0 = new TopicAndPartition(topic, 0);
		TopicAndPartition topicAndPartition_1 = new TopicAndPartition(topic, 1);
		fromOffsets.put(topicAndPartition_0, 0l);
		fromOffsets.put(topicAndPartition_1, 0l);
		*/
		
		
		JavaInputDStream<String> inputDStream = KafkaUtils.createDirectStream(streamContext, keyClazz, valClazz, keyDecoderClazz, valDecoderClazz, recordClazz, kafkaParams, topicAndPartitionOffsetMap, new Function<MessageAndMetadata<String,String>, String>() {
			private static final long serialVersionUID = 1L;
			public String call(MessageAndMetadata<String,String> v1) throws Exception {
				return v1.message();
			}
		});
		
		inputDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			public void call(JavaRDD<String> rdd) throws Exception {
				rdd.foreach(new VoidFunction<String>() {
					private static final long serialVersionUID = 1L;
					public void call(String message) throws Exception {
						System.out.println("message : " + message);
					}
				});
				
				OffsetRange[] offsets = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
				for (OffsetRange offsetRange : offsets) {
					//topic
					String topic = offsetRange.topic();
					//partition的编号
					int partitionId = offsetRange.partition();
					//读取的数据的offset起始位置
					long fromOffset = offsetRange.fromOffset();
					//读取的数据的offset结束位置
					long untilOffset = offsetRange.untilOffset();
					
					System.out.println("topic : " + topic + " ,partitionId : " + partitionId + " ,fromOffset : " + fromOffset + " ,untilOffset : " + untilOffset);
					
					TopicAndPartition topicAndPartition = offsetRange.topicAndPartition();
					Map<TopicAndPartition, Object> topicAndPartitionOffsetMap = new HashMap<TopicAndPartition, Object>();
					topicAndPartitionOffsetMap.put(topicAndPartition, untilOffset);
					
					//Java Map转成 Scala的mutable Map
					scala.collection.mutable.Map<TopicAndPartition, Object> topicAndPartitionOffsetScalaMutableMap = JavaConversions.mapAsScalaMap(topicAndPartitionOffsetMap);
					//scala的mutable map 转 scala的immutable map
					scala.collection.immutable.Map<TopicAndPartition, Object> topicAndPartitionOffsetScalaImmutableMap = 
							topicAndPartitionOffsetScalaMutableMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
								private static final long serialVersionUID = 1L;
								public scala.Tuple2<TopicAndPartition,Object> apply(scala.Tuple2<TopicAndPartition,Object> v) {
									return v;
								}
							});
					
					//提交offset
					kafkaCluster.setConsumerOffsets(groupId, topicAndPartitionOffsetScalaImmutableMap);
				}
			}
		});
		
		streamContext.start();
		streamContext.awaitTermination();
	}
}
