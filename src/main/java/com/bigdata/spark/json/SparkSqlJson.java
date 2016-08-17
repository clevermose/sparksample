package com.bigdata.spark.json;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SparkSqlJson {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparkSqlOracle").setMaster("local[8]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		//DataFrame userDataFrame = sqlContext.read().json("json/user-1.json"); //这边直接传一个文件ok
		DataFrame userDataFrame = sqlContext.read().json("json");
		userDataFrame.registerTempTable("user");
		
		DataFrame result = sqlContext.sql("select * from user");
		List<Row> rows = result.collectAsList();
		for (Row row : rows) {
			String name = row.getString(0);
			String sex = row.getString(1);
			String age = row.getString(2);
			System.out.println(name + " : " + sex + " : " + age);
		}
		
		javaSparkContext.stop();
	}

}
