package com.bigdata.spark.dataframe;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateDataFrameWithSchema {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparkSqlOracle").setMaster("local[8]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		
		StructType schema = new StructType(new StructField[] {
				new StructField("s_name", DataTypes.StringType, false, Metadata.empty()),
				new StructField("s_sex", DataTypes.StringType, false, Metadata.empty()),
				new StructField("s_age", DataTypes.StringType, false, Metadata.empty())
		});
		
		DataFrame userDataFrame = sqlContext.read().json("json");
		userDataFrame.registerTempTable("user");
		
		List<Row> rows = userDataFrame.collectAsList();
		
		//使用rows和schema来创建DataFrame
		DataFrame newDF = sqlContext.createDataFrame(rows, schema);
		newDF.registerTempTable("new_user");
		
		DataFrame result = sqlContext.sql("select s_name,s_sex,s_age from new_user");
		rows = result.collectAsList();
		for (Row row : rows) {
			String name = row.getString(0);
			String sex = row.getString(1);
			String age = row.getString(2);
			System.out.println(name + " : " + sex + " : " + age);
		}
		
		javaSparkContext.stop();
	}

}
