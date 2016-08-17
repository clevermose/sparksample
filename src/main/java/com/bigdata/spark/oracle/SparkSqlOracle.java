package com.bigdata.spark.oracle;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * spark¡¨Ω”oracle
 * @author haiswang
 *
 */
public class SparkSqlOracle {

	public static void main(String[] args) {
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("driver", "oracle.jdbc.driver.OracleDriver");
		options.put("url", "jdbc:oracle:thin:@10.249.73.141:1521:orcl");
		options.put("user", "scott");
		options.put("password", "tiger");
		options.put("dbtable", "EMP");
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparkSqlOracle").setMaster("local[8]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		sqlContext.read().format("jdbc").options(options).load().toDF().registerTempTable("spark_emp");
		
		DataFrame result = sqlContext.sql("select * from spark_emp");
		List<Row> rows = result.collectAsList();
		for (Row row : rows) {
			BigDecimal empNo = row.getDecimal(0);
			String ename = row.getString(1);
			String job = row.getString(2);
			System.out.println(empNo.intValue() + " : " + ename + " : " + job);
		}
		
		System.out.println("over.");
		javaSparkContext.stop();
	}

}
