package com.github.projectflink.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

public class TPCH3Sql {

	public static void main(String[] args) {
		String master = args[0];
		SparkConf conf = new SparkConf().setAppName("TCPH Q3 Sql").setMaster(master);
		JavaSparkContext sc  = new JavaSparkContext(conf);
		JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(sc);
	}
	
}
