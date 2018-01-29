package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContext {

	// A name for the spark instance. Can be any string
	private static String appName = "Sapient Assignment";
	// Pointer / URL to the Spark instance - embedded
	private static String sparkMaster = "local[*]";

	private static JavaSparkContext spContext = null;


	private static void getConnection(String appName) {

		if (spContext == null) {
			// Setup Spark configuration
			System.setProperty("hadoop.home.dir", "C:\\Users\\samra\\Desktop\\Sapient\\Hadoop Materials\\");
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster).setSparkHome("C:\\Users\\samra\\");
			spContext = new JavaSparkContext(conf);

		}

	}

	public static JavaSparkContext getContext() {

		if (spContext == null) {
			getConnection(appName);
		}
		return spContext;
	}

	public static JavaSparkContext getContext(String appName) {

		if (spContext == null) {
			getConnection(appName);
		}
		return spContext;
	}

	public static void closeContext() {
		if (spContext != null) {
			spContext.close();
		}
	}

}
