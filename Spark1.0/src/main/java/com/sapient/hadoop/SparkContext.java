package com.sapient.hadoop;



public class SparkContext {

	// A name for the spark instance. Can be any string
	private static String appName = "V2 Maestros";
	// Pointer / URL to the Spark instance - embedded
	private static String sparkMaster = "local[*]";

	private static JavaSparkContext spContext = null;

	private static String tempDir = "file:///d:/temp/spark-warehouse";

	private static void getConnection(String appName) {

		if (spContext == null) {
			// Setup Spark configuration
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);

			// Make sure you download the winutils binaries into this directory
			// from
			// https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
			// System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");

			// Create Spark Context from configuration
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
	}}
