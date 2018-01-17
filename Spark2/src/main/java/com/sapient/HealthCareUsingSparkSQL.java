package com.sapient;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.sapient.JavaSQLDataSourceExample.Cube;
import com.sapient.JavaSQLDataSourceExample.Square;

public class HealthCareUsingSparkSQL {

	public static void main(String[] args) {
		//winutils.exe chmod 777 D:\tmp\hive 
		System.setProperty("hadoop.home.dir", "C:\\Users\\TEMP\\Desktop\\SparkWS\\");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL data sources example")
				.config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath()).enableHiveSupport()
				.getOrCreate();

		runBasicDataSourceExample(spark);

		spark.stop();
	}

	private static void runBasicDataSourceExample(SparkSession spark) {

		Dataset<Row> usersDF = spark.read().csv("D:\\Hadoop Materials\\CTS\\HiveCodes\\HIVE\\*.csv");
		 System.out.println(usersDF.count());
		usersDF = usersDF.toDF("id", "un", "dob", "cont1", "mailId", "cont2", "gender", "disease", "weight");

		usersDF.write().partitionBy("gender","disease").bucketBy(5, "mailId").format("orc")
				.saveAsTable("HEALTH_CARE");
		 //spark.sql("REFRESH TABLE HEALTH_CARE");
		//usersDF = spark.sql("select * from HEALTH_CARE limit 30 ");
		usersDF.show();
	}
}