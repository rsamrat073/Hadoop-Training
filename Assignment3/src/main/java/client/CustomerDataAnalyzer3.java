package client;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.OptimizeMetadataOnlyQuery;

public class CustomerDataAnalyzer3 {
	private SparkSession spark;

	public CustomerDataAnalyzer3() {
		System.setProperty("hadoop.home.dir", "D:\\GitHUB\\BigData\\Hadoop-Training\\Spark2\\");
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		spark = SparkSession.builder().master("local[*]").appName("Customer Data")
				.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();
	}

	public void createDBRetail() {

		spark.sql("drop table customer_avro");
		Dataset<Row> dstable = null;

		dstable = spark.read().option("header", true).csv("D:/GitHUB/BigData/Hadoop-Training/Assignment3/src/main/resources/customers.csv");
		dstable.show();
		dstable.withColumnRenamed("customer_street ", "customer_street").write().format("com.databricks.spark.avro")
				.saveAsTable("customer_avro");
		dstable.show();
		spark.sql("select * from customer_avro").write().option("spark.sql.parquet.compression.codec", "snappy")
		.format("parquet").saveAsTable("customer_parquet");;
		
	}

	public static void main(String[] args) {

		new CustomerDataAnalyzer3().createDBRetail();
	}

}
