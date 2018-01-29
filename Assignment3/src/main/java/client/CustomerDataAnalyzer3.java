package client;

import java.io.File;

import org.apache.spark.sql.SparkSession;

public class CustomerDataAnalyzer3 {
	private SparkSession spark;
	
	public CustomerDataAnalyzer3() {
		System.setProperty("hadoop.home.dir", "D:\\GitHUB\\BigData\\Hadoop-Training\\Spark2\\");
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
	     spark = SparkSession
	      .builder()
	      .master("local[*]")
	      .appName("Customer Data")
	      .config("spark.sql.warehouse.dir", warehouseLocation)
	      .enableHiveSupport()
	      .getOrCreate();
	}
	
	public void createDBRetail(){
		
		spark.read().load("");
		
	}
	
	public static void main(String[] args) {
		
		new CustomerDataAnalyzer3().createDBRetail();
	}

}
