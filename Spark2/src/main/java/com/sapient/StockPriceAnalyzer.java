package com.sapient;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StockPriceAnalyzer {

	public static void main(String args[]) throws Exception {

		// System.out.println(new File("NYSE_daily").getAbsolutePath());

		 System.setProperty("hadoop.home.dir", "D:\\Hadoop Materials\\");
		SparkSession spark = SparkSession.builder().master("local[*]")
				.config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath()).enableHiveSupport()
				.appName("Java Spark NSYS Stock Dividend Assignment").getOrCreate();
		// TODO 1) JAVARDD -->

		// TODO 1) JAVARDD -->

		spark.sql("SET hive.exec.dynamic.partition=true");
		spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict");

		Dataset<Row> sresult = spark.sql(
				"CREATE  TABLE  NYSE_CLOSE_PRICE(stockexchange string,stocksymbol string,date1 date,low float,high float,mid float,closeprice float,nofstocks int,finalcloseprice float) row format delimited fields terminated by '\\t' stored as textfile");

		// spark.sql("LOAD DATA INPATH
		// 'file:///hdfs://10.150.222.60:8020/projectdata/stock/' INTO TABLE
		// nyse");
		spark.sql("LOAD DATA LOCAL INPATH 'D:/Hadoop Materials/Hadoop-Training/project_data/NYSE_daily' INTO TABLE NYSE_CLOSE_PRICE");

//		spark.sql("CREATE TABLE IF NOT EXISTS NYSE_CLOSE_PRICE_PARTITION (closeprice"
//				+ "float) partitioned by(stocksymbol String) row format delimited fields"
//				+ "terminated by '\\t' stored as textfile");
//		sresult = spark.sql("insert into NYSE_CLOSE_PRICE_PARTITION partition(stocksymbol)"
//				+ "select distinct closeprice,stocksymbol from NYSE_CLOSE_PRICE");
//		sresult.printSchema();

		spark.sql("select * from NYSE_CLOSE_PRICE").show(10);

		///////////// Dividend//////////////////

		// Dataset<Row> dresult = spark.sql(
		// "CREATE TABLE IF NOT EXISTS dividend(stockexchange string,
		// stocksymbol string, date1 date, dividend float) row format delimited
		// fields terminated by '\\t' stored as textfile ");
		// spark.sql("LOAD DATA LOCAL INPATH
		// 'D:/HADOOPWORKSPACE/project_data/NYSE_dividends' INTO TABLE
		// dividend");
		//
		// /// creating dividend partition
		// spark.sql(
		// " CREATE TABLE IF NOT EXISTS dividend_partition(dividend
		// float)partitioned by(stocksymbol string) row format delimited fields
		// terminated by '\\t' stored as textfile ");
		// spark.sql("insert into dividend_partition
		// partition(stocksymbol)select dividend, stocksymbol from dividend ");
		//
		// spark.sql("select * from dividend_partition").show(10);

		/// creating max_min table
		// Dataset<Row> Max_minresult= spark.sql("CREATE TABLE IF NOT EXISTS
		// max_min_stock(stocksymbol string, price float, rate float) row format
		// delimited fields terminated by '\\t' stored as textfile");
		//
		// spark.sql("insert into max_min_stock(select dnp.stocksymbol,
		// max(nsp.closeprice), max(dnp.dividend) from dividend_partition dnp,
		// nyse_partition nsp where dnp.stocksymbol=nsp.stocksymbol group by
		// dnp.stocksymbol )");
		//
		// spark.sql("select * from max_min_stock ").show();

	}

}
