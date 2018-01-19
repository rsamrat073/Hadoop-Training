package com.sapient;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StockPriceAnalyzer {

	public static void main(String args[]) throws Exception {

		SparkSession spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath()).enableHiveSupport()
				.appName("Java Spark NSYS Stock Dividend Assignment").getOrCreate();

		// will proceeds after actual work done
		
		/*JavaRDD<Stock> stockRDD = spark.read().textFile("hdfs://10.150.222.60:8020/projectdata/stock/").javaRDD()
				.map(line -> {
					String[] sparts = line.split("\t");
					Stock stock = new Stock();
					stock.setName(sparts[1]);
					stock.setStock(Double.parseDouble(sparts[6]));
					return stock;
				});
		stockRDD.collect().forEach(System.out::println);*/
		
		 spark.sql("SET hive.exec.dynamic.partition=true");
         spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict");
		
			/*spark.sql("drop table nyse");
			spark.sql("drop table nyse_partition");
		   Dataset<Row> sresult = spark.sql("CREATE  TABLE IF NOT  EXISTS nyse(stockexchange string,stocksymbol string,date1 date,low float,high float,mid float,closeprice float,nofstocks int,finalcloseprice float) row format delimited fields terminated by '\\t' stored as textfile");
		   
		  
           
		// spark.sql("LOAD DATA INPATH 'file:///hdfs://10.150.222.60:8020/projectdata/stock/' INTO TABLE nyse");
		   spark.sql("LOAD DATA LOCAL INPATH 'D:/HADOOPWORKSPACE/project_data/NYSE_daily' INTO TABLE nyse");

		   
		   
           spark.sql("CREATE TABLE IF NOT EXISTS nyse_partition (closeprice float) partitioned by(stocksymbol String) row format delimited fields terminated by '\\t' stored as textfile");
           sresult=spark.sql("insert into nyse_partition partition(stocksymbol) select closeprice,stocksymbol from nyse");

		   
		   sresult.printSchema();
   
           spark.sql("select * from nyse_partition").show(10);
          
           
           /////////////Dividend//////////////////
           
           Dataset<Row>dresult= spark.sql("CREATE TABLE IF NOT EXISTS dividend(stockexchange string, stocksymbol string, date1 date, dividend float) row format delimited fields terminated by '\\t' stored as textfile ");
                                spark.sql("LOAD DATA LOCAL INPATH 'D:/HADOOPWORKSPACE/project_data/NYSE_dividends' INTO TABLE dividend");
                                
                                /// creating dividend partition
		                         spark.sql(" CREATE TABLE IF NOT EXISTS dividend_partition(dividend float)partitioned by(stocksymbol string) row format delimited fields terminated by '\\t' stored as textfile ");     
	                             spark.sql("insert into dividend_partition partition(stocksymbol)select dividend, stocksymbol from dividend "); 
	                             
	                           spark.sql("select * from dividend_partition").show(10);  
	                           */
	                           
	                           /// creating max_min table
	        Dataset<Row> Max_minresult= spark.sql("CREATE TABLE IF NOT EXISTS max_min_stock(stocksymbol string, price float, rate float) row format delimited fields terminated by '\\t' stored as textfile"); 
	                                    
	                                    spark.sql("insert into max_min_stock(select dnp.stocksymbol, max(nsp.closeprice), max(dnp.dividend) from dividend_partition dnp, nyse_partition nsp where dnp.stocksymbol=nsp.stocksymbol group by dnp.stocksymbol )");     
	                             
                               	         spark.sql("select * from max_min_stock ").show();                    
	
	
	}

}
