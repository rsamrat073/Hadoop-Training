package com.sapient.www;

import java.io.File;
import java.sql.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

import scala.Tuple2;
import scala.collection.IndexedSeq;

public class SparkJoiner {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Hadoop Materials\\");
		SparkSession spark = SparkSession.builder().master("local[*]")
				.config("spark.cassandra.connection.host", "127.0.0.1")
				.config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath()).enableHiveSupport()

				.appName("Java Spark NSYS Stock Dividend Assignment").getOrCreate();

		JavaRDD<Stock> closePriceBySymbol = spark.read()
				.textFile(
						"C:\\Users\\TEMP.SAPIENT\\Desktop\\Sapient_classnotes (1)\\project_data\\project_data\\NYSE_daily")
				.javaRDD().map(line -> {
					String[] parts = line.split("\\t");
					Stock stock = new Stock();
					stock.setExchange(parts[0]);
					stock.setTickersymbol(parts[1]);
					stock.setDate(parts[2]);
					stock.setLow(Float.parseFloat(parts[3]));
					stock.setMid(Float.parseFloat(parts[4]));
					stock.setHigh(Float.parseFloat(parts[5]));
					stock.setClose(Float.parseFloat(parts[6]));
					stock.setIsin(Integer.parseInt(parts[7]));
					return stock;
				});

		JavaPairRDD<String, Float> maxPriceSymbolRDD = closePriceBySymbol
				.mapToPair(new PairFunction<Stock, String, Float>() {

					@Override
					public Tuple2<String, Float> call(Stock s) throws Exception {
						Tuple2<String, Float> t = new Tuple2<String, Float>(s.getTickersymbol(), s.getClose());
						return t;
					}

				}).reduceByKey(new Function2<Float, Float, Float>() {

					@Override
					public Float call(Float c1, Float c2) throws Exception {

						return c2 > c1 ? c2 : c1;
					}
				});
		CassandraJavaUtil.javaFunctions(maxPriceSymbolRDD)
				.writerBuilder("stock_keyspace", "nyse_close_price", CassandraJavaUtil.mapTupleToRow(String.class, Float.class))
				.withConsistencyLevel(ConsistencyLevel.QUORUM).saveToCassandra();

		JavaRDD<Dividend> dividends = spark.read()
				.textFile(
						"C:\\Users\\TEMP.SAPIENT\\Desktop\\Sapient_classnotes (1)\\project_data\\project_data\\NYSE_dividends")
				.javaRDD().map(line -> {
					String[] parts = line.split("\\t");
					Dividend div = new Dividend();
					div.setExchange(parts[0]);
					div.setTickersymbol(parts[1]);
					div.setDate(parts[2]);
					div.setDividend(Float.parseFloat(parts[3]));
					return div;
				});

		JavaPairRDD<String, Float> maxDividendRDD = dividends
				.mapToPair(new PairFunction<Dividend, String, Float>() {

					@Override
					public Tuple2<String, Float> call(Dividend s) throws Exception {
						Tuple2<String, Float> t = new Tuple2<String, Float>(s.getTickersymbol(), s.getDividend());
						return t;
					}

				}).reduceByKey(new Function2<Float, Float, Float>() {

					@Override
					public Float call(Float c1, Float c2) throws Exception {

						return c2 > c1 ? c2 : c1;
					}
				});
		System.out.println();
		CassandraJavaUtil.javaFunctions(maxDividendRDD)
				.writerBuilder("stock_keyspace", "nyse_dividend", CassandraJavaUtil.mapTupleToRow(String.class, Float.class))
				.withConsistencyLevel(ConsistencyLevel.QUORUM).saveToCassandra();
		//System.out.println(maxPriceSymbolRDD.collect());

	}

}
