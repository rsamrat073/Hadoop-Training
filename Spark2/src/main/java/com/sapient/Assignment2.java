package com.sapient;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Assignment2 {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "C:\\Users\\TEMP\\Desktop\\SparkWS\\");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL basic example")
				.getOrCreate();

		JavaRDD<Stock> closePriceBySymbol = spark.read().textFile("D:\\Hadoop Materials\\NYSE.csv").javaRDD()
				.map(line -> {
					String[] parts = line.split(",");
					Stock stock = new Stock();
					stock.setName(parts[1]);
					stock.setStock(Double.parseDouble(parts[6]));
					return stock;
				});

		closePriceBySymbol = closePriceBySymbol.sortBy(new Function<Stock, Double>() {

			@Override
			public Double call(Stock s) throws Exception {
				// TODO Auto-generated method stub
				return s.getStock();
			}
		}, false, 6);

		JavaPairRDD<String, Double> maxPriceSymbolRDD = closePriceBySymbol
				.mapToPair(new PairFunction<Stock, String, Double>() {

					@Override
					public Tuple2<String, Double> call(Stock s) throws Exception {
						Tuple2<String, Double> t = new Tuple2<String, Double>(s.getName(), s.getStock());
						return t;
					}

				}).reduceByKey(new Function2<Double, Double, Double>() {

					@Override
					public Double call(Double d1, Double d2) throws Exception {

						return d2 > d1 ? d2 : d1;
					}
				});

		System.out.println(maxPriceSymbolRDD.collect());
		maxPriceSymbolRDD.saveAsTextFile("stock_min_max");
		JavaPairRDD<String, Iterable<Double>> stockGroupedData = maxPriceSymbolRDD.groupByKey(new CustomPartition());

//		JavaRDD<Double> minMaxForPartition = stockGroupedData.mapPartitions(f)
		
	}

}

class MaxStock implements Comparator<Stock>, Serializable {

	@Override
	public int compare(Stock o1, Stock o2) {

		return (int) (o2.getStock() - o1.getStock());
	}
}

class CustomPartition extends Partitioner {

	@Override
	public int getPartition(Object arg0) {
		String t = (String) arg0;

		return t.charAt(0) % 'A';
	}

	@Override
	public int numPartitions() {

		return 6;
	}

}