package com.sapient;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Assignment {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\Users\\TEMP\\Desktop\\SparkWS\\");

		SparkSession spark = SparkSession.builder().master("local[*]").appName("JavaWordCount").getOrCreate();
		SparkContext jsc = spark.sparkContext();
		JavaRDD<String> custs = spark.read()
				.textFile("C:/Users/TEMP/Downloads/Spark2/Spark2/src/main/java/com/sapient/custs").javaRDD();
		JavaRDD<String> taxs = spark.read()
				.textFile("C:/Users/TEMP/Downloads/Spark2/Spark2/src/main/java/com/sapient/txns").javaRDD();

		JavaRDD<String> custs1 = custs.flatMap(s -> Arrays.asList(s.split("/n")).iterator());
		JavaRDD<String> taxs1 = taxs.flatMap(s -> Arrays.asList(s.split("/n")).iterator());

		// for (String t : custs1.collect()) {
		// System.out.println(t);
		// }
		// for (String t : taxs1.collect()) {
		// System.out.println(t);
		// }

		JavaRDD<String> reRDD = taxs1.repartition(2);
		JavaPairRDD<Integer, Double> taxns2 = reRDD
				.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",")[2]), Double.parseDouble(s.split(",")[3])));

		JavaPairRDD<Integer, Double> taxns3 = taxns2.reduceByKey((i1, i2) -> i1 + i2);
		System.out.println(taxns3.collect().toString());

		JavaRDD<String> reRDD2 = custs1.repartition(2);
		JavaPairRDD<Integer, String> custs2 = reRDD2.mapToPair(
				s -> new Tuple2<>(Integer.parseInt(s.split(",")[0]), s.split(",")[1] + " " + s.split(",")[2]));

		System.out.println(custs2.collect().toString());

		JavaPairRDD<Integer, Tuple2<String, Double>> results = custs2.join(taxns3);
		//results.values().collect().sort(new TupleMapLongComparator());;
		 for (Tuple2<String, Double> t : results.values().collect()) {
				 System.out.println(t);
				 }
//		System.out.println(results);
		Thread.sleep(4545445);
	}

}

class TupleMapLongComparator implements Comparator<Tuple2<String, Double>>, Serializable {

	@Override
	public int compare(Tuple2<String, Double> arg0, Tuple2<String, Double> arg1) {
		// TODO Auto-generated method stub
		return (int) (arg0._2()-arg1._2());
	}
	
}
