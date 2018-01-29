package client;

import java.io.File;
import java.util.List;

import org.apache.derby.tools.sysinfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import breeze.linalg.min;
import model.Employee;
import scala.Tuple2;
import utils.SparkContext;

public class EmployeeDataAnalyzer {

	private JavaRDD<Employee> emps;

	public EmployeeDataAnalyzer() {

		JavaRDD<String> empsAll = SparkContext.getContext().textFile(new File(
				"C:\\Users\\samra\\Desktop\\Sapient\\Spark Beginnig\\Assignment3\\src\\main\\resources\\employees.txt")
						.getAbsolutePath());
		String header = empsAll.first();
		emps = empsAll.filter(data -> !data.equals(header)).map(line -> {
			String[] p = line.split(",");

			Employee e = new Employee();
			e.setEmpNumber(Integer.parseInt(p[0]));
			e.setManagerName(p[1]);
			e.setMonth(p[2]);
			e.setSlaesPerMonth(Float.parseFloat(p[3].trim().replace('$', ' ')));
			e.setSlaesPerYear(Float.parseFloat(p[4].trim().replace('$', ' ')));
			e.setCommission(Float.parseFloat(p[5].contains("-") ? "0.00" : p[5].trim().replace('$', ' ')));

			return e;
		});

	}

	public void getEmployeeSalesPerMonth() {
		emps.map(data ->

		new Tuple2<>(data.getEmpNumber(), "Month->" + data.getMonth() + " Sales->" + data.getSlaesPerMonth())

		).groupBy(data -> data._1()).collect().stream().forEach(data -> System.out.println(data._2()));

	}

	public void calculateTotalSalePerEmployee() {
		emps.mapToPair(data -> new Tuple2<Integer, Float>(data.getEmpNumber(), data.getSlaesPerYear()))
				.reduceByKey((x, y) -> x + y).collect().forEach(s -> System.out.println(s));

	}

	public void calculateAverageEmployeeMonthlySales() {
		JavaPairRDD<Integer, Tuple2<Float,Integer>> countSalesPerMonth = emps.mapToPair(data -> new Tuple2<Integer, Float>(data.getEmpNumber(), data.getSlaesPerMonth()))
				.mapValues(d -> new Tuple2<>(d, 1));

		JavaPairRDD<Integer, Float> avgSales = countSalesPerMonth.reduceByKey((x,y)->new Tuple2<>(x._1()+y._1(),x._2()+y._2()))
		.mapToPair(d->new Tuple2<>(d._1(),d._2()._1()/d._2()._2()));
		avgSales.collect().forEach(s->System.out.println(s));
		
	}
	
	public void maxMinSalesOverYear() {
		
		
		JavaPairRDD<Integer, Tuple2<Integer,Float>> actualSalesPerYear = 
				emps.mapToPair(data->new Tuple2<>(data.getEmpNumber(),new Tuple2<>(data.getEmpNumber(),data.getSlaesPerYear())));
		actualSalesPerYear.mapValues(t->new Tuple2<>(t._2(),1.0F)).
	reduceByKey((x,y)->new Tuple2<>(x._1()>y._1()?x._1():y._1(),x._1()>y._1()?y._1():x._1()))
		.collect().forEach(s->System.out.println(s));
		
		
	
	}

	public static void main(String[] args) {

		new EmployeeDataAnalyzer().maxMinSalesOverYear();

	}

}