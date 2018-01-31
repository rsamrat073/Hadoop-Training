package client;

import java.io.File;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import model.Employee;
import scala.Tuple2;
import utils.SparkContext;

public class EmployeeDataAnalyzer {

	private JavaRDD<Employee> emps;
	
	
	private SparkSession spark;

	public EmployeeDataAnalyzer() {

		System.setProperty("hadoop.home.dir", "D:\\GitHUB\\BigData\\Hadoop-Training\\Spark2\\");
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		spark = SparkSession.builder().master("local[*]").appName("Customer Data")
				.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();
		
		JavaRDD<String> empsAll = SparkContext.getContext(spark.sparkContext())
				.textFile(new File("src\\main\\resources\\employees.txt").getAbsolutePath());
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

	public void getEmployeeSalesPerYear() {

		// Total Sales per Month
		emps.mapToPair(data -> new Tuple2<>(data.getEmpNumber(), data.getSlaesPerMonth()))
				.reduceByKey((x, y) -> x.floatValue() + y.floatValue()).collect().forEach(s -> System.out.println(s));

		// Total Sales per Year
		emps.mapToPair(data -> new Tuple2<>(data.getEmpNumber(), data.getSlaesPerYear()))
				.reduceByKey((x, y) -> x.floatValue() + y.floatValue()).collect().forEach(s -> System.out.println(s));

	}

	public void calculateTotalSalePerEmployee() {
		emps.mapToPair(data -> new Tuple2<Integer, Float>(data.getEmpNumber(), data.getSlaesPerYear()))
				.reduceByKey((x, y) -> x + y).collect().forEach(s -> System.out.println(s));

	}

	public JavaPairRDD<Integer, Float> calculateAverageEmployeeMonthlySales() {
		JavaPairRDD<Integer, Tuple2<Float, Integer>> countSalesPerMonth = emps
				.mapToPair(data -> new Tuple2<Integer, Float>(data.getEmpNumber(), data.getSlaesPerMonth()))
				.mapValues(d -> new Tuple2<>(d, 1));

		JavaPairRDD<Integer, Float> avgSales = countSalesPerMonth
				.reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()))
				.mapToPair(d -> new Tuple2<>(d._1(), d._2()._1() / d._2()._2()));
		avgSales.collect().forEach(s -> System.out.println(s));
		return avgSales;

	}

	public void maxMinSalesOverYear() {

		JavaPairRDD<Integer, Tuple2<Integer, Float>> actualSalesPerYear = emps.mapToPair(
				data -> new Tuple2<>(data.getEmpNumber(), new Tuple2<>(data.getEmpNumber(), data.getSlaesPerYear())));
		actualSalesPerYear.mapValues(t -> new Tuple2<>(t._2(), 1.0F))
				.reduceByKey(
						(x, y) -> new Tuple2<>(x._1() > y._1() ? x._1() : y._1(), x._1() > y._1() ? y._1() : x._1()))
				.collect().forEach(s -> System.out.println(s));

	}

	public void maxMinAvgSalesOverMonth() {

		JavaPairRDD<Integer, Float> avgSales = calculateAverageEmployeeMonthlySales();
		
		System.out.println("MAX-->"+avgSales.mapToPair(data->new Tuple2<>(data._2(),data._1()))
		.sortByKey(false).first()+" MIN-->"+avgSales.mapToPair(data->new Tuple2<>(data._2(),data._1()))
		.sortByKey(true).first());

	}

	
	public void getTotalSalesPerYear(){
		
		spark.sql("use retail_db");
		//spark.createDataFrame(emps, Employee.class).createOrReplaceTempView("employees");
		spark.sql("select empNumber,sum(slaesPerMonth) as TOTALSALES from employess group by empNumber order by empNumber").repartition(1).write().format("json").save("src\\main\\resources\\problem_1.1\\");;
		spark.sql("select empNumber,sum(slaesPerYear) as TOTALSALES from employess group by empNumber order by empNumber").repartition(1).write().format("json").save("src\\main\\resources\\problem_1.2");
		spark.sql("select empNumber,avg(slaesPerMonth) as AVGSALES from employess group by empNumber order by empNumber").repartition(1).write().format("json").save("src\\main\\resources\\problem_1.3");
		spark.sql("select empNumber,min(slaesPerYear) as MINSALE,max(slaesPerYear) as MAXSALE from employess group by empNumber order by empNumber").repartition(1).write().format("json").save("src\\main\\resources\\problem_1.4");
		
	}
	
	
	public static void main(String[] args) {

		new EmployeeDataAnalyzer().getTotalSalesPerYear();

	}

}
