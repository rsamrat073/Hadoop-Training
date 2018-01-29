package client;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import model.Customer;
import model.ReferenceAddress;
import scala.Tuple2;
import utils.SparkContext;

public class CustomerDataAnalyzer {

	private JavaRDD<Customer> custs;
	private JavaPairRDD<String, String> refsAddr;

	public CustomerDataAnalyzer() {

		JavaRDD<String> custAll = SparkContext.getContext()
				.textFile(new File(
						"D:\\GitHUB\\BigData\\Hadoop-Training\\Assignment3\\src\\main\\resources\\customers.csv")
								.getAbsolutePath());
		String header = custAll.first();
		custs = custAll.filter(data -> !data.equals(header)).map(line -> {
			String[] p = line.split(",");
			Customer cust = new Customer();
			cust.setCustomerID(Integer.parseInt(p[0]));
			cust.setCustomerFirstName(p[1]);
			cust.setCustomerSecondName(p[2]);
			cust.setCustomerEmailID(p[3]);
			cust.setCustomerPassword(p[4]);
			cust.setCustomerStreetAddr(p[5]);
			cust.setCustomerCity(p[6]);
			cust.setCustomerState(p[7]);
			cust.setCustomerAddrZip(Integer.parseInt(p[8]));

			return cust;
		});

		JavaRDD<String> refAll = SparkContext.getContext()
				.textFile(new File(
						"D:\\GitHUB\\BigData\\Hadoop-Training\\Assignment3\\src\\main\\resources\\referencedata.csv")
								.getAbsolutePath());

		refsAddr = refAll.mapToPair(data -> {

			String[] p = data.split(",");

			return new Tuple2<String, String>(p[1], p[0]);
		});

	}

	public void displayCustomerAddress() {
		//System.out.println(Arrays.asList(custs.collect().get(1).getCustomerStreetAddr().split(" ")));
		Map<String, String> refMap = refsAddr.collectAsMap();
		custs.collect().forEach(s -> {

			if(refMap.get(s.getCustomerStreetAddr().split(" ")[s.getCustomerStreetAddr().split(" ").length-1])!=null){
				s.setCustomerStreetAddr(s.getCustomerStreetAddr().replace(s.getCustomerStreetAddr().split(" ")[s.getCustomerStreetAddr().split(" ").length-1],
						refMap.get(s.getCustomerStreetAddr().split(" ")[s.getCustomerStreetAddr().split(" ").length-1])));
			}
			System.out.println(s);

		});
	}

	

	public static void main(String[] args) {
		new CustomerDataAnalyzer().displayCustomerAddress();

	}

}
