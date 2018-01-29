package client;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import model.Customer;
import scala.Tuple2;
import utils.SparkContext;

public class CustomerDataAnalyzer2 {

	private JavaRDD<Customer> custs;

	public CustomerDataAnalyzer2() {

		JavaRDD<String> custAll = SparkContext.getContext()
				.textFile(new File(
						"D:\\GitHUB\\BigData\\Hadoop-Training\\Assignment3\\src\\main\\resources\\customers_with_delimiters.csv")
								.getAbsolutePath());
		String header = custAll.first();
		custs = custAll.filter(data -> !data.equals(header)).map(line -> {
			String[] pWithCommaAndQuotes = null;
			Customer cust = new Customer();
			if (((String) line).contains("\"")) {

				pWithCommaAndQuotes = line.split("\"");
				//Arrays.asList(pWithCommaAndQuotes[2].split(",")).forEach(s->System.out.println(s));
				
				cust.setCustomerID(Integer.parseInt(pWithCommaAndQuotes[0].split(",")[0]));
				cust.setCustomerFirstName(pWithCommaAndQuotes[0].split(",")[1]);
				cust.setCustomerSecondName(pWithCommaAndQuotes[0].split(",")[2]);
				cust.setCustomerEmailID(pWithCommaAndQuotes[0].split(",")[3]);
				cust.setCustomerPassword(pWithCommaAndQuotes[0].split(",")[4]);
				cust.setCustomerStreetAddr(pWithCommaAndQuotes[1].replace(","," "));
				cust.setCustomerCity(pWithCommaAndQuotes[2].split(",")[1]);
				cust.setCustomerState(pWithCommaAndQuotes[2].split(",")[2]);
				cust.setCustomerAddrZip(Integer.parseInt(pWithCommaAndQuotes[2].split(",")[3]));
			} else {
				pWithCommaAndQuotes = line.split(",");
				cust.setCustomerID(Integer.parseInt(pWithCommaAndQuotes[0]));
				cust.setCustomerFirstName(pWithCommaAndQuotes[1]);
				cust.setCustomerSecondName(pWithCommaAndQuotes[2]);
				cust.setCustomerEmailID(pWithCommaAndQuotes[3]);
				cust.setCustomerPassword(pWithCommaAndQuotes[4]);
				cust.setCustomerStreetAddr(pWithCommaAndQuotes[5]);
				cust.setCustomerCity(pWithCommaAndQuotes[6]);
				cust.setCustomerState(pWithCommaAndQuotes[7]);
				cust.setCustomerAddrZip(Integer.parseInt(pWithCommaAndQuotes[8]));

			}

			return cust;
		});

	}

	public static void main(String[] args) {
		new CustomerDataAnalyzer2().display();

	}

	private void display() {
		
		JavaRDD<String> adress=custs.map(data->data.getCustomerStreetAddr());		
		adress=adress.flatMap(data->Arrays.asList(data.split(" ")).iterator());
		
		JavaPairRDD<String, Integer>adressRes=adress.mapToPair(s->new Tuple2<>(s,1));
		adressRes.reduceByKey((x,y)->x.intValue()+y.intValue()).collect().forEach(s->System.out.println(s._1()+"-->"+s._2()));
		
	}
}
