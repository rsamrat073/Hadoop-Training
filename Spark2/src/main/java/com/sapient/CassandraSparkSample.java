package com.sapient;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class CassandraSparkSample {
	
	public static void main (String []args) {
		System.setProperty("hadoop.home.dir", "D:\\Hadoop Materials\\");
		SparkConf sparkConf = new SparkConf().setAppName("spark cassandra app").setMaster("local[2]")
				.set("spark.cassandra.connection.host", "10.150.222.36");
  
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        
        try (Session session = connector.openSession()) {
//            session.execute("DROP KEYSPACE IF EXISTS java_api");
//            session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE fateh_keyspace.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
//            session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
//            session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
        }
        
	}
}
