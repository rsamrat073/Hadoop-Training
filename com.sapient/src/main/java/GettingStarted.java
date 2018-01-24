

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

 
public class GettingStarted {
 
 
	public static void main(String[] args) {
 
		Cluster cluster;
		Session session;
 
		// Connect to the cluster and keyspace "demo"
  	//cluster = Cluster.builder().addContactPoint("192.168.133.128").build();

	DCAwareRoundRobinPolicy policyObject =   DCAwareRoundRobinPolicy.builder().withLocalDc("BLR").withUsedHostsPerRemoteDc(1).build();
		
	cluster = Cluster.builder()
			    .addContactPoints("10.150.222.36", "10.150.222.104", "10.150.222.83")
				//.withCredentials("yourusername", "yourpassword")
			    .withLoadBalancingPolicy(policyObject)
			    //.withLoadBalancingPolicy(new RoundRobinPolicy())
			    
			    .withQueryOptions( new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE)) // please give your DC name here
			    .build(); 
	//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1522);
  	session = cluster.connect("samrat_keyspace");
	
    session.execute("create table if not exists users1(lastname text primary key,age int,city text,email text,firstname text)");
		// Insert one record into the users table
		session.execute("INSERT INTO users1 (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
 
		// Use select to get the user we just entered
		ResultSet results = session.execute("SELECT * FROM users1 WHERE lastname='Jones'");
		for (Row row : results) {
  		System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
		}
 
		// Update the same user with a new age
    session.execute("update users1 set age = 36 where lastname = 'Jones'");
 
		// Select and show the change
		results = session.execute("select * from users1 where lastname='Jones'");
		for (Row row : results) {
    	System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
		}
 
		// Delete the user from the users table
    session.execute("DELETE FROM users1 WHERE lastname = 'Jones'");
 
		// Show that the user is gone
    results = session.execute("SELECT * FROM users1");
    for (Row row : results) {
    	System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"),  row.getString("city"), row.getString("email"), row.getString("firstname"));
		}
 
		// Clean up the connection by closing it
    cluster.close();
	}
}