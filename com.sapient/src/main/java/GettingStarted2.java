
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

public class GettingStarted2 {

	public static void main(String[] args) {

		Cluster cluster;
		Session session;

		DCAwareRoundRobinPolicy policyObject = DCAwareRoundRobinPolicy.builder().withLocalDc("BLR")
				.withUsedHostsPerRemoteDc(1).build();

		cluster = Cluster.builder().addContactPoints("10.150.222.36", "10.150.222.104", "10.150.222.83")

				.withLoadBalancingPolicy(policyObject)

				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE)) 
				.build();	

		session = cluster.connect("samrat_keyspace");

		
		ResultSet results = session.execute("SELECT * FROM employees1 ");
		for (Row row : results) {
			System.out.println(row.getString("city"));
		}

//		session.execute("update users1 set age = 36 where lastname = 'Jones'");
//
//		results = session.execute("select * from users1 where lastname='Jones'");
//		for (Row row : results) {
//			System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
//		}

	

		cluster.close();
	}
}