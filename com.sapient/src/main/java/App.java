import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class App 
{
    public static void main( String[] args )
    {
    	Cluster cluster = Cluster.builder().addContactPoint("192.168.133.128").withPort(9042).build();
    	cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(560000);
//        Cluster cluster = Cluster.builder()
//                          .addContactPoints("192.168.133.128")
//                          .build();
        Session session = cluster.connect();
        String cqlStatement = "CREATE KEYSPACE myfirstcassandradb WITH " + 
                              "replication = {'class':'SimpleStrategy','replication_factor':1}";        
        session.execute(cqlStatement);

        String cqlStatement2 = "CREATE TABLE myfirstcassandradb.users (" + 
                               " user_name varchar PRIMARY KEY," + 
                               " password varchar " + 
                               ");";
        session.execute(cqlStatement2);

        System.out.println("Done");
        System.exit(0);
    }
}