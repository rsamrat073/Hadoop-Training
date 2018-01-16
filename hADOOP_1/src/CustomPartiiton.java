import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartiiton extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text arg0, IntWritable arg1, int arg2) {

			
		
		
		
		return 1;
	}

}
