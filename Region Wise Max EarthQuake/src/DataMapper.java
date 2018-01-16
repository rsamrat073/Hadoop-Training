import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private Text region = new Text();
	private final static DoubleWritable magnitude = new DoubleWritable();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		System.out.println(value.toString()+" "+value.toString());
		
		if(!value.toString().startsWith("Src")){
			region.set(value.toString().split(",")[11]);
			magnitude.set(Double.parseDouble(value.toString().split(",")[8]));
			context.write(region,magnitude);
		}

	
	}

}
