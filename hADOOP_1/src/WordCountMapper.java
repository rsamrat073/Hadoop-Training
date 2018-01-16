
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context contex) throws IOException, InterruptedException {

		contex.write(new Text(value.toString().split(",")[9]), 
				new Text(value.toString().split(",")[8]));

	}

}
