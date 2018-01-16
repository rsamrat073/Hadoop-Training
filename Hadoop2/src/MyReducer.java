import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
@Override
protected void reduce(Text t, Iterable<IntWritable> i,
		Reducer<Text, IntWritable, Text, IntWritable>.Context c) throws IOException, InterruptedException {
	System.out.println("MyReducer.reduce()");
	// TODO Auto-generated method stub
	super.reduce(t, i, c);
}
}
