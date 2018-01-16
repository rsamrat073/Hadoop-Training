import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
 
 
public class WordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
  
 private IntWritable totalWordCount = new IntWritable();
  
 @Override
 public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
	 System.out.println(values.toString());
	 
	 
 }
}
