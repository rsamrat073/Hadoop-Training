import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.out.println("usage: [input] [output]");
//			System.exit(-1);
//		}

		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(2);
		// job.setPartitionerClass(MyPartioner.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://sandbox.hortonworks.com:8020/Health"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://sandbox.hortonworks.com:8020/Health_Reduce"));

		job.setJarByClass(WordCount.class);

		job.submit();

	}
}