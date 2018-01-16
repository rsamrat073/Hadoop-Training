import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	DoubleWritable maxMagnitude = new DoubleWritable(0);

	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context arg2) throws IOException, InterruptedException {

		Iterator<DoubleWritable> it = values.iterator();

		Double t = 0.0;

		while (it.hasNext()) {
			DoubleWritable temp = it.next();
			if (temp.get() > t) {
				t = temp.get();
			}
		}
		maxMagnitude.set(t);
		arg2.write(arg0, maxMagnitude);

	}
}
