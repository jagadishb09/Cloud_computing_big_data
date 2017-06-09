

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


public class Reduce1 extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>
{

	@Override
	public void reduce(Text arg0, Iterator<FloatWritable> arg1,
			OutputCollector<Text, FloatWritable> arg2, Reporter arg3)
			throws IOException {
		Float value=(float) 0;
		
			value = arg1.next().get();
		
		arg2.collect(arg0, new FloatWritable(value));
		
	}
}
