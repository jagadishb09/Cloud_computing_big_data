

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reducer2 extends MapReduceBase implements Reducer<FloatWritable,Text, FloatWritable,Text>
{

	@Override
	public void reduce(FloatWritable arg0,Iterator<Text>  arg1,
			OutputCollector<FloatWritable,Text> arg2, Reporter arg3)
			throws IOException {
		
		
		
		String value =new String("");
		while(arg1.hasNext())
		value+="-"+arg1.next();
		Text value1=new Text(value);
		arg2.collect(arg0, value1);
		
	}
	}
