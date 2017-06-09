

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.IOException;
import java.util.StringTokenizer;


public class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable,Text>
{

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<FloatWritable,Text> arg2, Reporter arg3)
			throws IOException {
		
		String line=arg1.toString();
		String[] arr=line.split("	");
		
		Text key1=new Text();
		key1.set(arr[0]);
		
		float key=(float) 0;
		key=Float.parseFloat(arr[1]);
		
		
		arg2.collect(new FloatWritable(key),key1);
		
		
		
	}
	}
