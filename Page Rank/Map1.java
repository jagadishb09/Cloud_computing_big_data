
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.IOException;
import java.util.StringTokenizer;


public class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable>
{

	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, FloatWritable> arg2, Reporter arg3)
			throws IOException {
		
		Text key=new Text();
        String line = arg1.toString();
        int a=0;
        String[] arr=line.split("	");
        String line1 = null;
        line=arr[0];
      
        while(a<arr.length)
        {	
        line1=arr[a];

        a++;
        }
        
        Float value= (float) (0.85+(0.15*Float.parseFloat(line1)));
        
        FloatWritable value1=new FloatWritable(value);
        
        key.set(line);
        
        arg2.collect(key, value1);

		
	}

}
