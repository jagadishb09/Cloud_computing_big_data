

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.Iterator;


public class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>
{
      
      public void reduce(Text k, Iterator<FloatWritable> results, OutputCollector<Text, FloatWritable> result, Reporter fil) throws IOException
      {
            Float total = (float) 0;
            while (results.hasNext())
            {
               total += results.next().get();
            }
            result.collect(k, new FloatWritable(total));
      }
}
