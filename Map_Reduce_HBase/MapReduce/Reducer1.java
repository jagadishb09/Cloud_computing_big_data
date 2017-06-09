package bigdatahw1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.Iterator;


public class Reducer1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
      
      public void reduce(Text k, Iterator<IntWritable> results, OutputCollector<Text, IntWritable> result, Reporter fil) throws IOException
      {
            int total = 0;
            while (results.hasNext())
            {
               total += results.next().get();
            }
            result.collect(k, new IntWritable(total));
            
      }
}