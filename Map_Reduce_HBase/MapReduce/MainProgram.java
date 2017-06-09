package bigdatahw1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.util.Bytes.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.conf.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.*;


public class MainProgram extends Configured implements Tool{
	public int run(String[] args) throws Exception
      {
		Configuration conf = HBaseConfiguration.create();
		HTable hTable = new HTable(conf, "bighw3");
    	  Path in = new Path(args[0]);
          Path out = new Path(args[1]);
            JobConf set = new JobConf(getConf(), MainProgram.class);
            set.setJobName("MainProgram");
            set.setOutputKeyClass(Text.class);
            set.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(set, in);
            FileOutputFormat.setOutputPath(set, out);
            set.setMapperClass(Map.class);
            set.setReducerClass(Reducer1.class);
            JobClient.runJob(set);
            	FileReader  inputFile= new FileReader(args[2]);
            	BufferedReader bufferedreader=new BufferedReader(inputFile);
            	String line;
            	String line1[];
            	Integer i=new Integer(0);
            	while((line=bufferedreader.readLine())!=null)
            	{
            	line1=line.split("	");
            	    Put putobj=new Put(Bytes.toBytes(line1[0]));
                    putobj.add(Bytes.toBytes("number"),Bytes.toBytes("number"),Bytes.toBytes(line1[1]));
                    hTable.put(putobj);
                    i++;
                    }
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
        int fin = ToolRunner.run(new Configuration(), new MainProgram(),args);
            System.exit(fin);
      }
}