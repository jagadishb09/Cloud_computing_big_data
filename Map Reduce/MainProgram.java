import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;


public class MainProgram extends Configured implements Tool{
	public int run(String[] args) throws Exception
      {
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
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
        int fin = ToolRunner.run(new Configuration(), new MainProgram(),args);
            System.exit(fin);
      }
}