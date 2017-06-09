
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;


public class MainProgram extends Configured implements Tool{
	public int run(String[] args) throws Exception
      {
		
		
	    String input1="/home/jagadish/Downloads/bigdatahw5-jagadish/wiki-micro.txt";
	    String output1="/home/jagadish/Downloads/bigdatahw5-jagadish/output";
	  Path in1 = new Path(input1);
      Path out1 = new Path(output1);
        JobConf set1 = new JobConf(getConf(), MainProgram.class);
        set1.setJobName("MainProgram");
        set1.setOutputKeyClass(Text.class);
        set1.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(set1, in1);
        FileOutputFormat.setOutputPath(set1, out1);
        set1.setMapperClass(Map.class);
        set1.setReducerClass(Reduce.class);
        JobClient.runJob(set1);
		
		
		
		
		
		
		    String input="/home/jagadish/Downloads/bigdatahw5-jagadish/output/part-00000";
		    String output="/home/jagadish/Downloads/bigdatahw5-jagadish/output/output";
    	  Path in = new Path(input);
          Path out = new Path(output);
            JobConf set = new JobConf(getConf(), MainProgram.class);
            set.setJobName("Main");
            set.setOutputKeyClass(Text.class);
            set.setOutputValueClass(FloatWritable.class);
            FileInputFormat.addInputPath(set, in);
            FileOutputFormat.setOutputPath(set, out);
            set.setMapperClass(Map1.class);
            set.setReducerClass(Reduce1.class);
           for(int i=0;i<5;i++)
            {
            JobClient.runJob(set);
           input=output+"/part-00000";
           in =new Path(input);
            
           output+="/output";
           out =new Path(output);
            
           FileInputFormat.addInputPath(set, in);
           FileOutputFormat.setOutputPath(set, out); 
            }
           
         
           String input2=input;
		    String output2=output;;
   	  Path in2 = new Path(input2);
         Path out2 = new Path(output2);
           JobConf set2 = new JobConf(getConf(), MainProgram.class);
           set2.setJobName("Main1");
           set2.setOutputKeyClass(FloatWritable.class);
           set2.setOutputValueClass(Text.class);
           FileInputFormat.addInputPath(set2, in2);
           FileOutputFormat.setOutputPath(set2, out2);
           set2.setMapperClass(Map2.class);
           set2.setReducerClass(Reducer2.class);
           JobClient.runJob(set2);
           
           
	return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
        int fin = ToolRunner.run(new Configuration(), new MainProgram(),args);
            System.exit(fin);
      }
}
