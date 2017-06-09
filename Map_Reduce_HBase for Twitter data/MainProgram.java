package bighw4;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.util.Bytes.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.conf.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.*;


public class MainProgram extends Configured implements Tool{
	static String path = "/home/jagadish/Downloads/Homework4/java-data";
	public int run(String[] args) throws Exception
      {
  	  		Configuration conf = HBaseConfiguration.create();
		  HTable hTable = new HTable(conf, "result");
		
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
        	int value[]=new int[20];
        	for(int i=0;i<20;i++)
        	{
        		value[i]=0;
        	}
        	
        	while((line=bufferedreader.readLine())!=null)
        	{
        	line1=line.split("	");
        	for(int i=0;i<20;i++)
        	{
        		if(Integer.parseInt(line1[1])>value[i])
        		{
        			for(int j=19;j>i;j--)
        			{
        			value[j]=value[j-1];	
        			
        			}
        			value[i]=Integer.parseInt(line1[1]);
        			break;
        		}
        		
        	}
        	}
        	//for(int i=0;i<20;i++)
        		//System.out.println(value[i]);
        	FileReader  inputFile1= new FileReader(args[2]);
        	bufferedreader=new BufferedReader(inputFile1);
        	while((line=bufferedreader.readLine())!=null)
        	{
        		line1=line.split("	");
        		//System.out.println(line1[1]);
        		for(int i=0;i<20;i++)
            	{
        			if(Integer.parseInt(line1[1])==value[i])
        			{
        				Put putobj=new Put(Bytes.toBytes(line1[0]));
        				System.out.println(line1[0]);
        				putobj.add(Bytes.toBytes("frequency"),Bytes.toBytes("frequency"),Bytes.toBytes(line1[1]));
        				hTable.put(putobj);
        				break;
        			}
            	}
        		
        	}
        	
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
    	  readFile(new File(path));
        int fin = ToolRunner.run(new Configuration(), new MainProgram(),args);
            System.exit(fin);
      }
      private static void readFile(File file) throws Exception {
  		File[] files = file.listFiles();

  		if (files == null || files.length == 0) {
  			return;
  		}

  		for (File f : files) {
  			if (!f.isDirectory()) {
  				String fileName = f.getName();
  				String regEx = "NY2DC\\w+";
  				Pattern p = Pattern.compile(regEx);
  				Matcher m = p.matcher(fileName);
  				if (m.find()) {
  					readCSV(f.getAbsolutePath());
  				}
  			}
  		}
  	}
  	
      public static void readCSV(String path) throws Exception{
    	  Configuration conf = HBaseConfiguration.create();
  		  HTable hTable = new HTable(conf, "twitter");
    	  File file = new File(path);
          BufferedReader reader = null;
          String line3=new String("");
          Pattern p =  Pattern.compile("[^a-zA-Z\\s]");
          try {
              reader = new BufferedReader(new FileReader(file));
              String temp = null;
              while((temp = reader.readLine()) != null){
                  String[] pair = temp.split(",");
                  //System.out.println("key:" + pair[0]);
                  Put putobj=new Put(Bytes.toBytes(pair[0]));
                  String value = new String(pair[1].getBytes(), "UTF-8");
                  Matcher m=p.matcher(value);
          		  value=m.replaceAll(line3);
                  putobj.add(Bytes.toBytes("text"),Bytes.toBytes("text"),Bytes.toBytes(value));
                  //System.out.println("value:" + value);
                  hTable.put(putobj);
              }
          } catch (FileNotFoundException e) {
              e.printStackTrace();
          } catch (IOException e) {
              e.printStackTrace();
          }finally {
              if (reader != null){
                  try {
                      reader.close();
                  } catch (IOException e) {
                      e.printStackTrace();
                  }
              }
          }
          hTable.close();
      }
}