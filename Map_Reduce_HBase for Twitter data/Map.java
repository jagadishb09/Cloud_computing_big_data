package bighw4;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
import java.io.IOException;
import java.util.StringTokenizer;


public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
      
      private final static IntWritable words = new IntWritable(1);
      private Text word = new Text();
      public void map(LongWritable k, Text t, OutputCollector<Text, IntWritable> result, Reporter fil) throws IOException
      {

    	  FileReader  inputFile1= new FileReader("/home/jagadish/Downloads/Homework4/stop-word-list.csv");
    	 // BufferedReader br=new BufferedReader(inputFile1);
    	    BufferedReader br=new BufferedReader(inputFile1);
    	    String temp1=br.readLine();
    	    String[] temp=temp1.split(",");
    	    
           // String line1=new String("abcdefghi");
            //String line2=new String(" ");
            String line3=new String("");


            Pattern p1=Pattern.compile("\\s");
            for(int z=0;z<temp.length;z++)
            {
            Matcher m1=p1.matcher(temp[z]);
            temp[z]=m1.replaceAll(line3);
            }
    	  Configuration conf = HBaseConfiguration.create();
  		  HTable hTable = new HTable(conf, "twitter");
  		  Scan scan=new Scan();
  		  ResultScanner rs1=hTable.getScanner(scan);
  		  for (Result result1 : rs1){
            String line = Bytes.toString(result1.value());
            line=line.toLowerCase();

            

            Pattern p =  Pattern.compile("[^a-zA-Z\\s]");
            Matcher m=p.matcher(line);
            line=m.replaceAll(line3);

            StringTokenizer one = new StringTokenizer(line);
            //StringTokenizer two = new StringTokenizer(line);
    	    String[][] token1 =new String[500][2];

            while (one.hasMoreTokens())
            {
            	//System.out.println("hi");
            	    
            	    int count10=0;

                    token1[1][0]=one.nextToken();  
            		for(int z=0;z<temp.length;z++)
            		{
            			if(temp[z].equals(token1[1][0]))
            			{
            				count10=1;
            				break;
            			}
            		}
                       if(count10==0)
                       {
                   	   word.set(token1[1][0]);

                    	result.collect(word, words);}
                       count10=0;
			}
            }
	    hTable.close();
      }
       }