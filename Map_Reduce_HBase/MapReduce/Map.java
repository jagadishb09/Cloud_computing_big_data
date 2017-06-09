package bigdatahw1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


import java.io.IOException;
import java.util.StringTokenizer;


public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
      
      private final static IntWritable words = new IntWritable(1);
      private Text word = new Text();
      public void map(LongWritable k, Text t, OutputCollector<Text, IntWritable> result, Reporter fil) throws IOException
      {

            String line = t.toString();
            line=line.toLowerCase();
            String line1=new String("abcdefghi");
            String line2=new String(" ");
            String line3=new String("");


            Pattern p1=Pattern.compile("\\s");
            Matcher m1=p1.matcher(line);
            line=m1.replaceAll(line1);
            

            Pattern p =  Pattern.compile("[^a-zA-Z]");
            Matcher m=p.matcher(line);
            line=m.replaceAll(line3);
            

            Pattern p2 =  Pattern.compile("abcdefghi");
            Matcher m2=p2.matcher(line);
            line=m2.replaceAll(line2);

            StringTokenizer one = new StringTokenizer(line);
            StringTokenizer two = new StringTokenizer(line);
    	    String[][] token1 =new String[500][2];
    	    String[][] token2 =new String[500][2];
    	    Integer x=new Integer(0);
    	    Integer y=new Integer(0);
    	    int a=0;
    	    int b=0;

            while (one.hasMoreTokens())
            {
            token1[a][0]=one.nextToken();
            token1[a][1]=x.toString();
            while (two.hasMoreTokens())
            {

 		token2[b][0]=two.nextToken();
 		token2[b][1]=y.toString();


    if(token1[a][1].equals(token2[b][1]))

    {}
    else
    {
            		
               
                   	 word.set(token1[a][0].concat("-".concat(token2[b][0])));

                    	result.collect(word, words);
			}
    y++;
            }
            y=0;
            two=new StringTokenizer(line);
            x++;
            }

       }
}