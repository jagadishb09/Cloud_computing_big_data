

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


import java.io.IOException;
import java.util.StringTokenizer;


public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable>
{
      
	static int count;
      public void map(LongWritable k, Text t, OutputCollector<Text, FloatWritable> result, Reporter fil) throws IOException
      {

            String line = t.toString();
            line=line.toLowerCase();
            String line3="_";
            int a=1;
            
            Pattern p1=Pattern.compile("\\s");
            Matcher m1=p1.matcher(line);
            line=m1.replaceAll(line3);
            
            
            int begin=line.indexOf("<title>");            
            int end=line.indexOf("</title>",begin+7);
            
            if(begin<0 || end<0)
            	return;
            begin+=7;
            
            String title=line.substring(begin, end);
            begin=end+8;
            begin=line.indexOf("<text",begin);
            begin=line.indexOf(">",begin);
            
            end=line.indexOf("</text>",begin+6);
            
            if(begin<0 || end<0)
            	return;
            begin+=6;
            
            String text=line.substring(begin,end);
            String text1 = "";
            count=1;
            //begin=end+7;
            begin=text.indexOf("[[");
            
            end=text.indexOf("]]",begin+2);
            int x;
            
            while(begin>0 && end>0)
            {	
            	x=end;
            	if(a==1)
            		count=0;
            	a++;
            begin=begin+2;

            int end1=text.substring(begin,end).indexOf('|');
            if(end1>0)
            	{
            	end=begin+end1;
            	text1+=text.substring(begin,end)+",";  
            	begin=end+1;
  	
            	}
            else
            {
            //begin=end+2;
        	text1+=text.substring(begin,end)+",";  
            }
            begin=x+2;
            begin=text.indexOf("[[",begin);
            
            end=text.indexOf("]]",begin+2);
            count++;
            
            }
            
            Text k1=new Text();
            

            	
            	k1.set(title+"	<"+text1+">");
            
            

            result.collect(k1,new FloatWritable(1f/(float)count));
            
       }
}
