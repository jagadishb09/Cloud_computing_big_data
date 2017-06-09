

import java.io.IOException;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.conf.*;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.getRow;
import org.apache.hadoop.conf.*;

import java.io.PrintWriter;
@WebServlet("/bigd")
public class bigd extends HttpServlet {
	private static final long serialVersionUID = 1L;
       

    public bigd() {
        super();
    }

	@SuppressWarnings("deprecation")
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		String number=request.getParameter("option");
		String oneword=request.getParameter("oneword");
		String twoword=request.getParameter("twoword");
		String twoword1=request.getParameter("twoword1");
		String line[];
		Configuration conf = HBaseConfiguration.create();
		HTable hTable = new HTable(conf, "bighw3");
		PrintWriter out = response.getWriter() ;
		if (Integer.parseInt(number)==2)
		{
	    Get get=new Get((twoword.toLowerCase()+"-"+twoword1.toLowerCase()).getBytes());
	    Result rs=hTable.get(get);
		  
	        out.println("<html>") ;
	        out.println("<head><title>Hello Servlet</title></head>") ;
	        out.println("<body>") ;
	     
	        out.println("<h1>" + twoword +" and "+ twoword1+"</h1>") ;
	        
	        out.print(" together occurs "+Bytes.toString(rs.value())+" times ");
	        
	        
	        out.println("</body>") ;
	        out.println("</html>") ;
	        out.close() ;
	        
		}
		if (Integer.parseInt(number)==1)
		{
	    Scan scan=new Scan();
	    String line1;
	    ResultScanner rs1=hTable.getScanner(scan);
	    for (Result result : rs1){
	    	

	    	line=(Bytes.toString(result.getRow())).split("-");
	    	if(Bytes.toString(result.getRow()).startsWith(oneword.toLowerCase()))
	    	{
	    		out.println("<html>") ;
		        out.println("<head><title>Hello Servlet</title></head>") ;
		        out.println("<body>") ;
		        
		         
		        
		        out.print("word with "+oneword+ "->"+line[1]+" occurs "+Bytes.toString(result.value())+" times ");
		        
		        out.print("<BR>");
		     
	    		
	    	}
	    	
	    }
	    out.println("</body>") ;
        out.println("</html>") ;
		 out.close();
	           
		}
	        hTable.close();
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doGet(request, response);
		
	}

}
