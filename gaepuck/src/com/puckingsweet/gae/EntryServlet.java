package com.puckingsweet.gae;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import java.net.URLEncoder;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

@SuppressWarnings("serial")
public class EntryServlet extends HttpServlet {

	//TODO add proper logging
	public static final String PAGESIZE = "1";
	public static final String ENTRYDOMAIN = "entry";
	
	public void init(ServletConfig conf) throws ServletException {
		try {
			//InputStream is = conf.getServletContext().getResourceAsStream("/WEB-INF/AwsCredentials.properties");
			String creds = "";
			InputStream is = new ByteArrayInputStream(creds.getBytes());
			PropertiesCredentials props = new PropertiesCredentials(is);
			conf.getServletContext().setAttribute("AwsCredentials.properties", props);
			
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		super.init(conf);
	}

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		//resp.setContentType("text/plain");
		resp.setContentType("application/json");
		System.out.println("Hello, world!!");

		
		PropertiesCredentials props = (PropertiesCredentials) getServletContext().getAttribute("AwsCredentials.properties");
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(props);
				

		try {
			JSONObject items = new JSONObject();
			JSONObject attrs = null;
			
			//Build and execute query
			SelectResult sr = sdb.select(buildQuery(req));
			String srNextToken = sr.getNextToken();
			if(srNextToken != null){
				srNextToken = URLEncoder.encode(srNextToken, "ISO-8859-1");
				items.put("nextToken", srNextToken);
				System.out.print("  srNextToken: " + srNextToken);
				System.out.println();
			}
			
			//Parse result set
			for (Item item : sr.getItems()) {
				attrs = new JSONObject();
				System.out.println("  Item");
				System.out.println("    Name: " + item.getName());
				
				for (Attribute attribute : item.getAttributes()) {
					attrs.put(attribute.getName(), attribute.getValue());
					System.out.println("      Attribute");
					System.out.println("        Name:  " + attribute.getName());
					System.out.println("        Value: " + attribute.getValue());
					
				}
				
				items.put(item.getName(), attrs);
				System.out.println("w4oot");
			}
			
			
			StringWriter out = new StringWriter();
			items.writeJSONString(out);
			String jsonText = out.toString();
			resp.getWriter().println(jsonText);
			System.out.println();
			 

		} catch (AmazonServiceException ase) {
			System.out
					.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SimpleDB, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SimpleDB, "
							+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}
	
	private SelectRequest buildQuery(HttpServletRequest req) {
		//TODO protect inputs from injection
		String entryId = null;
		String path = req.getPathInfo();
		if(path != null && !"".equals(path)){
			System.out.println("path: " + path);
			if(path.startsWith("/")){
				entryId = path.split("/", 3)[1];
				System.out.println("entryId: " + entryId);
			}
		}
		
		// Select data from a domain
		// Notice the use of backticks around the domain name in our select expression. 
		StringBuffer selectExpression = new StringBuffer("select * from `").append(ENTRYDOMAIN).append("` where create_date > '2003-06-21 02:00:00'");
		if(entryId != null && !"".equals(entryId)){
			selectExpression.append(" and itemName() = '").append(entryId).append("'");
		}
		selectExpression.append(" order by create_date desc");
		selectExpression.append(" limit ").append(PAGESIZE);
		
		System.out.println("Selecting: " + selectExpression.toString() + "\n");
		SelectRequest selectRequest = new SelectRequest(selectExpression.toString());
		
		String nextToken = (String)req.getParameter("nextToken");
		if(nextToken != null){
			selectRequest.setNextToken(nextToken);
			System.out.println("with nextToken: " + nextToken);
		}
		
		return(selectRequest);
	}
}
