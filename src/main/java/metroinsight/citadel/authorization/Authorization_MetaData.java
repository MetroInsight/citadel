
/*
 * hardcoded path of /home/sandeep/MetroInsight/Codes/Citadel-Sandeep/citadel/src/main/resources/hbase-site.xml
 */

package metroinsight.citadel.authorization;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import io.vertx.core.json.JsonObject;


/*
 * inserts user token into the HBase and query it and retrieves it at runtime.
 * links user token to UserID.
 * UserID is gmail if Google Oauth is used. 
 * Else it can Admin created userID (To-DO)
 * 
 */
public class Authorization_MetaData {

	  static TableName table_meta = TableName.valueOf("metadata");
	  static String family_ds = "ds";
	  static String family_user = "owner";
	  static String family_policy = "policy";//this family will be storing like: userid and policy with it,
	  //Since userid is changing and growing or shrinking, we don't have a string array below.
	  String [] owner_qualifier= {"ID","token"};
	
	
	  Authorization_MetaData meta=null;
	  Connection connection=null;
	  Table table=null;
	
	  void create_connection()
	{
	 try{
		 
		 //System.out.println("Create_connection called");
		 
			meta = new Authorization_MetaData();
			Configuration config = HBaseConfiguration.create();
			//String path = meta.getClass().getResource("resources/hbase-site.xml").getPath();
			//config.addResource(new Path(path));
			config.addResource(new Path("/home/sandeep/MetroInsight/Codes/Citadel-Sandeep/citadel/src/main/resources/hbase-site.xml"));
			HBaseAdmin.checkHBaseAvailable(config);
			connection = ConnectionFactory.createConnection(config);
			
		//	System.out.println("Create_connection done");
			
		}//end try
		catch(Exception e)
		{	
			e.printStackTrace();
		}//end catch
		
	}//end create connection
	
	 void create_table()
	{
		try{
		Admin admin = connection.getAdmin();
		HTableDescriptor desc = new HTableDescriptor(table_meta);
		desc.addFamily(new HColumnDescriptor(family_ds));
		desc.addFamily(new HColumnDescriptor(family_user));
		desc.addFamily(new HColumnDescriptor(family_policy));
		if(!admin.tableExists(table_meta))
			{ 
			admin.createTable(desc);
			System.out.println("Table created");
			}
		else
			{
				System.out.println("Table exists");
			}
		
		table = connection.getTable(table_meta);
		
		}//end try
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}//end create_table
	
	
	String get_token(String userID)
	{
		String token="";
		try 
		{
		 // Instantiating the Scan class
	     Scan scan = new Scan();
	  // Scanning the required columns
	     scan.addColumn(Bytes.toBytes(family_user), Bytes.toBytes(owner_qualifier[0]));
	      
	  // Getting the scan result
	      ResultScanner scanner = table.getScanner(scan);
	      
	   // Reading values from scan result
	      for (Result result = scanner.next(); result != null; result = scanner.next())

	      System.out.println("Found row : " + result);
	      //closing the scanner
	      scanner.close();
	      
	      
		//token = UUID.randomUUID().toString();//"115239272283116371008";
		}//end try
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		return token;
		
	}//end get_token()
	
	
	
	 String insert_token(String userID)//given a userID inserts token into the database
	{ //should check before by other fxns to see id userID already doesn't exist in DB,
	  //else it will duplicate the ID
		 String token="";
	   try 
	   {
		   String rowid=userID;//UUID.randomUUID().toString();
		   byte[] row_id = Bytes.toBytes(rowid);
		   Put p = new Put(row_id);
		   p.addColumn(family_user.getBytes(), Bytes.toBytes(owner_qualifier[0]), Bytes.toBytes(userID));
		    token=UUID.randomUUID().toString();//randomly generate a token for user
		   p.addColumn(family_user.getBytes(), Bytes.toBytes(owner_qualifier[1]), Bytes.toBytes(token));
		   table.put(p);
	   }
	   catch(Exception e)
	   {
		   e.printStackTrace();
	   }
	   
	   return token;
	}//end insert_token
	
	public static void main(String[] args) {
		
		/*
		 * creates the tables, should be run first.
		 */
		
		
		Authorization_MetaData met = new Authorization_MetaData();
		met.create_connection();
		met.create_table();
		//String token=met.insert_token("sand.iitr@gmail.com");
		//System.out.println("Token is:"+token);
		met.get_token("sand.iitr@gmail.com");
		
		
	}//end main
	
}//end class Authorization_MetaData
