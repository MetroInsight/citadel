package metroinsight.citadel.data.impl;

import java.util.Random;
import java.util.UUID;

import org.junit.experimental.theories.DataPoint;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import metroinsight.citadel.data.DataService;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;

public class GeomesaService implements DataService {
 
	static GeomesaHbase gmh;
  
  public GeomesaService() {
	  //initialize the geomesa database
    if(gmh==null) {
 	   gmh = new GeomesaHbase();
 	   gmh.geomesa_initialize();
    }
  }

  static void initialize() {
	  if(gmh==null){
	   gmh = new GeomesaHbase();
	   gmh.geomesa_initialize();
	 }
	}
  
  @Override
  public void insertPoint(JsonObject data, Handler<AsyncResult<Boolean>> resultHandler) {
	   // Validate if it complies to the schema. No actual usage
	    // TODO: Need to change this to proper validation instead.
	   // DataPoint dataPoint = data.mapTo(DataPoint.class); 
	    
	    gmh.geomesa_insertData(data, res -> {
		      if (res.succeeded()) {
			        // Load result to future if success.
			        System.out.println("Succeeded in InsertPoint GeomesaService");
			        resultHandler.handle(Future.succeededFuture(true));
			      } else {
			        // TODO: Need to add failure behavior.
			    	System.out.println("Failed in InsertPoint GeomesaService");
			    	resultHandler.handle(Future.succeededFuture(false));
			      }
	          });
	    
		
	} 
	  
  @Override
  public void queryPointBox(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  	
	  try{	  
	  String lat_min="";
	  if(query.containsKey("lat_min"))
		  lat_min=query.getString("lat_min");
	  String lat_max=query.getString("lat_max");
	  String lng_min=query.getString("lng_min");
	  String lng_max=query.getString("lng_max");
	  gmh.Query_Box_Lat_Lng(lat_min, lat_max, lng_min, lng_max, res -> {
	    	if (res.succeeded()) {
	    		JsonArray resultJson = res.result();
	    		resultHandler.handle(Future.succeededFuture(resultJson));
	      } else {
	      	res.cause().printStackTrace();
	      }
	    });
	  }
	  catch(Exception e)  {
		  e.printStackTrace();
	  }
	  
  }
  
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  
	try{
  	String lat_min="";
  	String lat_max="";
  	String lng_min="";
  	String lng_max="";
  	String unixTimeStamp_min="";
  	String unixTimeStamp_max="";
  	
  	if(query.containsKey("lat_min"))
  		lat_min=query.getString("lat_min");
  	if(query.containsKey("lat_max"))
  		lat_max=query.getString("lat_max");
  	if(query.containsKey("lng_min"))
  		lng_min=query.getString("lng_min");
  	if(query.containsKey("lng_max"))
  		lng_max=query.getString("lng_max");
  	if(query.containsKey("unixTimeStamp_min"))
  		unixTimeStamp_min=query.getString("unixTimeStamp_min");
  	if(query.containsKey("unixTimeStamp_max"))
  		unixTimeStamp_max=query.getString("unixTimeStamp_max");
  	
  	//String boxAndRangeQuery=lat_min+lat_max+lng_min+lng_max+unixTimeStamp_min+unixTimeStamp_max;
  	if(lat_min.length()>0&&lat_max.length()>0&&lng_min.length()>0&&lng_max.length()>0&&unixTimeStamp_min.length()>0&&unixTimeStamp_max.length()>0){
  		//query is box and range both, other cases need to be implemented too
  	  gmh.Query_Box_Lat_Lng_Time_Range(lat_min, lat_max, lng_min, lng_max, unixTimeStamp_min, unixTimeStamp_max, res -> {
	    	if (res.succeeded()) {
	    		JsonArray resultJson = res.result();
	    		resultHandler.handle(Future.succeededFuture(resultJson));
	      } else {
	      	res.cause().printStackTrace();
	      }
	    });
  	  
  	}//end if
  	
  	
	}//end try
	catch(Exception e){
		e.printStackTrace();
	}
  }//end queryPoint
  
  public static void main(String[] args) {	
	  
	  //testing the fucntionality of Geomesa service:
	  GeomesaService GS=new GeomesaService();
	  
	  //inserting the data points
		 int count =0;
		 String srcid="axd";
		 double value_min=10.0;
		 double value_max=20.0;
		 double lat_min=30.0;
		 double lng_min=60.0;
		 double diff_loc=5.0;
		 Random random=new Random(5771);
		 
		 for(int i=0;i<count;i++){
			 /*
			  *   private String srcid;//unique srcid for the stream belonging to same dataset
				  private String unixTimeStamp;//unix timestamp in milliseconds stored in string format
				  private String lat;//latitude
				  private String lng;//longitude
				  private String value;//value of this data point
			  */
			double value = value_min+random.nextDouble()*(value_max-value_min);
			long millis = System.currentTimeMillis();
				
			long unixTimeStamp = millis;//note time is used in millisec in the System
			double lat=lat_min+random.nextDouble()*diff_loc;
			double lng=lng_min+random.nextDouble()*diff_loc;	
			
			 JsonObject data = new JsonObject();	
			 data.put("srcid", srcid);
			 data.put("unixTimeStamp", Long.toString(unixTimeStamp));
			 data.put("lat", Double.toString(lat));
			 data.put("lng", Double.toString(lng));
			 data.put("value", Double.toString(value));
			 
			 System.out.println(i+ ": Data Point to Insert is:" + data.toString());
			 GS.insertPoint(data, ar -> {
			    	if (ar.failed()) {
			          	System.out.println(ar.cause().getMessage());
			        	} else {
			        		System.out.println("Insertion done in Main");
			        	}
			        	});
			 
		 }//end for
		 
		 
		 //query the points just inserted:
		 String lat_minq="30",lat_maxq="30.1",lng_minq="60",lng_maxq="60.1";
		 String unixTimeStamp_min="1499813708623",unixTimeStamp_max="1499813709645";
		 
		 
		 JsonObject query = new JsonObject();
		 query.put("lat_min", lat_minq);
		 query.put("lat_max", lat_maxq);
		 query.put("lng_min", lng_minq);
		 query.put("lng_max", lng_maxq);
		 query.put("unixTimeStamp_min", unixTimeStamp_min);
		 query.put("unixTimeStamp_max", unixTimeStamp_max);
		 
		 GS.queryPointBox(query, ar -> {
		    	if (ar.failed()) {
		          	System.out.println(ar.cause().getMessage());
		        	} else {
		        		
		        		String result=ar.result().toString();
		        		System.out.println("Query Results are:"+result);
		        		System.out.println("Query done in Main");
		        	}
		       });
		 
		 GS.queryPoint(query, ar -> {
		    	if (ar.failed()) {
		          	System.out.println(ar.cause().getMessage());
		        	} else {
		        		
		        		String result=ar.result().toString();
		        		System.out.println("Query 2 Results are:"+result);
		        		System.out.println("Query 2 done in Main");
		        	}
		       });
		 
	  
  }



}
