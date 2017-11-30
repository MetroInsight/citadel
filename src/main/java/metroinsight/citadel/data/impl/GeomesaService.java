package metroinsight.citadel.data.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.data.DataService;

public class GeomesaService implements DataService {
 
	static GeomesaHbase gmh;
  
  public GeomesaService() {
	  //initialize the geomesa database
    if(gmh==null) 
    {
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
  public void insertData(JsonArray data, Handler<AsyncResult<Boolean>> resultHandler) {
	   // Validate if it complies to the schema. No actual usage
	    // TODO: Need to change this to proper validation instead.
	   // DataPoint dataPoint = data.mapTo(DataPoint.class); 
	    
	    gmh.geomesa_insertData(data, res -> {
		      if (res.succeeded()) {
			        // Load result to future if success.
			        //System.out.println("Succeeded in InsertPoint GeomesaService");
			        resultHandler.handle(Future.succeededFuture(true));
			      } else {
			        // TODO: Need to add failure behavior.
			    	//System.out.println("Failed in InsertPoint GeomesaService");
			    	resultHandler.handle(Future.succeededFuture(false));
			      }
	          });
	    
		
	} 
	  
  @Override
  public void queryDataBox(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  	
	  try{	  
      double lat_min;
      if(query.containsKey("lat_min"))
        lat_min=query.getDouble("lat_min");
      else
        lat_min = 0;
      double lat_max=query.getDouble("lat_max");
      double lng_min=query.getDouble("lng_min");
      double lng_max=query.getDouble("lng_max");
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
  public void queryData(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  
    try{
    	//System.out.println("in queryData GeomesaService query is:"+query);
      Double lat_max = query.getDouble("lat_max");
      Double lat_min = query.getDouble("lat_min");
      Double lng_min = query.getDouble("lng_min");
      Double lng_max = query.getDouble("lng_max");
      long timestamp_min = query.getLong("timestamp_min");
      long timestamp_max = query.getLong("timestamp_max");
      
      //String boxAndRangeQuery=lat_min+lat_max+lng_min+lng_max+timestamp_min+timestamp_max;
        //query is box and range both, other cases need to be implemented too
      gmh.Query_Box_Lat_Lng_Time_Range(lat_min, lat_max, lng_min, lng_max, timestamp_min, timestamp_max, res -> {
        if (res.succeeded()) {
          JsonArray resultJson = res.result();
         
		System.out.println("Result in queryData GeomesaService size is: "+resultJson.size());
          resultHandler.handle(Future.succeededFuture(resultJson));
          } else {
            res.cause().printStackTrace();
            }
        });
      }//end if
      catch(Exception e){
        e.printStackTrace();
      }
    }
  
  /*
   * Geomesa query performs well when the time range is restricted to the range
   * actually present in the data sets, else it is very badly affected with arbitrary time ranges
   * 
   */
  
  public static void main(String[] args) {	
	  
	  //testing the fucntionality of Geomesa service:
      //System.setProperty("hadoop.home.dir", "/home/sandeep/metroinsight/installations/hadoop/hadoop-2.8.0");
	  
      /*
	   String home = System.getenv("HADOOP_HOME");
	   System.out.println("Sandeep Home is:"+home);
	   home = System.getProperty("hadoop.home.dir");
	   System.out.println("Sandeep Home is:"+home);
	   */
	   
	  GeomesaService GS=new GeomesaService();
	  
	   
	  //inserting the data points
	  int count =0;//0*729000;
	  String uuid="uuid1";
	  double value_min=10.0;
	  double value_max=20.0;
	  double lat_min=30.0;
	  double lng_min=60.0;
	  double diff_loc=2.0;
	  String geometryType = "point";
	  Random random=new Random();
	  DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
	  Long SECONDS_PER_YEAR = 365L;//365L * 24L * 60L * 60L;
	  
	  for(int i=0;i<count;i++){		 
			double value = value_min+random.nextDouble()*(value_max-value_min);
			//long millis = System.currentTimeMillis();
			DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
			
			long timestamp = dateTime.getMillis();//1388534500000L;//millis;//note time is used in millisec in the System
			double lat=lat_min+random.nextDouble()*diff_loc;
			double lng=lng_min+random.nextDouble()*diff_loc;	
			JsonArray data = new JsonArray();
			JsonObject datum = new JsonObject();	
			datum.put("uuid", uuid);
			datum.put("timestamp", timestamp);
			datum.put("value", value);
			ArrayList<ArrayList<Double>> coordinates = new ArrayList<ArrayList<Double>>();
			ArrayList<Double> coordinate = new ArrayList<Double>();
			coordinate.add(lng);
			coordinate.add(lat);
			coordinates.add(coordinate);
			datum.put("geometryType", geometryType);
			datum.put("coordinates", coordinates);
			data.add(datum);
			
			System.out.println(i+ ": Data Point to Insert is:" + data.toString());
			GS.insertData(data, ar -> {
			   	if (ar.failed()) {
			         	System.out.println(ar.cause().getMessage());
			       	} else {
			       		System.out.println("Insertion done in Main");
			       	}
			       	});
			 
		 }//end for
		 
		 for(int k=0;k<2;k++)
		 {
		 //query the points just inserted:
			 
		 double lat_minq=31.0;
		 double lat_maxq=32.0;
		 double lng_minq=60.0;
		 double lng_maxq=62.0;
		 
		 /*	 
		 double lat_minq=lat_min+random.nextDouble()*diff_loc;
		 double lat_maxq=lat_minq+0.1;
		 double lng_minq=lng_min+random.nextDouble()*diff_loc;
		 double lng_maxq=lng_minq+0.1;
		 */
		 
		 //long timestamp_min=1388534400000L,timestamp_max=1389312000000L;//1504059232123L;//1389312000000L;
		                  //1388534400000 
		DateTime dateTime1 = MIN_DATE;//.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
		 DateTime dateTime2 = dateTime1.plusSeconds((int) Math.round( (SECONDS_PER_YEAR)));
		 long timestamp_min=dateTime1.getMillis();
		 long timestamp_max=dateTime2.getMillis();//1420070400000L;//
		 
		 JsonObject query = new JsonObject();
		 query.put("lat_min", lat_minq);
		 query.put("lat_max", lat_maxq);
		 query.put("lng_min", lng_minq);
		 query.put("lng_max", lng_maxq);
		 query.put("timestamp_min", timestamp_min);
		 query.put("timestamp_max", timestamp_max);
		 long millistart;long milliend;
		 
		// millistart = System.currentTimeMillis();
		 
		 /*
		 GS.queryDataBox(query, ar -> {
		    	if (ar.failed()) {
		          	System.out.println(ar.cause().getMessage());
		        	} else {
		        		
		        		String result=ar.result().toString();
		        		System.out.println("Query Results are:"+result);
		        		//System.out.println("Result size is:"+result.length());
		        		JsonArray datarec=new JsonArray(result);
		    			System.out.println(datarec.size());
		    			
		        		System.out.println("Query done in Main");
		        	}
		       });
		 
		 milliend = System.currentTimeMillis();
	     System.out.println("Time taken is:"+(milliend-millistart));
	     
	     */
		 System.out.println(k+" : Query is:"+query);
	     millistart = System.currentTimeMillis();
		 GS.queryData(query, ar -> {
		    	if (ar.failed()) {
		          	System.out.println(ar.cause().getMessage());
		        	} else {
		        		
		        		String result=ar.result().toString();
		        		
		    			
		        		//System.out.println("Query 2 Results are:"+result);
		        		JsonArray datarec=new JsonArray(result);
		    			System.out.println("Result size is: "+datarec.size());
		    			
		        		
		        	}
		       });
		 milliend = System.currentTimeMillis();
		 System.out.println(k+" : Query 2 done in Main");
		 
	     System.out.println("Time taken is:"+(milliend-millistart));
	     System.out.println();
	     
		 }//end for loop on query
	  
  }



}
