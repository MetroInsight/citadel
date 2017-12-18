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

public class GeomesaService_direct_Call implements DataService {
 
	static GeomesaHbase_old1 gmh;
  
  public GeomesaService_direct_Call() {
	  //initialize the geomesa database
    if(gmh==null) {
 	   gmh = new GeomesaHbase_old1();
 	   gmh.geomesa_initialize();
    }
  }

  static void initialize() {
	  if(gmh==null){
	   gmh = new GeomesaHbase_old1();
	   gmh.geomesa_initialize();
	 }
	}
  
  @Override
  public void insertData(String uuid, JsonArray data, Handler<AsyncResult<Boolean>> resultHandler) {
	  //string uuid is dummy here, and not used 
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
         
			//System.out.println("Result in queryData GeomesaService size is: "+resultJson.size());
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
	  GeomesaService_direct_Call GS=new GeomesaService_direct_Call();
	  
	  
	  //inserting the data points
	  int count =0*729000;
	  String uuid="uuid1";
	  double value_min=10.0;
	  double value_max=20.0;
	  double lat_min=30.0;
	  double lng_min=60.0;
	  double diff_loc=2.0;
	  String geometryType = "point";
	  Random random=new Random();
	  DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
	  Long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
	  
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
			GS.insertData("xxxx",data, ar -> {//"xxxx" is dummy string
			   	if (ar.failed()) {
			         	System.out.println(ar.cause().getMessage());
			       	} else {
			       		System.out.println("Insertion done in Main");
			       	}
			       	});
			 
		 }//end for
		 
		 for(int k=0;k<100;k++)
		 {
		 //query the points just inserted:
			 
		 double lat_minq=31.0;
		 double lat_maxq=31.5;
		 double lng_minq=60.0;
		 double lng_maxq=60.5;
		 
		 
		 long timestamp_min=1388534400000L,timestamp_max=1389312000000L;
		 String date1="2014-02-01T00:00:00.000Z";
		 String date2="2014-02-10T00:00:00.000Z";
		 
		 JsonObject query = new JsonObject();
		 query.put("lat_min", lat_minq);
		 query.put("lat_max", lat_maxq);
		 query.put("lng_min", lng_minq);
		 query.put("lng_max", lng_maxq);
		 query.put("timestamp_min", timestamp_min);
		 query.put("timestamp_max", timestamp_max);
		 System.out.println(k+" : Query is:"+query);
		 
		 long millistart;long milliend;
	     millistart = System.currentTimeMillis();
	    
	      //JsonArray result=GS.gmh.Query_Box_Lat_Lng_Time_Range( lat_minq,  lat_maxq,  lng_minq,  lng_maxq, timestamp_min , timestamp_max);
	     
	     //JsonArray result=GS.gmh.Query_Box_Lat_Lng_Time_Range2( lat_minq,  lat_maxq,  lng_minq,  lng_maxq, date1 , date2);
	     //System.out.println("Size of result set is:"+result.size());
	     
	     
	      GS.queryData(query, ar -> {
		    	if (ar.failed()) {
		          	System.out.println(ar.cause().getMessage());
		        	} else {
		        		
		        		String result2=ar.result().toString();
		        		//System.out.println("Query Results are:"+result2);
		        		//System.out.println("Result size is:"+result2.length());
		        		JsonArray datarec=new JsonArray(result2);
		    			System.out.println("Result Size is:"+datarec.size());
		    			
		        		//System.out.println("Query done in Main");
		        	}
		       });
	      
	      milliend = System.currentTimeMillis();
		// System.out.println("Size of result set is:"+result.size());
		// System.out.println(k+" : Query 2 done in Main");
	     System.out.println("Time taken is:"+(milliend-millistart));
	     System.out.println();
	     
		 }//end for loop on query
	  
  }

@Override
public void queryData(JsonObject query, String policy,
		Handler<AsyncResult<JsonArray>> resultHandler) {
	// TODO Auto-generated method stub
	
}

@Override
public void queryDataUUIDs(JsonObject query,
		Handler<AsyncResult<JsonArray>> resultHandler) {
	// TODO Auto-generated method stub
	
}



}
