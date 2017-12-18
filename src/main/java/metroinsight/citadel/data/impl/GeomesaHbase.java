package metroinsight.citadel.data.impl;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.model.Datapoint;

public class GeomesaHbase {
	public DataStore dataStore=null;
	static String simpleFeatureTypeName = "MetroInsight";//"QuickStart";//
	static SimpleFeatureBuilder featureBuilder=null;
	
	
	public void geomesa_initialize() {
		
		try {
			if (dataStore == null) {
				Map<String, Serializable> parameters = new HashMap<>();
				parameters.put("bigtable.table.name", "Geomesa");
				
				//DataStoreFinder is from Geotools, returns an indexed datastore if one is available.
				dataStore = DataStoreFinder.getDataStore(parameters);
				
				// establish specifics concerning the SimpleFeatureType to store
				SimpleFeatureType simpleFeatureType = createSimpleFeatureType();

				// write Feature-specific metadata to the destination table in HBase
				// (first creating the table if it does not already exist); you only
				// need
				// to create the FeatureType schema the *first* time you write any
				// Features
				// of this type to the table
				//System.out.println("Creating feature-type (schema):  " + simpleFeatureTypeName);
				dataStore.createSchema(simpleFeatureType);
				
				
			}//end if

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	static SimpleFeatureType createSimpleFeatureType() throws SchemaException {
		
		/*
		 * We use the DataUtilities class from Geotools to create a FeatureType that will describe the data
		 * 
		 */
		
		SimpleFeatureType simpleFeatureType = DataUtilities.createType(simpleFeatureTypeName,
				"point_loc:Point:srid=4326,"+// a Geometry attribute: Point type
				"uuid:String,"+// a String attribute
				"value:String,"+// a String attribute
				"date:Date"// a date attribute for time
				);
		
		return simpleFeatureType;
		
		
	}

	
	static FeatureCollection createNewFeatures_old(SimpleFeatureType simpleFeatureType, JsonArray data) {
		
		DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
		
		if(featureBuilder==null)
		  featureBuilder = new SimpleFeatureBuilder(simpleFeatureType);
		
		SimpleFeature simpleFeature=featureBuilder.buildFeature(null);
		
		try {
		  /*
			String uuid = data.getString("uuid");
			String timestamp = data.getString("timestamp");//timestamp is in milliseconds
			String value = data.getString("value");
			Date date= new Date(Long.parseLong(timestamp));
		  JsonObject geometryJson = data.getJsonObject("geometry");
		  String geometryType = geometryJson.getString("type").toLowerCase();
		  */
		  for (int i = 0; i < data.size(); i++) {
		    JsonObject datum = data.getJsonObject(i);
		    Datapoint dp = datum.mapTo(Datapoint.class); 
		    String geometryType = dp.getGeometryType();
		    List<List<Double>> coordinates = dp.getCoordinates();
		    if (geometryType.equals("point")) {
		      Double lng = coordinates.get(0).get(0);
		      Double lat = coordinates.get(0).get(1);
		      Geometry geometry = WKTUtils$.MODULE$.read("POINT(" + lat.toString() + " " + lng.toString() + ")");
		      simpleFeature.setAttribute("point_loc", geometry);
		      }
		    else {
		      throw new java.lang.RuntimeException("Only Point is supported for geometry type.");
		      }
        simpleFeature.setAttribute("uuid", dp.getUuid());
        simpleFeature.setAttribute("value", dp.getValue());
        simpleFeature.setAttribute("date", new Date(dp.getTimestamp()));
        featureCollection.add(simpleFeature);
		  }

			// accumulate this new feature in the collection
		} catch (Exception e) {
			e.printStackTrace();
		}

		return featureCollection;
	}
	
static FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, String uuid, JsonArray data) {
		
		DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
		
		
		if(featureBuilder==null)
		  featureBuilder = new SimpleFeatureBuilder(simpleFeatureType);
		
		SimpleFeature simpleFeature=featureBuilder.buildFeature(null);
		
		try {
		  /*
			String uuid = data.getString("uuid");
			String timestamp = data.getString("timestamp");//timestamp is in milliseconds
			String value = data.getString("value");
			Date date= new Date(Long.parseLong(timestamp));
		  JsonObject geometryJson = data.getJsonObject("geometry");
		  String geometryType = geometryJson.getString("type").toLowerCase();
		  */
		  for (int i = 0; i < data.size(); i++) {
		    JsonObject datum = data.getJsonObject(i);
		    Datapoint dp = datum.mapTo(Datapoint.class); 
		    String geometryType = dp.getGeometryType();
		    List<List<Double>> coordinates = dp.getCoordinates();
		    if (geometryType.equals("point")) {
		      Double lng = coordinates.get(0).get(0);
		      Double lat = coordinates.get(0).get(1);
		      Geometry geometry = WKTUtils$.MODULE$.read("POINT(" + lng.toString() + " " + lat.toString() + ")");
		      simpleFeature.setAttribute("point_loc", geometry);
		      }
		    else {
		      throw new java.lang.RuntimeException("Only Point is supported for geometry type.");
		      }
        //simpleFeature.setAttribute("uuid", dp.getUuid());//made uuid change to the static
		    simpleFeature.setAttribute("uuid", uuid);
        simpleFeature.setAttribute("value", dp.getValue());
        simpleFeature.setAttribute("date", new Date(dp.getTimestamp()));
        featureCollection.add(simpleFeature);
		  }

			// accumulate this new feature in the collection
		} catch (Exception e) {
			e.printStackTrace();
		}

		return featureCollection;
	}
	
	static void insertFeatures(DataStore dataStore, FeatureCollection featureCollection)
			throws IOException {

		FeatureStore featureStore = (FeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);
		featureStore.addFeatures(featureCollection);
	}
	
	public void geomesa_insertData(String uuid, JsonArray data) {
		
		//System.out.println("Inserting Data in geomesa_insertData(JsonObject data) in GeomesaHbase");
		
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}

			// establish specifics concerning the SimpleFeatureType to store
			SimpleFeatureType simpleFeatureType = createSimpleFeatureType();

			// create new features locally, and add them to this table
			//System.out.println("Creating new features");
			FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, uuid, data);
			//System.out.println("Inserting new features");
			insertFeatures(dataStore, featureCollection);
			//System.out.println("done inserting Data");

			/*
			 * //querying Data now, results as shown below:
			 * System.out.println("querying Data now, results as shown below:");
			 * Query();
			 */
			//System.out.println("Done");

		} // end try
		catch (Exception e) {

			e.printStackTrace();
		}

	}// end function

	static JsonArray queryFeatures_Box_Lat_Lng(DataStore dataStore, String geomField, double x0,
			double y0, double x1, double y1) throws CQLException, IOException {

		// construct a (E)CQL filter from the search parameters,
		// and use that as the basis for the query
		String cqlGeometry = "BBOX(" + geomField + ", " + x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";
		Filter cqlFilter = CQL.toFilter(cqlGeometry);
		
		Query query = new Query(simpleFeatureTypeName, cqlFilter);

		// submit the query, and get back an iterator over matching features
		FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
		FeatureIterator featureItr = featureSource.getFeatures(query).features();
        
		System.out.println("Query is:"+query);

		JsonArray ja = new JsonArray();

		// loop through all results
		int n = 0;
		while (featureItr.hasNext()) {
			Feature feature = featureItr.next();

			
			try{
			JsonObject Data = new JsonObject();
			Data.put("uuid", feature.getProperty("uuid").getValue());
			Date date=(Date) feature.getProperty("date").getValue();
			Data.put("timestamp", date.getTime());
			Point point =(Point) feature.getProperty("point_loc").getValue();
			Coordinate cd=point.getCoordinates()[0];//since it a single point
			Data.put("lat", cd.x);
			Data.put("lng", cd.y);
			Data.put("value", feature.getProperty("value").getValue());
			ja.add(Data);	
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
		}
		featureItr.close();

		return ja;
	}//end function
	
	private JsonArray queryFeatures_Box_Lat_Lng_Time_Range(String uuid, DataStore dataStore2, String geomField, String dateField, Double lat_min,
			Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max) throws Exception {
		JsonArray ja = new JsonArray();

		
		try{
		// construct a (E)CQL filter from the search parameters,
		// and use that as the basis for the query
		String cqlGeometry = "BBOX(" + geomField + ", " + lat_min + ", " + lng_min + ", " + lat_max + ", " + lng_max + ")";
		Date datemin=new Date(Long.valueOf(timestamp_min));
		Date datemax=new Date(Long.valueOf(timestamp_max));
		
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		//format.setTimeZone(TimeZone.getTimeZone("PDT"));
		String date1=format.format(datemin);
		String date2=format.format(datemax);
		//Date date3=format.parse(date1);
		//System.out.println("Date range is:"+date1.toString()+" : "+date2.toString());
		
		String cqlDates = "(" + dateField + " during " + date1+"/" + date2+")";
		String filter=cqlGeometry+" AND "+cqlDates;
		
		if(!uuid.equals(""))
		{
			String uuid_filter=" "+"uuid ='" + uuid +"'"+" ";
			filter=filter + " AND "+ uuid_filter;
			
		}//adding constrains on UUID
		
		Filter cqlFilter = CQL.toFilter(filter);
		
		System.out.println("Filter is:"+filter);
		
		Query query = new Query(simpleFeatureTypeName, cqlFilter);
		
		/*This line force the geomesa to evaluate the bounding box very accurately*/
		query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);
		
		//System.out.println("Query in queryFeatures_Box_Lat_Lng_Time_Range is:"+query.toString());
		
		// submit the query, and get back an iterator over matching features
		FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
		FeatureIterator featureItr = featureSource.getFeatures(query).features();
		
		// loop through all results
		int n = 0;
		while (featureItr.hasNext()) {
			Feature feature = featureItr.next();
			//System.out.println("Next:"+n++);
			try{
			JsonObject Data = new JsonObject();
			Data.put("uuid", feature.getProperty("uuid").getValue());
			Date date=(Date) feature.getProperty("date").getValue();
			Data.put("timestamp", date.getTime());
			Point point =(Point) feature.getProperty("point_loc").getValue();
			Coordinate cd=point.getCoordinates()[0];//since it a single point
			Data.put("lat", cd.x);
			Data.put("lng", cd.y);
			Data.put("value", feature.getProperty("value").getValue());
			ja.add(Data);	
			}
			catch(Exception e){
				e.printStackTrace();
				
			}
			
		}
		featureItr.close();
		//System.out.println("Next:"+n++);
		
		
		}//end try
		catch(Exception e){
			System.out.println("Exception Encountered--in queryFeatures_Box_Lat_Lng_Time_Range(String uuid, DataStore dataStore2, String geomField, String dateField, Double lat_min, Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max) ");
			e.printStackTrace();
			throw e;
		}//end catch
		return ja;
	}//end function
	
	
	private JsonArray queryFeatures_Box_Lat_Lng_Time_Range2(DataStore dataStore2, String geomField, String dateField, Double lat_min,
			Double lng_min, Double lat_max, Double lng_max, String date1, String date2) {
		JsonArray ja = new JsonArray();

		
		try{
		// construct a (E)CQL filter from the search parameters,
		// and use that as the basis for the query
		String cqlGeometry = "BBOX(" + geomField + ", " + lat_min + ", " + lng_min + ", " + lat_max + ", " + lng_max + ")";
		
		
		String cqlDates = "(" + dateField + " during " + date1+"/" + date2+")";
		String filter=cqlGeometry+" AND "+cqlDates;
		
		Filter cqlFilter = CQL.toFilter(filter);
		
		
		Query query = new Query(simpleFeatureTypeName, cqlFilter);
		
		/*This line force the geomesa to evaluate the bounding box very accurately*/
		query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);
		
		//System.out.println("Query in queryFeatures_Box_Lat_Lng_Time_Range is:"+query.toString());
		
		// submit the query, and get back an iterator over matching features
		FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
		FeatureIterator featureItr = featureSource.getFeatures(query).features();
		
		// loop through all results
		int n = 0;
		while (featureItr.hasNext()) {
			Feature feature = featureItr.next();
			//System.out.println("Next:"+n++);
			try{
			JsonObject Data = new JsonObject();
			Data.put("uuid", feature.getProperty("uuid").getValue());
			Date date=(Date) feature.getProperty("date").getValue();
			Data.put("timestamp", date.getTime());
			Point point =(Point) feature.getProperty("point_loc").getValue();
			Coordinate cd=point.getCoordinates()[0];//since it a single point
			Data.put("lat", cd.x);
			Data.put("lng", cd.y);
			Data.put("value", feature.getProperty("value").getValue());
			ja.add(Data);	
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
		}
		featureItr.close();
		//System.out.println("Next:"+n++);
		
		
		}//end try
		catch(Exception e){
			e.printStackTrace();
		}//end catch
		return ja;
	}//end function
	
	
	public JsonArray Query_Box_Lat_Lng(double lat_min, double lat_max, double lng_min, double lng_max) {
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}
		

			// query a few Features from this table
			System.out.println("Submitting query in Query_Box_Lat_Lng GeomesaHbase ");
			JsonArray result = queryFeatures_Box_Lat_Lng(dataStore, "point_loc", lat_min, lng_min, lat_max, lng_max);

			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}//end function

	JsonArray Query_Box_Lat_Lng_Time_Range(String uuid, Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			long timestamp_min, long timestamp_max) throws Exception {
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}
		

			// query a few Features from this table
			//System.out.println("Submitting query in Query_Box_Lat_Lng_Time_Range GeomesaHbase ");
			//the point_loc and date should be part of the config
			JsonArray result = queryFeatures_Box_Lat_Lng_Time_Range(uuid, dataStore, "point_loc","date", lat_min, lng_min, lat_max, lng_max,timestamp_min,timestamp_max);

			return result;
		} catch (Exception e) {
			System.out.println("Exception Encountered--Query_Box_Lat_Lng_Time_Range(String uuid, Double lat_min, Double lat_max, Double lng_min, Double lng_max, long timestamp_min, long timestamp_max)");
			e.printStackTrace();
			throw e;
		}
	}//end function
	
	JsonArray Query_Box_Lat_Lng_Time_Range2(Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			String timestamp_min, String timestamp_max) {
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}
		

			// query a few Features from this table
			//System.out.println("Submitting query in Query_Box_Lat_Lng_Time_Range GeomesaHbase ");
			//the point_loc and date should be part of the config
			JsonArray result = queryFeatures_Box_Lat_Lng_Time_Range2(dataStore, "point_loc","date", lat_min, lng_min, lat_max, lng_max,timestamp_min,timestamp_max);

			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}//end function


	public void geomesa_insertData(String uuid, JsonArray data, Handler<AsyncResult<Boolean>> resultHandler) {
		
		try{
		    geomesa_insertData(uuid, data);
		    resultHandler.handle(Future.succeededFuture(true));
		}
		catch(Exception e){
			resultHandler.handle(Future.succeededFuture(false));
			e.printStackTrace();
		}
		
	}//end function

	public void Query_Box_Lat_Lng(double lat_min, double lat_max, double lng_min, double lng_max,Handler<AsyncResult<JsonArray>> resultHandler) {
		JsonArray result=new JsonArray();
		try{
			result = Query_Box_Lat_Lng( lat_min,  lat_max,  lng_min,  lng_max);
			resultHandler.handle(Future.succeededFuture(result));
		}
		catch(Exception e){
			resultHandler.handle(Future.succeededFuture(result));//in this case the result is empty jsonarray
			e.printStackTrace();
		}
		
	}//end function

	public void Query_Box_Lat_Lng_Time_Range(String uuid, Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			long timestamp_min, long timestamp_max, Handler<AsyncResult<JsonArray>> resultHandler) {
		JsonArray result=new JsonArray();
		try{
			result=Query_Box_Lat_Lng_Time_Range( uuid, lat_min,  lat_max,  lng_min,  lng_max, timestamp_min , timestamp_max);
			resultHandler.handle(Future.succeededFuture(result));
		}
		catch(Exception e){
			//resultHandler.handle(Future.succeededFuture(result));//in this case the result is empty jsonarray
			resultHandler.handle(Future.failedFuture(e));// in this case the result is empty jsonarray
			System.out.println("Exception Encountered--Query_Box_Lat_Lng_Time_Range(String uuid, Double lat_min, Double lat_max, Double lng_min, Double lng_max, long timestamp_min, long timestamp_max, Handler<AsyncResult<JsonArray>> resultHandler)");
			e.printStackTrace();
		}
		
	}

	//also have policy as input
	public void Query_Box_Lat_Lng_Time_Range(String uuid, String policy,
			Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			long timestamp_min, long timestamp_max,  Handler<AsyncResult<JsonArray>> resultHandler) {
		
		
		JsonArray result=new JsonArray();
		try{
			result=Query_Box_Lat_Lng_Time_Range( uuid, policy, lat_min,  lat_max,  lng_min,  lng_max, timestamp_min , timestamp_max);
			resultHandler.handle(Future.succeededFuture(result));
		}
		catch(Exception e){
			//resultHandler.handle(Future.succeededFuture(result));//in this case the result is empty jsonarray
			resultHandler.handle(Future.failedFuture(e));// in this case the result is empty jsonarray
			System.out.println("Exception Encountered--Query_Box_Lat_Lng_Time_Range(String uuid,String policy, Double lat_min, Double lat_max, Double lng_min, Double lng_max, long timestamp_min, long timestamp_max, Handler<AsyncResult<JsonArray>> resultHandler)");
			e.printStackTrace();
		}
		
	}//end public void Query_Box_Lat_Lng_Time_Range(String uuid, String policy,

	
	//has policy in it
	JsonArray Query_Box_Lat_Lng_Time_Range(String uuid, String policy, Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			long timestamp_min, long timestamp_max) throws Exception {
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}
		

			// query a few Features from this table
			//System.out.println("Submitting query in Query_Box_Lat_Lng_Time_Range GeomesaHbase ");
			//the point_loc and date should be part of the config
			JsonArray result = queryFeatures_Box_Lat_Lng_Time_Range(uuid,policy, dataStore, "point_loc","date", lat_min, lng_min, lat_max, lng_max,timestamp_min,timestamp_max);

			return result;
		} catch (Exception e) {
			System.out.println("Exception Encountered--Query_Box_Lat_Lng_Time_Range(String uuid, Double lat_min, Double lat_max, Double lng_min, Double lng_max, long timestamp_min, long timestamp_max)");
			e.printStackTrace();
			throw e;
		}
	}//end function

	
	private JsonArray queryFeatures_Box_Lat_Lng_Time_Range(String uuid, String policy, DataStore dataStore2, String geomField, String dateField, Double lat_min,
			Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max) throws Exception {
		JsonArray ja = new JsonArray();

		
		try{
			
			/*
			 * Policy Operation begin
			 */
			
			/*
			 * Allowed Space begins
			 */
			String policy_allow_space="";
			
			JsonObject where=new JsonObject();
			JsonObject Policy=new JsonObject(policy);
			if(Policy.containsKey("where"))
			{
				 where = Policy.getJsonObject("where");
				if(where.containsKey("allowedPolygons"))
				{
				
					JsonArray allowedPolygons = where.getJsonArray("allowedPolygons");
					for(int i=0;i<allowedPolygons.size();i++)
					  {
						policy_allow_space = policy_allow_space + " INTERSECTS ( "+geomField+" , POLYGON ((";
						
						//we already verified during insertion that this polygon is correct and allowed
						JsonArray polygon=allowedPolygons.getJsonArray(i); 
						
						for(int j=0;j<polygon.size();j++)
						  {
							  JsonObject pos=polygon.getJsonObject(j);
							  double lat=pos.getDouble("lat");
							  double lng=pos.getDouble("lng");  
							  policy_allow_space=policy_allow_space+lat+" "+lng;
							  
							  if(j<polygon.size()-1)
								  policy_allow_space=policy_allow_space+",";
							  
						  }//end for(int j=0;j<polygon.size();j++)
						
						 policy_allow_space=policy_allow_space+")))";
						
						 //multiple allowed polygons are OR with each other
						 if(i<allowedPolygons.size()-1)
							  policy_allow_space=policy_allow_space + " OR ";
						 
					  }//end for(int i=0;i<allowedPolygons.size();i++)
					
					System.out.println("\n \n Allow Space Policy Translation:"+policy_allow_space);
					
				}//end if(where.containsKey("allowedPolygons"))
				
			}//end if(Policy.containsKey("where"))
			/*
			 * Allowed space ends
			 */
		
			
			/*
			 * deny Space begins
			 */
			String policy_deny_space="";
			
			
			if(Policy.containsKey("where"))
			{
				//JsonObject where = Policy.getJsonObject("where");//using previous where
				if(where.containsKey("denyPolygons"))
				{
				
					JsonArray denyPolygons = where.getJsonArray("denyPolygons");
					for(int i=0;i<denyPolygons.size();i++)
					  {
						policy_deny_space = policy_deny_space + " NOT INTERSECTS ( "+geomField+" , POLYGON ((";
						
						//we already verified during insertion that this polygon is correct and allowed
						JsonArray polygon=denyPolygons.getJsonArray(i); 
						
						for(int j=0;j<polygon.size();j++)
						  {
							  JsonObject pos=polygon.getJsonObject(j);
							  double lat=pos.getDouble("lat");
							  double lng=pos.getDouble("lng");  
							  policy_deny_space=policy_deny_space+lat+" "+lng;
							  
							  if(j<polygon.size()-1)
								  policy_deny_space=policy_deny_space+",";
							  
						  }//end for(int j=0;j<polygon.size();j++)
						
						policy_deny_space=policy_deny_space+")))";
						
						 //multiple deny polygons are AND with each other
						 if(i<denyPolygons.size()-1)
							 policy_deny_space=policy_deny_space + " AND ";
						 
					  }//end for(int i=0;i<allowedPolygons.size();i++)
					
					System.out.println("\n \n Deny Space Policy Translation:"+policy_deny_space);
					
				}//end if(where.containsKey("allowedPolygons"))
				
			}//end if(Policy.containsKey("where"))
			/*
			 * deny space ends
			 */
		
			
			/*
			 * Policy operations end
			 */
		// construct a (E)CQL filter from the search parameters,
		// and use that as the basis for the query
		String cqlGeometry = "BBOX(" + geomField + ", " + lat_min + ", " + lng_min + ", " + lat_max + ", " + lng_max + ")";
		Date datemin=new Date(Long.valueOf(timestamp_min));
		Date datemax=new Date(Long.valueOf(timestamp_max));
		
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		//format.setTimeZone(TimeZone.getTimeZone("PDT"));
		String date1=format.format(datemin);
		String date2=format.format(datemax);
		//Date date3=format.parse(date1);
		//System.out.println("Date range is:"+date1.toString()+" : "+date2.toString());
		
		
		String filter=cqlGeometry;
		
		if(policy_allow_space.length()>0)
		{
			filter = filter+ " AND "+ " ( "+policy_allow_space+" )";
			
		}//end if(policy_allow_space.length()>0)
		
		if(policy_deny_space.length()>0)
		{
			filter =" ( "+ filter+ " AND "+ " ( "+policy_deny_space+" )"+" )";
		}
			
		if(!uuid.equals(""))
		{
			String uuid_filter=" "+"uuid ='" + uuid +"'"+" ";
			filter=filter + " AND "+" ( "+ uuid_filter+" )";
			
		}//adding constrains on UUID
		
		
		String cqlDates = "(" + dateField + " during " + date1+"/" + date2+")";
		filter=filter + " AND "+cqlDates;
		
		System.out.println("Filter is: \n"+filter);
		
		
		Filter cqlFilter = CQL.toFilter(filter);
		
		
		
		Query query = new Query(simpleFeatureTypeName, cqlFilter);
		
		/*This line force the geomesa to evaluate the bounding box very accurately*/
		query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);
		
		//System.out.println("Query in queryFeatures_Box_Lat_Lng_Time_Range is:"+query.toString());
		
		// submit the query, and get back an iterator over matching features
		FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
		FeatureIterator featureItr = featureSource.getFeatures(query).features();
		
		// loop through all results
		int n = 0;
		while (featureItr.hasNext()) {
			Feature feature = featureItr.next();
			//System.out.println("Next:"+n++);
			try{
			JsonObject Data = new JsonObject();
			Data.put("uuid", feature.getProperty("uuid").getValue());
			Date date=(Date) feature.getProperty("date").getValue();
			Data.put("timestamp", date.getTime());
			Point point =(Point) feature.getProperty("point_loc").getValue();
			Coordinate cd=point.getCoordinates()[0];//since it a single point
			Data.put("lat", cd.x);
			Data.put("lng", cd.y);
			Data.put("value", feature.getProperty("value").getValue());
			ja.add(Data);	
			}
			catch(Exception e){
				e.printStackTrace();
				
			}
			
		}
		featureItr.close();
		//System.out.println("Next:"+n++);
		
		
		}//end try
		catch(Exception e){
			System.out.println("Exception Encountered--in queryFeatures_Box_Lat_Lng_Time_Range(String uuid, DataStore dataStore2, String geomField, String dateField, Double lat_min, Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max) ");
			e.printStackTrace();
			throw e;
		}//end catch
		return ja;
	}//end function
	
	
	
	
}//end GeomesaHbase class
