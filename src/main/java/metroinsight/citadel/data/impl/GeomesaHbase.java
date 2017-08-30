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
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
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

	
	static FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, JsonArray data) {
		
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
	
	static void insertFeatures(DataStore dataStore, FeatureCollection featureCollection)
			throws IOException {

		FeatureStore featureStore = (FeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);
		featureStore.addFeatures(featureCollection);
	}
	
	public void geomesa_insertData(JsonArray data) {
		
		//System.out.println("Inserting Data in geomesa_insertData(JsonObject data) in GeomesaHbase");
		
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}

			// establish specifics concerning the SimpleFeatureType to store
			SimpleFeatureType simpleFeatureType = createSimpleFeatureType();

			// create new features locally, and add them to this table
			//System.out.println("Creating new features");
			FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, data);
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
	
	private JsonArray queryFeatures_Box_Lat_Lng_Time_Range(DataStore dataStore2, String geomField, String dateField, Double lat_min,
			Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max) {
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
		System.out.println("Date range is:"+date1.toString()+" : "+date2.toString());
		
		String cqlDates = "(" + dateField + " during " + date1+"/" + date2+")";
		String filter=cqlGeometry+" AND "+cqlDates;
		
		Filter cqlFilter = CQL.toFilter(filter);
		
		
		Query query = new Query(simpleFeatureTypeName, cqlFilter);

		// submit the query, and get back an iterator over matching features
		FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
		FeatureIterator featureItr = featureSource.getFeatures(query).features();
		
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

	private JsonArray Query_Box_Lat_Lng_Time_Range(Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			long timestamp_min, long timestamp_max) {
		try {

			if (dataStore == null) {
				geomesa_initialize();
			}
		

			// query a few Features from this table
			//System.out.println("Submitting query in Query_Box_Lat_Lng_Time_Range GeomesaHbase ");
			//the point_loc and date should be part of the config
			JsonArray result = queryFeatures_Box_Lat_Lng_Time_Range(dataStore, "point_loc","date", lat_min, lng_min, lat_max, lng_max,timestamp_min,timestamp_max);

			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}//end function
	


	public void geomesa_insertData(JsonArray data, Handler<AsyncResult<Boolean>> resultHandler) {
		
		try{
		    geomesa_insertData(data);
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

	public void Query_Box_Lat_Lng_Time_Range(Double lat_min, Double lat_max, Double lng_min, Double lng_max,
			long timestamp_min, long timestamp_max, Handler<AsyncResult<JsonArray>> resultHandler) {
		JsonArray result=new JsonArray();
		try{
			result=Query_Box_Lat_Lng_Time_Range( lat_min,  lat_max,  lng_min,  lng_max, timestamp_min , timestamp_max);
			resultHandler.handle(Future.succeededFuture(result));
		}
		catch(Exception e){
			resultHandler.handle(Future.succeededFuture(result));//in this case the result is empty jsonarray
			e.printStackTrace();
		}
		
	}



	
	
	
}//end GeomesaHbase class
