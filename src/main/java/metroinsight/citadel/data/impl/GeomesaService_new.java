package metroinsight.citadel.data.impl;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Geometry;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.commons.cli.*;
import org.geotools.data.*;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GeomesaService_new {
   

	static GeomesaHbase gmh;
	
	public GeomesaService_new() {
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
	 
	
    // sub-set of parameters that are used to create the HBase DataStore
	static int count=0;
	static Random random;
  
    static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName)
            throws SchemaException {

        // list the attributes that constitute the feature type
    	 List<String> attributes = Lists.newArrayList(
                 "srcid:String",
                 "value:String",     // some types require full qualification (see DataUtilities docs)
                 "date:Date",               // a date-time field is optional, but can be indexed
                 "*point_loc:Point:srid=4326"  // the "*" denotes the default geometry (used for indexing)
                              // you may have as many other attributes as you like...
         );

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                DataUtilities.createType(simpleFeatureTypeName, simpleFeatureTypeSchema);

        return simpleFeatureType;
    }

    static FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, int numNewFeatures) {
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        String id;
        Object[] NO_VALUES = {};
        Long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
        
        DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
        Double MIN_X = 30.0;
        Double MIN_Y =  60.0;
        Double DX = 5.0;
        Double DY = 5.0;

        for (int i = 0; i < numNewFeatures; i ++) {
            // create the new (unique) identifier and empty feature shell
            id = "Observation." + Integer.toString(count);
            count++;
            SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, NO_VALUES, id);

            // be sure to tell GeoTools explicitly that you want to use the ID you provided
            simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

            // populate the new feature's attributes

            // Who: string value
            simpleFeature.setAttribute("srcid","sandy_123_456");

            // What: long value
            simpleFeature.setAttribute("value", "10");

            // Where: location: construct a random point within a 2-degree-per-side square
            double x = MIN_X + random.nextDouble() * DX;
            double y = MIN_Y + random.nextDouble() * DY;
            Geometry geometry = WKTUtils$.MODULE$.read("POINT(" + x + " " + y + ")");
            simpleFeature.setAttribute("point_loc", geometry);

            // When: date-time:  construct a random instant within a year
            DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
            simpleFeature.setAttribute("date", dateTime.toDate());

            // accumulate this new feature in the collection
            featureCollection.add(simpleFeature);
        }

        return featureCollection;
    }
   
    

    static void insertFeatures(String simpleFeatureTypeName,
                               DataStore dataStore,
                               FeatureCollection featureCollection)
            throws IOException {

        FeatureStore featureStore = (FeatureStore)dataStore.getFeatureSource(simpleFeatureTypeName);
        featureStore.addFeatures(featureCollection);
    }

    static Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
                               String dateField, String t0, String t1,
                               String attributesQuery)
            throws CQLException, IOException {

        // there are many different geometric predicates that might be used;
        // here, we just use a bounding-box (BBOX) predicate as an example.
        // this is useful for a rectangular query area
        String cqlGeometry = "BBOX(" + geomField + ", " +
                x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";

        // there are also quite a few temporal predicates; here, we use a
        // "DURING" predicate, because we have a fixed range of times that
        // we want to query
        
        
        
        String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";

        // there are quite a few predicates that can operate on other attribute
        // types; the GeoTools Filter constant "INCLUDE" is a default that means
        // to accept everything
        String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;

        String cql = cqlGeometry + " AND " + cqlDates;
        return CQL.toFilter(cql);
    }

    static void queryFeatures(String simpleFeatureTypeName,
                              DataStore dataStore,
                              String geomField, double x0, double y0, double x1, double y1,
                              String dateField, String t0, String t1,
                              String attributesQuery)
            throws CQLException, IOException {

        // construct a (E)CQL filter from the search parameters,
        // and use that as the basis for the query
        Filter cqlFilter = createFilter(geomField, x0, y0, x1, y1, dateField, t0, t1, attributesQuery);
        Query query = new Query(simpleFeatureTypeName, cqlFilter);
        System.out.println("Query is :"+query);
        // submit the query, and get back an iterator over matching features
        FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
        FeatureIterator featureItr = featureSource.getFeatures(query).features();

        // loop through all results
        int n = 0;
        while (featureItr.hasNext()) {
        	
            Feature feature = featureItr.next();
            
            System.out.println((++n) + ".  " +
                    feature.getProperty("srcid").getValue() + "|" +
                    feature.getProperty("value").getValue() + "|" +
                    feature.getProperty("date").getValue() + "|" +
                    feature.getProperty("point_loc").getValue());
                   
        	n++;
        }
        System.out.println(n);
        featureItr.close();
    }

    public static void main(String[] args) throws Exception {
        // find out where -- in HBase -- the user wants to store data
       
    	 random = new Random(5771);
    	
    	 //GeomesaService_new gs =new GeomesaService_new();
    	 
    	 
    	
        Map<String, Serializable> parameters = new HashMap<>();
		parameters.put("bigtable.table.name", "Geomesa4");
		DataStore dataStore = DataStoreFinder.getDataStore(parameters);
        assert dataStore != null;
       
    	 
        long millistart;long milliend;
        
        
        // establish specifics concerning the SimpleFeatureType to store
        String simpleFeatureTypeName = "QuickStart";
        SimpleFeatureType simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);
        
        
        System.out.println("Creating feature-type (schema):  " + simpleFeatureTypeName);
        dataStore.createSchema(simpleFeatureType);

         millistart = System.currentTimeMillis();
        
        for(int k=0;k<7;k++)
        {
        // create new features locally, and add them to this table
        //System.out.println("Creating new features");
        FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, 10000);
       // System.out.println("Inserting new features");
        insertFeatures(simpleFeatureTypeName, dataStore, featureCollection);
         milliend = System.currentTimeMillis();
		 System.out.println("count:"+k);
		 System.out.println("Time"+(milliend-millistart));
        }
        System.out.println("Insertion Done");
        
        
        // query a few Features from this table
        System.out.println("Submitting query");
        
        millistart = System.currentTimeMillis();
        for(int i=0;i<2;i++)
        {
        queryFeatures(simpleFeatureTypeName, dataStore,
                "point_loc", 30, 60, 30.01, 60.01,
                "date", "2014-02-01T00:00:00.000Z", "2014-02-03T23:59:59.999Z",
                null);
        }
        milliend = System.currentTimeMillis();
        System.out.println("Time taken is:"+(milliend-millistart));
        System.out.println("Done query");
        
        
    }
}
