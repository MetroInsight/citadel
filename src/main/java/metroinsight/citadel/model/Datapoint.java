package metroinsight.citadel.model;

import java.util.List;

import com.vividsolutions.jts.geom.Geometry;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class Datapoint {
	
  private String srcid;//unique srcid for the stream belonging to same dataset
  private long timestamp;//unix timestamp in milliseconds stored in string format
  private double value;//value of this data point
  private List<List<Double>> coordinates;
  private String geometryType;
  
  public Datapoint(){
    Geometry a;
  }
  
  public Datapoint(JsonObject json) {
    this.srcid = json.getString("srcid");
    this.timestamp = json.getLong("timestamp");
    this.value = json.getDouble("value");
    this.geometryType = json.getString("geometryType");
    this.coordinates = (List<List<Double>>) json.getValue("coordinates");
  }
  
  public Datapoint(String srcid, Long timestamp, double value, String geometryType, List<List<Double>> coordinates) {
    this.srcid = srcid;
    this.timestamp = timestamp;
    this.geometryType = geometryType;
    this.coordinates = coordinates;
    this.value = value;
  }
  
  public Datapoint(Datapoint other) {
    this.srcid = other.srcid;
  	this.timestamp = other.timestamp;
  	this.value = other.value;
  	this.coordinates = other.coordinates;
  	this.geometryType = other.geometryType;
  }

  public final String getSrcid(){
    return srcid;
  }

  public final Long getTimestamp(){
	    return timestamp;
	  }
  
  public final double getValue(){
	    return value;
	  }
  
  public final void setSrcid(String srcid){
	    this.srcid = srcid;
	  }
  
  public final void setTimestamp(Long timestamp){
    this.timestamp = timestamp;
  }
  
  public final void setValue(double value){
	    this.value = value;
	  }
  
  public final void setGeometryType(String geometryType) {
    this.geometryType = geometryType;
  }
  
  public final String getGeometryType() {
    return this.geometryType;
  }
  
  public final List<List<Double>> getCoordinates() {
    return this.coordinates;
  }
  
  public final void setCoordinates(List<List<Double>>coordinates) {
    this.coordinates = coordinates;
  }
  
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.put("srcid", srcid);
    json.put("timestamp", timestamp);
    json.put("value", value);
    json.put("geometryType", geometryType);
    json.put("coordinates", coordinates.toString());
    return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }
  
}
