package metroinsight.citadel.model;

import java.util.List;

import com.vividsolutions.jts.geom.Geometry;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject()
public class Datapoint {
	
  private String uuid;//unique uuid for the stream belonging to same dataset
  private long timestamp;//unix timestamp in milliseconds stored in string format
  private double value;//value of this data point
  private List<List<Double>> coordinates;
  private String geometryType;
  
  public Datapoint(){
    Geometry a;
  }
  
  public Datapoint(JsonObject json) {
    this.uuid = json.getString("uuid");
    this.timestamp = json.getLong("timestamp");
    this.value = json.getDouble("value");
    this.geometryType = json.getString("geometryType");
    this.coordinates = (List<List<Double>>) json.getValue("coordinates");
  }
  
  public Datapoint(String uuid, Long timestamp, double value, String geometryType, List<List<Double>> coordinates) {
    this.uuid = uuid;
    this.timestamp = timestamp;
    this.geometryType = geometryType;
    this.coordinates = coordinates;
    this.value = value;
  }
  
  public Datapoint(Datapoint other) {
    this.uuid = other.uuid;
  	this.timestamp = other.timestamp;
  	this.value = other.value;
  	this.coordinates = other.coordinates;
  	this.geometryType = other.geometryType;
  }

  public final String getUuid(){
    return uuid;
  }

  public final Long getTimestamp(){
	    return timestamp;
	  }
  
  public final double getValue(){
	    return value;
	  }
  
  public final void setUuid(String uuid){
	    this.uuid = uuid;
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
    json.put("uuid", uuid);
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
