package metroinsight.citadel.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class Datapoint {
	
  private String srcid;//unique srcid for the stream belonging to same dataset
  private String unixTimeStamp;//unix timestamp in milliseconds stored in string format
  private String lat;//latitude
  private String lng;//longitude
  private String value;//value of this data point
  
  public Datapoint(){
    
  }
  
  public Datapoint(JsonObject json) {
  	
	this.srcid = json.getString("srcid");
	this.unixTimeStamp = json.getString("unixTimeStamp");
  	this.lat = json.getString("lat");
 	this.lng = json.getString("lng");
 	this.value = json.getString("value");
  	
  }
  
  public Datapoint(String srcid,String unixTimeStamp,String lat, String lng, String value){
  
    this.srcid = srcid;
    this.unixTimeStamp = unixTimeStamp;
    this.lat = lat;
    this.lng=lng;
    this.value=value;
  }
  
  public Datapoint(Datapoint other) {
	this.srcid = other.srcid;
  	this.unixTimeStamp = other.unixTimeStamp;
  	this.lat = other.lat;
  	this.lng=other.lng;
  	this.value=other.value;
  }

  public final String getSrcid(){
    return srcid;
  }

  public final String getUnixTimeStamp(){
	    return unixTimeStamp;
	  }
  
  public final String getLat(){
    return lat;
  }
  
  public final String getLng(){
	    return lng;
	  }
  
  public final String getValue(){
	    return value;
	  }
  
  public final void setSrcid(String srcid){
	    this.srcid = srcid;
	  }
  
  public final void setUnixTimeStamp(String unixTimeStamp){
    this.unixTimeStamp = unixTimeStamp;
  }

  public final void setLat(String lat){
    this.lat = lat;
  }
  
  public final void setLng(String lng){
	this.lng = lng;
	  }
  
  public final void setValue(String value){
	    this.value = value;
	  }
  
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    
    
    json.put("srcid", srcid);
    json.put("unixTimeStamp", unixTimeStamp);
    json.put("lat", lat);
    json.put("lng", lng);
    json.put("value", value);
   
    //DatapointConverter.toJson(this, json);
    return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }

  
}
