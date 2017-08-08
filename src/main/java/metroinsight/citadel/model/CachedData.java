package metroinsight.citadel.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject()
public class CachedData {
  private String pointType;
  private String unit;
  private String name;
  private Double lng;
  private Double lat;
  private Long timestamp;
  private Double value;
  
  public CachedData(){
    
  }
  
  public CachedData(JsonObject json) {
  	this.pointType = json.getString("pointType");
  	this.unit = json.getString("unit");
  	this.name = json.getString("name");
  	this.lng = json.getDouble("lng");
  	this.lat = json.getDouble("lat");
  	this.value = json.getDouble("value");
  	this.timestamp= json.getLong("timestamp");
  }
  
  public CachedData(String pointType, String unit, String name, Double lng, Double lat, Long timestamp, Double value){
    this.pointType = pointType;
    this.unit = unit;
    this.name = name;
    this.lng = lng;
    this.lat = lat;
    this.timestamp = timestamp;
    this.value = value;
  }
  
  public CachedData(CachedData other) {
    this.pointType = other.pointType;
    this.unit = other.unit;
    this.name = other.name;
    this.lng = other.lng;
    this.lat = other.lat;
    this.timestamp = other.timestamp;
    this.value = other.value;
  }
  
  public final String getName() {
    return this.name;
  }

  public final void setName(String name) {
    this.name = name;
  }
  
  public final String getPointType(){
    return pointType;
  }

  public final void setPointType(String pointType){
    this.pointType = pointType;
  }

  public final String getUnit(){
    return unit;
  }

  public final void setUnit(String unit){
    this.unit = unit;
  }
  
  public final Double getLng() {
    return this.lng;
  }
  
  public final void setLng(Double lng) {
    this.lng = lng;
  }

  public final Double getLat() {
    return this.lat;
  }
  
  public final void setLat(Double lat) {
    this.lat = lat;
  }

  public final Double getValue() {
    return this.value;
  }
  
  public final void setValue(Double value) {
    this.value = value;
  }
  
  public final Long getTimestamp() {
    return this.timestamp;
  }
  
  public final void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }
  
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.put("name", name);
    json.put("timestamp", timestamp);
    json.put("value", value);
    json.put("lng", lng);
    json.put("lat", lat);
    json.put("unit", unit);
    json.put("pointType", pointType);
    return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }
  
}
