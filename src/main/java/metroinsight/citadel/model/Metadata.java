package metroinsight.citadel.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class Metadata {
  private String pointType;
  private String unit;
  private String uuid;
  private String name;
  
  public Metadata(){
    
  }
  
  public Metadata(JsonObject json) {
  	this.pointType = json.getString("pointType");
  	this.unit = json.getString("unit");
  	this.uuid = json.getString("uuid");
  	this.name = json.getString("name");
  }
  
  public Metadata(String pointType, String unit, String uuid, String name){
    this.pointType = pointType;
    this.unit = unit;
    this.uuid = uuid;
    this.name = name;
  }
  
  public Metadata(Metadata other) {
  	this.pointType = other.pointType;
  	this.unit = other.unit;
  	this.uuid = other.uuid;
  	this.name = other.name;
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

  public final String getUuid(){
    return uuid;
  }

  public final String getUnit(){
    return unit;
  }
  
  public final void setPointType(String pointType){
    this.pointType = pointType;
  }

  public final void setUuid(String uuid){
    this.uuid = uuid;
  }

  public final void setUnit(String unit){
    this.unit = unit;
  }
  
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    /*
    json.put("pointType", pointType);
    json.put("unit", unit);
    json.put("uuid", uuid);
    */
    MetadataConverter.toJson(this, json);
    return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }

  
}
