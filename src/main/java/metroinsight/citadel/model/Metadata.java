package metroinsight.citadel.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class Metadata {
  private String pointType;
  private String unit;
  private String srcid;
  
  public Metadata(){
    
  }
  
  public Metadata(JsonObject json) {
  	this.pointType = json.getString("pointType");
  	this.unit = json.getString("unit");
  	this.srcid = json.getString("srcid");
  }
  
  public Metadata(String pointType, String unit, String srcid){
    this.pointType = pointType;
    this.unit = unit;
    this.srcid = srcid;
  }
  
  public Metadata(Metadata other) {
  	this.pointType = other.pointType;
  	this.unit = other.unit;
  	this.srcid = other.srcid;
  }
  
  public final String getPointType(){
    return pointType;
  }

  public final String getSrcid(){
    return srcid;
  }

  public final String getUnit(){
    return unit;
  }
  
  public final void setPointType(String pointType){
    this.pointType = pointType;
  }

  public final void setSrcid(String srcid){
    this.srcid = srcid;
  }

  public final void setUnit(String unit){
    this.unit = unit;
  }
  
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    /*
    json.put("pointType", pointType);
    json.put("unit", unit);
    json.put("srcid", srcid);
    //*/
    MetadataConverter.toJson(this, json);
    return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }

  
}
