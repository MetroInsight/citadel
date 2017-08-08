package metroinsight.citadel.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@DataObject()
public class BaseContent {
  private Boolean success;
  private String reason;
  private JsonArray results;

  public BaseContent(JsonObject json) {
  	this.success = json.getBoolean("success");
  	this.results = json.getJsonArray("results");
  	this.reason = json.getString("reason");
  }
  
  public BaseContent() {
    this.success = false;
    this.results = new JsonArray();
    this.reason = "";
  }
  
  public BaseContent(Boolean success, String reason , JsonArray results) {
    this.success = success;
    this.reason = reason;
    this.results = results;
  }

  public BaseContent(Boolean success, String reason) {
    this.success = success;
    this.reason = reason;
    this.results = new JsonArray();
  }
  
  public BaseContent(BaseContent other) {
    this.success = other.success;
    this.reason = other.reason;
    this.results = other.results;
  }
  
  public Boolean getSuccess() {
    return this.success;
  }
  
  public void setSucceess(Boolean success) {
    this.success = success;
  }
  
  public String getReason() {
    return this.reason;
  }
  
  public void setReason(String reason) {
    this.reason = reason;
  }
  
  public JsonArray getResults() {
    return results;
  }
  
  public void setResults(JsonArray results) {
    this.results = results;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.put("success", success);
    json.put("reason", reason);
    json.put("results", results);
    return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }
  
}

