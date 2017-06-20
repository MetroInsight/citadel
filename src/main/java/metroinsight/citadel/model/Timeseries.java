package metroinsight.citadel.model;

import java.util.HashMap;
import java.util.List;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class Timeseries{
//  List<Series>
  private String name;
  private List<String> columns;
  private List<HashMap<String, Float>> values;  // String=timestamp ISO, Float=value
                                                // TODO: String needs to be timestamp int?.
  //TODO I would like to use javatuples.Pair instead of HashMap.
  //     But don't know how to generate JSON coverters automatically from there.
  //		 This may need a manual implementation of converter functions.
  
  public Timeseries(){
  }

  public Timeseries(JsonObject json){
  	TimeseriesConverter.fromJson(json, this);
  }
  
  public Timeseries(String name, List<String> columns, List<HashMap<String, Float>> values) {
    this.name = name;
    this.columns = columns;
    this.values = values;
  }
  
  public final String getName(){
    return name;
  }

  public final List<HashMap<String, Float>> getValues() {
    return values;
  }

  public final List<String> getColumns() {
    return columns;
  }

  public final void setColumns(List<String> columns){
    this.columns = columns;
  }

  public final void setName(String name) {
    this.name = name;
  }

  public final void setValues(List<HashMap<String, Float>> values) {
    this.values = values;
  }
  //@Override 
  //public String toString() { 
    //List<String> list = new ArrayList<String>(map.values());
  //  return "Timeseries [name=" + name + ", columns=
  
  public JsonObject toJson() {
  	JsonObject json = new JsonObject();
  	TimeseriesConverter.toJson(this, json);
  	return json;
  }
  
  public String toString() {
  	return this.toJson().toString();
  }
}

