package metroinsight.citadel.benchmark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.buffer.Buffer;
import metroinsight.citadel.data.impl.GeomesaHbase;
import io.vertx.core.Vertx;

public class GeomesaAloneTest {
  
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    Buffer confBuffer = vertx.fileSystem().readFileBlocking("./src/main/resources/conf/citadel-conf.json");
    JsonObject configs = new JsonObject(confBuffer);
    String tableName = configs.getString("data.geomesa.tablename");
    GeomesaHbase gmh = new GeomesaHbase(tableName);
    String geomField = "loc";
    String dateField = "date";
    Double lat_min = 32.868623;
    Double lng_min = -117.244438;
    Double lat_max = 32.893202;
    Double lng_max = -117.214398;
    long ts_min = 1388534400000L;
    long ts_max = 1500813708623L;
    List uuids = new ArrayList();
    String beginTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
    System.out.println(beginTime);
    try {
      JsonArray res = gmh.queryFeatures_Box_Lat_Lng_Time_Range(geomField, dateField, lat_min, lng_min, lat_max, lng_max, ts_min, ts_max, uuids);
      System.out.println(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
    String endTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
    System.out.println(endTime);
  }

}
