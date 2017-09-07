package metroinsight.citadel.virtualsensor;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@VertxGen
@ProxyGen
public interface VirtualSensorService {
  String ADDRESS = "service.virtualsensor";
  String EVENT_ADDRESS = "virtualsensor";

  static VirtualSensorService createProxy(Vertx vertx, String address) {
    return new VirtualSensorServiceVertxEBProxy(vertx, address);
  }
  
//  void getVirtualSensor(String uuid, Handler<AsyncResult<VirtualSensor>> rh);

  void registerVirtualSensor(String code, JsonObject vsConfig, Handler<AsyncResult<String>> rh);
}
