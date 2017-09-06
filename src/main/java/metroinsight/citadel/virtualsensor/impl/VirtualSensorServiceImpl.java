package metroinsight.citadel.virtualsensor.impl;

import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import metroinsight.citadel.common.Util;
import metroinsight.citadel.virtualsensor.VirtualSensor;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class VirtualSensorServiceImpl implements VirtualSensorService {
  private final Vertx vertx;
  private final ServiceDiscovery discovery;

  public VirtualSensorServiceImpl (Vertx vertx, ServiceDiscovery discovery) {
    this.vertx = vertx;
    this.discovery = discovery;
  }
  
  //@Override
  public void getVirtualSensor(String uuid, Handler<AsyncResult<VirtualSensor>> rh) {
    // TODO:
  }
  
  @Override
  public void registerVirtualSensor(String code, JsonObject vsConfig, Handler<AsyncResult<String>> rh) {
    String uuid = UUID.randomUUID().toString();
    VirtualSensor vs = new VirtualSensor(vsConfig.getLong("period"), uuid, code, vsConfig.getString("languageType"), Util.jsonArray2StringArray(vsConfig.getJsonArray("dependentUUIDs")));
  }


}
