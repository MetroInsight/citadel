package metroinsight.citadel.virtualsensor.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.Util;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.virtualsensor.VirtualSensor;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class VirtualSensorServiceImpl implements VirtualSensorService {
  private final Vertx vertx;
  private final ServiceDiscovery discovery;
  private MetadataService metadataService;

  public VirtualSensorServiceImpl (Vertx vertx, ServiceDiscovery discovery) {
    this.vertx = vertx;
    this.discovery = discovery;
    metadataService = ProxyHelper.createProxy(MetadataService.class, vertx, MetadataService.ADDRESS);
  }
  
  //@Override
  public void getVirtualSensor(String uuid, Handler<AsyncResult<VirtualSensor>> rh) {
    // TODO:
  }
  
  @Override
  public void registerVirtualSensor(String code, JsonObject vsConfig, Handler<AsyncResult<String>> rh) {
    try {
      JsonObject metadata = new JsonObject();
      String[] metadataKeys = {"name", "unit", "pointType"};
      for(int i = 0; i < metadataKeys.length; i++) {
          String key = metadataKeys[i];
          metadata.put(key, vsConfig.getString(key));
      }
      
      // Create a VS like a normal sensor
      Future<String> createFuture = Future.future();
      metadataService.createPoint(metadata, createFuture);
      String uuid = createFuture.result();
      
      // Update VS-specific metadata
      Future<Void> updateFuture = Future.future();
      metadata.clear();
      String[] vsMetadataKeys = {"period", "languageType", "dependentUUIDs"};
      for(int i = 0; i < vsMetadataKeys.length; i++) {
          String key = vsMetadataKeys[i];
          metadata.put(key, vsConfig.getString(key));
      }
      metadata.put("code", code);
      metadataService.upsertMetadata(uuid, metadata, updateFuture);
      rh.handle(Future.succeededFuture(uuid));
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e));
    }

  }
  
  public void execVirtualSensor(String uuid) {
//    VirtualSensor vs = new VirtualSensor(vsConfig.getLong("period"), uuid, code, vsConfig.getString("languageType"), Util.jsonArray2StringArray(vsConfig.getJsonArray("dependentUUIDs")));
  }


}
