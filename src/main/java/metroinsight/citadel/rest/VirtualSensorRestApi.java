package metroinsight.citadel.rest;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.model.BaseContent;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class VirtualSensorRestApi extends RestApiTemplate {

  private VirtualSensorService vsService;
  private Vertx vertx;
  
  VirtualSensorRestApi(Vertx vertx) {
    vsService = ProxyHelper.createProxy(VirtualSensorService.class, vertx, VirtualSensorService.ADDRESS);
  }

  public void registerVirtualSensor(RoutingContext rc) {
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    String code = q.getString("code");
    q.remove("code");
    vsService.registerVirtualSensor(code, q, ar -> {
      HttpServerResponse resp = getDefaultResponse(rc);
      BaseContent content = new BaseContent();
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        resp.setStatusCode(400);
      } else {
        content.setSucceess(true);;
        String uuid = ar.result();
        JsonArray resultArray = new JsonArray();
        resultArray.add(uuid);
        content.setResults(resultArray);
        resp.setStatusCode(200);
      }
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
      });
  }

}
