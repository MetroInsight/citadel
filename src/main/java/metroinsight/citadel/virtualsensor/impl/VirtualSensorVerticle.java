package metroinsight.citadel.virtualsensor.impl;

import static metroinsight.citadel.virtualsensor.VirtualSensorService.ADDRESS;
import static metroinsight.citadel.virtualsensor.VirtualSensorService.EVENT_ADDRESS;

import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class VirtualSensorVerticle extends MicroServiceVerticle {

  @Override
 public void start() {
    super.start();
    VirtualSensorService service = new VirtualSensorServiceImpl(vertx, discovery);
    ProxyHelper.registerService(VirtualSensorService.class, vertx, service, ADDRESS);
    
    publishEventBusService("virtualsensor", ADDRESS, VirtualSensorService.class, rh -> {
      if (rh.failed()) {
        rh.cause().printStackTrace();
      } else {
        System.out.println("VirtualSensor service published: " + rh.succeeded());
      }
    });
    
    publishMessageSource("virtualsensor-events", EVENT_ADDRESS, rh -> {
      if (rh.failed()) {
        rh.cause().printStackTrace();
      } else {
        System.out.println("VirtualSensor event service published: " + rh.succeeded());
      }
    });
  }
}
