package metroinsight.citadel;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.authorization.AuthorizationVerticle;
import metroinsight.citadel.metadata.impl.MetadataVerticle;
import metroinsight.citadel.policy.PolicyVerticle;
import metroinsight.citadel.rest.RestApiVerticle;
import metroinsight.citadel.virtualsensor.impl.VirtualSensorVerticle;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    // Deploy verticles.
    
    // Deploy options
    DeploymentOptions options = new DeploymentOptions().setConfig(config());


    vertx.deployVerticle(MetadataVerticle.class.getName(), options);
    vertx.deployVerticle(VirtualSensorVerticle.class.getName(), options);

    vertx.deployVerticle(PolicyVerticle.class.getName(), options);
    vertx.deployVerticle(AuthorizationVerticle.class.getName(), options);

    options.setWorker(true);

    vertx.deployVerticle(RestApiVerticle.class.getName(), options, ar -> {
      if (ar.failed()) {
        ar.cause().printStackTrace();
      }
    });
  }
}
