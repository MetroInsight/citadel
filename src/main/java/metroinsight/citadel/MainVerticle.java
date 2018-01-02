package metroinsight.citadel;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import metroinsight.citadel.authorization.AuthorizationVerticle;
import metroinsight.citadel.metadata.impl.MetadataVerticle;
import metroinsight.citadel.policy.PolicyVerticle;
import metroinsight.citadel.rest.RestApiVerticle;
import metroinsight.citadel.virtualsensor.impl.VirtualSensorVerticle;

public class MainVerticle extends AbstractVerticle {
	
	@Override
	public void start() throws Exception {
	// Deploy verticles.
    vertx.deployVerticle(MetadataVerticle.class.getName());
    vertx.deployVerticle(VirtualSensorVerticle.class.getName());
	
    vertx.deployVerticle(PolicyVerticle.class.getName());
	vertx.deployVerticle(AuthorizationVerticle.class.getName());
	
    DeploymentOptions opts = new DeploymentOptions()
        .setWorker(true);
    //System.setProperty("hadoop.home.dir", "/");
    //System.setProperty("log4j.configuration",  new File("resources", "log4j.properties").toURI().toURL().toString());
    opts.setConfig(config());
    /*
    vertx.deployVerticle(RestApiVerticle.class.getName(), opts, ar -> {
      if (ar.failed()) {
        ar.cause().printStackTrace();
        }
    });
    //*/
 }
}
