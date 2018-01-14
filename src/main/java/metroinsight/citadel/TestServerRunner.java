package metroinsight.citadel;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class TestServerRunner {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    Vertx vertx = Vertx.vertx();
    Buffer configBuffer = vertx.fileSystem().readFileBlocking("./src/main/resources/conf/citadel-conf.json");
    JsonObject configs = new JsonObject(configBuffer);
    DeploymentOptions options = new DeploymentOptions();
    options.setConfig(configs);
    vertx.deployVerticle(new MainVerticle(), options);
  }

}
