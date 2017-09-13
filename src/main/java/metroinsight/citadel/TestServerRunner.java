package metroinsight.citadel;

import io.vertx.core.Vertx;

public class TestServerRunner {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(new MainVerticle());
	}

}
