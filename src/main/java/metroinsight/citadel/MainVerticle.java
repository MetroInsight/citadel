package metroinsight.citadel;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import metroinsight.citadel.authorization.AuthorizationVerticle;

public class MainVerticle extends AbstractVerticle {
	
	@Override
	public void start() throws Exception {
	  // Deploy verticles.
    //vertx.deployVerticle(MetadataVerticle.class.getName());
    //vertx.deployVerticle(TimeseriesVerticle.class.getName());
    
		//old deployed
		//vertx.deployVerticle(RestApiVerticle.class.getName());
		
		DeploymentOptions opts = new DeploymentOptions()
	            .setWorker(true);
//PropertyConfigurator.configure("/home/citadel/metroinsight/citadel/citadel/src/main/resources/log4j.properties");
		//System.setProperty("hadoop.home.dir", "/");
		//System.setProperty("log4j.configuration",  new File("resources", "log4j.properties").toURI().toURL().toString());
	opts.setConfig(config());
    vertx.deployVerticle(RestApiVerticle.class.getName(), opts, ar -> {
    	if (ar.failed()) {
    		ar.cause().printStackTrace();
    	}
    });
    
    vertx.deployVerticle(AuthorizationVerticle.class.getName());
    
	}//end start()
	
	public static void main(String[] args)  {
	  ClassLoader cl = ClassLoader.getSystemClassLoader();
	  URL[] urls = ((URLClassLoader)cl).getURLs();
	  for(URL url: urls) {
		  System.out.println(url.getFile());
	  }
	}
	
	public void runner() {
		  Vertx vertx = Vertx.vertx();
		  vertx.deployVerticle(new MainVerticle());
	}

}
