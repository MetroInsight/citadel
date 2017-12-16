package metroinsight.citadel.policy;

import metroinsight.citadel.data.DataRestApi;
import metroinsight.citadel.metadata.MetadataRestApi;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;

public class PolicyVerticle extends AbstractVerticle {
	protected ServiceDiscovery discovery;
	PolicyManagement pm;
	@Override
	  public void start(Future<Void> fut){
	    // Init service discovery. Future purpose
	    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
	    
	    pm=new PolicyManagement();
	    
	    Router router = Router.router(vertx);
	    
	    HttpServerOptions options = new HttpServerOptions()
				  .setSsl(true)
				  .setKeyStoreOptions(
				  new JksOptions().
				   // setPath("/media/sandeep/2Tb/sandeep/MetroInsight/citadel_certificate/selfsigned.jks").
				  setPath("/Users/administrator/MetroInsight/self-signed-certificate/selfsigned.jks").  
				  //setPath("/home/sandeep/MetroInsight/citadel_certificate/selfsigned.jks").
				    setPassword("CitadelTesting")//very IMP: Change this password on the Production Version
				);
	    
	    // Main page. TODO
	    router.route("/").handler(rc -> {
	      HttpServerResponse response = rc.response();
	      response
	          .putHeader("content-type", "text/html")
	          .end("<h1>Welcome to Citadel Policy Management</h1>");
	    });
	    
	    router.route("/*").handler(BodyHandler.create());
	    
	    router.post("/api/registerPolicy").handler(pm::registerPolicy);
	    
	    
	    
	   
	    vertx
	        .createHttpServer(options)
	        .requestHandler(router::accept)
	        .listen(
	            config().getInteger("http.port", 8089),
	            result -> {
	              if (result.succeeded()) {
	                fut.complete();
	                System.out.println("POLICY_VERTICLE STARTED on " + Integer.toString(result.result().actualPort()));
	              } else {
	                fut.fail(result.cause());
	                }
	              }
	        );
	  }//end Start

	
}//end class PolicyVerticle
