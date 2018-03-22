package metroinsight.citadel.rest;

import java.io.File;
import java.io.IOException;

import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import io.vertx.ext.web.sstore.SessionStore;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import metroinsight.citadel.authorization.GoogleLogin;
import metroinsight.citadel.common.MicroServiceVerticle;

public class RestApiVerticle extends MicroServiceVerticle {

  MetadataRestApi metadataRestApi;
  DataRestApi dataRestApi;
  VirtualSensorRestApi vsRestApi;
  DataCacheRestApi dataCacheRestApi;
  
  void setAuthRoutes(Router router) {
    JsonObject configs = config();
    GoogleLogin googlelogin = new GoogleLogin(configs);
    router.route("/login").handler(CookieHandler.create());
    router.route("/logout").handler(CookieHandler.create());
    router.route("/index").handler(CookieHandler.create());
    router.route("/scripts").handler(CookieHandler.create());
    router.route("/api/token").handler(CookieHandler.create());
    router.route().handler(CookieHandler.create());
    // Create a clustered session store using defaults
    SessionStore store = LocalSessionStore.create(vertx);
    SessionHandler sessionHandler = SessionHandler.create(store);
    // Make sure all requests are routed through the session handler too

    router.route().handler(sessionHandler);
    router.route("/login*").handler(BodyHandler.create());
    router.route("/login").handler(googlelogin::DisplayLoginJade);

    router.route("/index*").handler(BodyHandler.create());
    router.route("/index").handler(googlelogin::DisplayIndexJade);

    // Bind "/" to our hello message - so we are still compatible.
    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();
      response.putHeader("content-type", "text/html").end("<h1>Welcome to Citadel Authorization</h1>");
    });

    // Serve static resources from the /assets directory
    // router.route("/assets/*").handler(StaticHandler.create("assets"));
    router.route("/scripts/*").handler(StaticHandler.create("scripts"));

    // REST API routing for accepting google token
    router.route("/api/token*").handler(BodyHandler.create());
    router.post("/api/token").handler(googlelogin::DisplayToken);

    // Routing to display index page
    router.route("/logout*").handler(BodyHandler.create());
    router.post("/logout").handler(googlelogin::LogOut);
  }

  @Override
  public void start(Future<Void> fut) {

    JsonObject configs = config();

    HttpServerOptions httpOptions = getBaseHttpOptions();
    int restPort = config().getInteger("rest.http.port", 8080);
    httpOptions.setPort(restPort);
    int authPort = config().getInteger("auth.http.port", 8088);
    int policyPort = config().getInteger("policy.http.port", 8089);
    int apidocPort = config().getInteger("apidoc.http.port", 9090);
    String hostname = config().getString("hostname", "localhost");

    // Init service discovery. Future purpose
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));

    // REST API modules
    // Jason Note: Propagating configs to all the modules is not a good practice in
    // microservices design.
    // However, as Authoriazation_Metadata is not a microservice but a library,
    // all the dependent modules need to know the configs.
    metadataRestApi = new MetadataRestApi(vertx, configs);
    dataRestApi = new DataRestApi(vertx, configs);
    vsRestApi = new VirtualSensorRestApi(vertx);
    dataCacheRestApi = new DataCacheRestApi(vertx);

    Router router = Router.router(vertx);

    router.route().handler(CorsHandler.create(".*")
        .allowedMethod(HttpMethod.GET)
        .allowedMethod(HttpMethod.POST)
        .allowedMethod(HttpMethod.OPTIONS)
        .allowCredentials(true)
        .allowedHeader("Access-Control-Allow-Credentials")
        .allowedHeader("X-PINGARUNER")
        .allowedHeader("Content-Type")
        .allowedHeader("authorization"));
    
    router.route().handler(BodyHandler.create());
    try {
      System.out.println("====================");
      System.out.println(new File("").getCanonicalPath());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // Main page. TODO
    router.route("/").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("content-type", "text/html").end("<h1>Welcome to Citadel</h1>\n"
          + "<h1><a href=\"http://metroinsight.westus.cloudapp.azure.com/doc/api/\">API Documentation</a></h1>");
    });

    router.route("/*").handler(BodyHandler.create());
    
    router.route("/api/ui").handler(StaticHandler.create("static/apidoc.html"));

    /*
    // Redirection to API Doc (TODO: Swagger should be tightly integrated.)
    router.get("/doc/api").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("Location", String.format("https://%s:%d/api/ui/", hostname, apidocPort)); // TODO: Need to fill this everytime for now.
      response.setStatusCode(303);
      response.end();
    });
    */

    // REST API routing for MetaData
    router.post("/api/point").blockingHandler(metadataRestApi::createPoint);
    router.get("/api/point/:uuid").blockingHandler(metadataRestApi::getPoint);
    router.post("/api/point/:uuid").blockingHandler(metadataRestApi::upsertMetadata);
    router.post("/api/query").blockingHandler(metadataRestApi::queryPoint);

    // REST API routing for Data
    router.post("/api/data").blockingHandler(dataRestApi::insertData);
    router.post("/api/querydata").blockingHandler(dataRestApi::queryData);
    router.post("/api/querydata/simplebbox").blockingHandler(dataCacheRestApi::querySimpleBbox);
    
    // TODO: Currently redirecting below APIs. They need to be integrated under this verticle.
    //       Better use Microservice archictecture.

    // REST API for auth
    /*
    router.get("/login").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("Location", String.format("https://%s:%d/login/", hostname, authPort));
      response.setStatusCode(303);
      response.end();
    });
    */
    setAuthRoutes(router);

    // REST API for policy
    router.post("/api/registerPolicy").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("Location", String.format("https://%s:%d/api/registerPolicy/", hostname, policyPort));
      response.setStatusCode(307);
      response.end();
    });

    HttpServer server = vertx.createHttpServer(httpOptions);
    server = server.requestHandler(router::accept);
    server.listen(
        result -> {
          if (result.succeeded()) {
            fut.complete();
            System.out.println("REST_API_VERTICLE STARTED on " + Integer.toString(result.result().actualPort()));
          } else {
            fut.fail(result.cause());
          }
        });
  }

}
