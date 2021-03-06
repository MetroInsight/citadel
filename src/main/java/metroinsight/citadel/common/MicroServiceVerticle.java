package metroinsight.citadel.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.servicediscovery.types.HttpEndpoint;
import io.vertx.servicediscovery.types.MessageSource;

/**
 * An implementation of {@link Verticle} taking care of the discovery and
 * publication of services.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MicroServiceVerticle extends AbstractVerticle {

  protected ServiceDiscovery discovery;
  protected Set<Record> registeredRecords = new ConcurrentHashSet<>();

  protected HttpServerOptions getBaseHttpOptions() {
    JsonObject configs = config();
    HttpServerOptions httpOptions = new HttpServerOptions().setSsl(true);
    String sslType = configs.getString("ssl.type", "nossl");
    if (sslType.equals("jks")) {
      httpOptions.setKeyStoreOptions(
          new JksOptions()
          .setPath(configs.getString("ssl.jks.fspath"))
          .setPassword(configs.getString("ssl.jks.password"))); // very
    } else if (sslType.equals("pemfile")) {
      httpOptions.setPemKeyCertOptions(new PemKeyCertOptions().setCertPath(configs.getString("ssl.pem.certfile"))
          .setKeyPath(configs.getString("ssl.pem.keyfile")));
    } else {
      httpOptions.setSsl(false);
    }
    return httpOptions;
  }

  @Override
  public void start() {
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
  }

  public void publishHttpEndpoint(String name, String host, int port, Handler<AsyncResult<Void>> completionHandler) {
    Record record = HttpEndpoint.createRecord(name, host, port, "/");
    publish(record, completionHandler);
  }

  public void publishMessageSource(String name, String address, Class contentClass,
      Handler<AsyncResult<Void>> completionHandler) {
    Record record = MessageSource.createRecord(name, address, contentClass);
    publish(record, completionHandler);
  }

  public void publishMessageSource(String name, String address, Handler<AsyncResult<Void>> completionHandler) {
    Record record = MessageSource.createRecord(name, address);
    publish(record, completionHandler);
  }

  public void publishEventBusService(String name, String address, Class serviceClass,
      Handler<AsyncResult<Void>> completionHandler) {
    Record record = EventBusService.createRecord(name, address, serviceClass);
    publish(record, completionHandler);
  }

  protected void publish(Record record, Handler<AsyncResult<Void>> completionHandler) {
    if (discovery == null) {
      try {
        start();
      } catch (Exception e) {
        throw new RuntimeException("Cannot create discovery service");
      }
    }

    discovery.publish(record, ar -> {
      if (ar.succeeded()) {
        registeredRecords.add(record);
      }
      completionHandler.handle(ar.map((Void) null));
    });
  }

  @Override
  public void stop(Future<Void> future) throws Exception {
    List<Future> futures = new ArrayList<>();
    for (Record record : registeredRecords) {
      Future<Void> unregistrationFuture = Future.future();
      futures.add(unregistrationFuture);
      discovery.unpublish(record.getRegistration(), unregistrationFuture);
    }

    if (futures.isEmpty()) {
      discovery.close();
      future.complete();
    } else {
      CompositeFuture composite = CompositeFuture.all(futures);
      composite.setHandler(ar -> {
        discovery.close();
        if (ar.failed()) {
          future.fail(ar.cause());
        } else {
          future.complete();
        }
      });
    }
  }
}
