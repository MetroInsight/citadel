package metroinsight.citadel.federation.impl.MesoWest;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.authorization.FederatedUserAccounts;

public class MesoWestAccounts extends FederatedUserAccounts{

  public MesoWestAccounts(Vertx vertx) {
    super(vertx);
    // TODO Auto-generated constructor stub
  }
  
  public void getMesoInfo(String identity, Handler<AsyncResult<JsonObject>> rh) {
    JsonObject query = new JsonObject().put("user_id", identity);
    JsonObject sel = new JsonObject().put("mesowest", 1);
    client.findOne(COLL_NAME, query, sel, res -> {
      if (res.succeeded()) {
        JsonObject resss = res.result();
        rh.handle(Future.succeededFuture(res.result()));
      } else {
        rh.handle(Future.failedFuture(res.cause().getMessage()));
      }
    });
  }
  
  public void getApikey(String identity, Handler<AsyncResult<String>> rh) {
    getMesoInfo(identity, res -> {
      if (res.succeeded()) {
        JsonObject resss = res.result();
        rh.handle(Future.succeededFuture(res.result().getString("apikey")));
      } else {
        rh.handle(Future.failedFuture(res.cause().getMessage()));
      }
    });
  }
  
  public void addMesoWestUser(String identity, String apikey, Handler<AsyncResult<Void>> rh) {
    // identity sould be an email.
    JsonObject query = new JsonObject().put("user_id", identity);
    JsonObject update = new JsonObject().put("$set", new JsonObject().put("mesowest", new JsonObject().put("apikey", apikey)));
    client.updateCollectionWithOptions(COLL_NAME, query, update, upsertOptions, res -> {
      if (res.succeeded()) {
        rh.handle(Future.succeededFuture());
      } else {
        rh.handle(Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

}
