package metroinsight.citadel.rest;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.authorization.authmetadata.AuthorizationMetadata;
import metroinsight.citadel.authorization.authmetadata.impl.AuthMetadataMongodb;
import metroinsight.citadel.common.ErrorMessages;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.datacache.DataCacheService;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.BaseContent;

public class MetadataRestApi extends RestApiTemplate {

  private MetadataService metadataService;
  private DataCacheService cacheService;
  Vertx vertx;

  AuthorizationMetadata authMetadata;


  public MetadataRestApi(Vertx vertx, JsonObject configs) {
    this.vertx = vertx;
    this.configs = configs;
    authMetadata = new AuthMetadataMongodb();
    metadataService = ProxyHelper.createProxy(MetadataService.class, vertx, MetadataService.ADDRESS);
    cacheService = ProxyHelper.createProxy(DataCacheService.class, vertx, DataCacheService.ADDRESS);
  }

  public void queryPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    // System.out.println("In query point:"+q);
    metadataService.queryPoint(q, ar -> {
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        resp.setStatusCode(400);
      } else {
        content.setSucceess(true);
        content.setResults(ar.result());
        resp.setStatusCode(200);
      }
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp.putHeader("content-length", cLen)
        .write(cStr)
        .end();
    });
  }
  
  public void upsertMetadata (RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    JsonObject body = rc.getBodyAsJson();
    String uuid = rc.request().getParam("uuid");
    if (!body.containsKey("userToken")) {
      sendErrorResponse(resp, 400, ErrorMessages.EMPTY_SEC_TOKEN);
      return;
    }
    JsonObject metadata = body.getJsonObject("metadata");
    String userToken = body.getString("userToken");
    String userId = authMetadata.getUserId(userToken);
    // TOOD: IMPORTANT: Check if the user has the right level of permission.
    if (metadata.containsKey("owner")) {
      // This transfers ownership currently. TODO: Maybe just add owner?
      // TODO: Need to remove previous relevant metadata and policy.
      String newOwnerId = metadata.getString("owner");
      authMetadata.insertDsOwner(uuid, newOwnerId);
      authMetadata.insert_policy(uuid, newOwnerId, "true");
    }
    // TODO: Evaluate if the keys/values are valid.
    metadataService.upsertMetadata(uuid, metadata, ar -> {
      if (ar.failed()) {
        sendErrorResponse(resp, 500, ar.cause().getMessage());
      } else {
        sendSuccesResponse(resp, 200, new JsonArray());
      }
    });
  }

  public void getPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    String uuid = rc.request().getParam("uuid");
    if (uuid == null) {
      sendErrorResponse(resp, 400, ErrorMessages.EMPTY_UUID);
      return ;
    } else {
      metadataService.getPoint(uuid, ar -> {
        if (ar.failed()) {
          content.setReason(ar.cause().getMessage());
          resp.setStatusCode(400);
        } else {
          JsonArray pointResult = new JsonArray();
          pointResult.add(ar.result());
          resp.setStatusCode(200);
          content.setSucceess(true);
          content.setResults(pointResult);
        }
        String cStr = content.toString();
        String cLen = Integer.toString(cStr.length());
        resp.putHeader("content-length", cLen).write(cStr).end();
      });
    }
  }

  public void createPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();

    JsonObject body = new JsonObject();
    try {
      body = rc.getBodyAsJson();
    } catch (Exception e) {
      sendErrorResponse(resp, 400, ErrorMessages.NOT_JSON);
      return ;
    }

    try {
      long startTime = System.nanoTime();
      System.out.println("body is:" + body);
      // check token and sensor is present
      /*
      if (body.containsKey(Auth_meta.userToken)) {
        String userToken = body.getString(Auth_meta.userToken);
        // check if this token exists in the HBase, and if it exists, what is the userID
        long authStartTime = System.nanoTime();
        String userId = Auth_meta.get_userID(userToken);
        long authEndTime = System.nanoTime();
        System.out.println(String.format("Auth check Time: %f", ((float)authEndTime - (float)authStartTime)/1000000));

        if (!userId.equals(""))// user is present in the system
        {
          // token exists and is linked to the valid userId
          JsonObject point = body.getJsonObject("point");
          String uuid = UUID.randomUUID().toString();
          point.put("uuid", uuid);// This is later used by metadataService.createPoint
          point.put("userId", userId);// This can be later used by metadataService.createPoint to link a point to
                                      // userID

          // original function to insert Point
          // Get the query as JSON.
          // Call createPoint in metadataService asynchronously.
          metadataService.createPoint(point, ar -> {
            // ar is a result object created in metadataService.createPoint
            // We pass what to do with the result in this format.
            String cStr;
            String cLen;
            if (ar.failed()) {
              // if the service is failed
              resp.setStatusCode(400);
              content.setReason(ar.cause().getMessage());
              cStr = content.toString();
            } else {

              // we succeeded

              // inserts the owner token, userId and ds_ID into the hbase metadata table
              long authCreateStartTime = System.nanoTime();
              Auth_meta.insert_ds_owner(uuid, userToken, userId);

              // insert the policy for Owner to default "true", no-space-time constraints
              Auth_meta.insert_policy(uuid, userId, "true");
              long authCreateEndTime = System.nanoTime();
              System.out.println(String.format("Point auth create time: %f", ((float)authCreateEndTime - (float)authCreateStartTime)/1000000));

              // Construct response object.
              resp.setStatusCode(201);
              JsonObject pointCreateContent = new JsonObject();
              pointCreateContent.put("success", true);
              pointCreateContent.put("uuid", ar.result().toString());
              cStr = pointCreateContent.toString();
            }
            cLen = Integer.toString(cStr.length());
            resp.putHeader("content-length", cLen)
              .write(cStr)
              .end();
            long endTime = System.nanoTime();
            System.out.println(String.format("Total Creation API Time: %f", ((float)endTime - (float)startTime)/1000000));
          });

        } // end if(!userId.equals(""))
        else {
          System.out.println("Token is not Valid");
          sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");

        } // end else

      } // end if(body.containsKey(Auth_meta.userToken)&&body.containsKey("sensor"))

      else {
        System.out.println("In MetadataRestApi: Insert data parameters are missing");
        sendErrorResponse(resp, 400, "Parameters are missing");
        */
      if (!body.containsKey("userToken")) {
        sendErrorResponse(resp, 400, ErrorMessages.EMPTY_SEC_TOKEN);
        return ;
      }
      String token = body.getString("userToken");
      String userId = authMetadata.getUserId(token);
      if (userId == null) {
        sendErrorResponse(resp, 400, ErrorMessages.USER_NOT_FOUND);
        return ;
      }
      JsonObject point = body.getJsonObject("point");
      if (!point.containsKey("name")) {
        sendErrorResponse(resp, 400, ErrorMessages.PARAM_MISSING);
      }
      
      //TODO: Once implemented, add if the point exists in redis cache.
      
      //String uuid = UUID.randomUUID().toString();
      //point.put("uuid", uuid);// This is later used by metadataService.createPoint
      point.put("userId", userId);// This can be later used by metadataService.createPoint to link a point to
                                  // userID

      // original function to insert Point
      // Get the query as JSON.
      // Call createPoint in metadataService asynchronously.
      metadataService.createPoint(point, ar -> {
        // ar is a result object created in metadataService.createPoint
        // We pass what to do with the result in this format.
        String cStr;
        String cLen;
        if (ar.failed()) {
          // if the service is failed
          resp.setStatusCode(400);
          content.setReason(ar.cause().getMessage());
          cStr = content.toString();
        } else {

          // we succeeded
          String uuid = ar.result();

          // inserts the owner token, userId and ds_ID into the hbase metadata table
          authMetadata.insertDsOwner(uuid, userId);

          // insert the policy for Owner to default "true", no-space-time constraints
          authMetadata.insert_policy(uuid, userId, "true");

          // Construct response object.
          resp.setStatusCode(201);
          JsonObject pointCreateContent = new JsonObject();
          pointCreateContent.put("success", true);
          pointCreateContent.put("uuid", ar.result().toString());
          cStr = pointCreateContent.toString();
        }
        cLen = Integer.toString(cStr.length());
        resp.putHeader("content-length", cLen).write(cStr).end();
        sendSuccesResponse(resp, 201, new JsonArray());
      });
    } catch (Exception e) {
      e.printStackTrace();
      sendErrorResponse(resp, 500, e.getMessage());
    }

    /*
     * end Verifying the Token is present in the point and is valid
     */

  }

}
