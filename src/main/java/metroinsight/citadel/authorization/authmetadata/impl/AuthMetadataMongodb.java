package metroinsight.citadel.authorization.authmetadata.impl;

import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.authorization.authmetadata.AuthorizationMetadata;

public class AuthMetadataMongodb implements AuthorizationMetadata {
  MongoClient mongo;
  MongoCollection<Document> userColl;
  MongoCollection<Document> dsColl;
  MongoCollection<Document> userDsColl;
  
  public AuthMetadataMongodb() {
    Buffer confBuffer = Vertx.vertx().fileSystem().readFileBlocking("./src/main/resources/conf/citadel-conf.json");
    JsonObject configs = new JsonObject(confBuffer);
    String hostname = configs.getString("auth.mongodb.hostname");
    Integer port = configs.getInteger("auth.mongodb.port");
  	String dbName = configs.getString("auth.mongodb.dbname");
  	String user = configs.getString("auth.mongodb.user");
  	char[] password = configs.getString("auth.mongodb.password").toCharArray();
  	MongoCredential credential = MongoCredential.createCredential(user, dbName, password);
  	MongoClientOptions mongoOpt = MongoClientOptions.builder().serverSelectionTimeout(3000).build();

  	mongo = new MongoClient(new ServerAddress(hostname, port), credential, mongoOpt);
    MongoDatabase db = mongo.getDatabase(dbName);
    String userCollName = "users";
    String dsCollName = "datastreams";
    String userDsCollName = "users-datastreams";
    userColl = db.getCollection(userCollName);
    dsColl = db.getCollection(dsCollName);
    userDsColl = db.getCollection(userDsCollName);
  }
  
  @Override
  public String getToken(String userId) {
    BasicDBObject query = new BasicDBObject("userId", userId);
    Document user = userColl.find(query).first();
    String token = (String) getValueOrNull(user, "token");
    return token;
  }

  @Override
  public void insertToken(String userId, String token) {
    BasicDBObject query = new BasicDBObject("userId", userId);
    Document doc = new Document()
        .append("userId", userId)
        .append("token", token);
    if (!userColl.replaceOne(query, doc, new UpdateOptions().upsert(true)).wasAcknowledged()) {
      throw new java.lang.Error("Failed at insertion");
    }
  }

  @Override
  public void insertDsOwner(String dsId, String ownerId) {
    BasicDBObject query = new BasicDBObject("dsId", dsId);
    Document doc = new Document()
        .append("dsId", dsId)
        .append("owner", ownerId);
    if (!dsColl.replaceOne(query, doc, new UpdateOptions().upsert(true)).wasAcknowledged()) {
      throw new java.lang.Error("Failed it insertion");
    }
  }
  
  private Object getValueOrNull(Document doc, Object key) {
    if (doc == null) {
      return null;
    } else {
      Object res = doc.get(key);
      return res;
    }
  }

  @Override
  public String getDsOwnerId(String dsId) {
    BasicDBObject query = new BasicDBObject("dsId", dsId);
    Document ds = dsColl.find(query).first();
    String ownerId = (String) getValueOrNull(ds, "owner");
    return ownerId;
  }

  @Override
  public String getUserId(String userToken) {
    BasicDBObject query = new BasicDBObject("token", userToken);
    Document user = userColl.find(query).first();
    return (String) getValueOrNull(user, "userId");
  }

  @Override
  public void insert_policy(String dsId, String userId, String policy) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Map<String, String> get_policy_uuids(String userId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String get_policy(String dsId, String userId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String get_ds_owner_token(String dsId) {
    // TODO Auto-generated method stub
    return null;
  }
  

  public static void main(String[] args) {
    AuthMetadataMongodb db = new AuthMetadataMongodb();
    String testUserId = "test_user_3748@metroinsight.io";
    String testUserToken = "abggGFiGMfhj,k16245a";
    String testDsId = "test_ds_7689";
    
    db.insertToken(testUserId, testUserToken);
    System.out.println("insert token success");

    String givenUser = db.getUserId(testUserToken);
    assert givenUser.equals(testUserId);
    System.out.println("receive user id success");

    String givenToken = db.getToken(testUserId);
    assert givenToken.equals(testUserToken);
    System.out.println("receive token success");
    
    db.insertDsOwner(testDsId, testUserId);
    System.out.println("insert owner success");

    String ownerId = db.getDsOwnerId(testDsId);
    assert ownerId.equals(testUserId);
    System.out.println("receive owner id success");
    
    String res = db.getToken("gahkljgaklga");
    assert res == null;
    System.out.println("void user success");
    System.out.println("TEST SUCCESS");
  }
}
