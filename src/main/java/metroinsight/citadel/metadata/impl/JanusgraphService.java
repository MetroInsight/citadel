package metroinsight.citadel.metadata.impl;

import java.util.HashMap;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.schema.JanusGraphManagement;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;

//Currently deprecated
public class JanusgraphService {
//  private final Vertx vertx;
  static JanusGraph graph;
  //static Client client;
  static GraphTraversalSource trav;
  static HashMap pointMap = new HashMap();
  
  private void InitSchema() {
    // Parse point type csv.
  }
  
  public JanusgraphService() {//Vertx vertx) {
 //   this.vertx = vertx;
    graph = JanusGraphFactory.open("/home/jbkoh/tools/janusgraph-0.1.1-hadoop2/conf/janusgraph-hbase.properties");    
    trav = graph.traversal();
    JanusGraphManagement mgmt = graph.openManagement();
    mgmt.buildIndex("name", Vertex.class);
    mgmt.buildIndex("uuid", Vertex.class);
    mgmt.buildIndex("rdf:type", Vertex.class);
    mgmt.commit();
    /*graph = JanusGraphFactory.build().
        set("storage.backend", "berkeleyje").
        set("storage.directory", "/data/graph").
        open();
        */
    /*
    try {
      cluster = Cluster.open("/home/jbkoh/repo/citadel/src/main/resources/configs/remote.yaml");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    client = cluster.connect();
    // Init schema file here
     */
  }
  
//  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  }

 // @Override
  public void getPoint(String uuid, Handler<AsyncResult<Metadata>> resultHandler) {

  }
  
  //@Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
    JanusGraphVertex pointType = graph.addVertex("name", jsonMetadata.getString("pointType"));
    String uuid = UUID.randomUUID().toString();
    Vertex v = graph.addVertex(
        "name", jsonMetadata.getString("name"),
        "unit", jsonMetadata.getString("unit"),
        "uuid", jsonMetadata.getString("uuid"));
    v.addEdge("rdf:type", pointType);
  }

//  @Override
  public void upsertMetadata(String uuid, JsonObject newMetadata, Handler<AsyncResult<Void>> rh) {
    assert false;
  }

//  @Override
  public void appendMetadata(String uuid, JsonObject newMetadata, Handler<AsyncResult<Void>> rh) {
    assert false;
  }

  public static void main(String[] args) {	
    //client.submit("graph.addVertex(T.label,'x','name','tom')");
    new JanusgraphService();
    Vertex steph = trav.V().has("name", "stephen").next();
    JanusGraphVertex bbb = graph.addVertex("name", "Jason");
    bbb.addEdge("hasFriend", steph);
    GraphTraversal<Vertex, Vertex> allVertices = trav.V();
    System.out.println("Existing vertices are:");
    while (allVertices.hasNext()) {
      System.out.println(allVertices.next().value("name").toString());
    }
    
    // Finding somebody who is a friend with Stephen
    System.out.println("\n");

    long beginTime = System.currentTimeMillis();
    Vertex vvv = trav.V(steph).inE("hasFriend").V().next();
    long endTime = System.currentTimeMillis();
    System.out.println(vvv.value("name").toString());
    System.out.println(String.format("Took %s ms", String.valueOf(endTime - beginTime)));

    System.out.println("\n");
    beginTime = System.currentTimeMillis();
    vvv = trav.V().outE("hasFriend").V().next();
    endTime = System.currentTimeMillis();
    System.out.println(vvv.value("name").toString());
    System.out.println(String.format("Took %s ms", String.valueOf(endTime - beginTime)));

  }

}
