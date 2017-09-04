package metroinsight.citadel.metadata.impl;

import java.util.UUID;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

public class VirtuosoService implements MetadataService  {
//  private final Vertx vertx;
  //static Client client;
  static VirtGraph graph = null;
  
  // Prefixes
  final String CITADEL = "http://metroinsight.io/citadel#";
  final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  final String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
  
  // Common Variables
  Node a;
  Node hasUnit;
  
  private void InitSchema() {
    // Init common variables
    a = NodeFactory.createURI(RDF + "type");
    hasUnit = NodeFactory.createURI(CITADEL + "hasUnit");

  }
  
  public VirtuosoService() {//Vertx vertx) {
    if (graph==null) {
      graph = new VirtGraph("citadel", "jdbc:virtuoso://localhost:1111", "dba", "dba");
    }
  }
  
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  }

  @Override
  public void getPoint(String uuid, Handler<AsyncResult<Metadata>> resultHandler) {

  }
  
  @Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
    try {
      String uuid = UUID.randomUUID().toString();
      Node point = NodeFactory.createURI(CITADEL + uuid);
      Node pointType = NodeFactory.createURI(CITADEL + jsonMetadata.getString("pointType"));
      graph.add(new Triple(point, a, pointType));
      Node unit = NodeFactory.createURI(CITADEL + jsonMetadata.getString("unit"));
      graph.add(new Triple(point, hasUnit, unit));
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }
  
  private ResultSet sparqlQuery(String qStr) {
    Query sparql = QueryFactory.create("SELECT ?s ?p ?o WHERE {?s ?p ?o}");
    VirtuosoQueryExecution vqd = VirtuosoQueryExecutionFactory.create(sparql, graph);
    return vqd.execSelect();
  }
  
  public static void main(String[] args) {	
    //client.submit("graph.addVertex(T.label,'x','name','tom')");
    VirtuosoService vs = new VirtuosoService();
    
    // Test inserting
    Node citadel = NodeFactory.createURI("http://metroinsight.io/citadel/schema");
    Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    Node sch = NodeFactory.createURI("schema");
    graph.add(new Triple(citadel, a, sch));
    System.out.println(String.format("Entire triples in the graph: %s", graph.getCount()));
    
    // Test querying
    String qStr = "SELECT ?s ?p ?o WHERE {?s ?p ?o}";
    ResultSet results = vs.sparqlQuery(qStr);
    while (results.hasNext()) {
      QuerySolution result = results.nextSolution();
      RDFNode graphName = result.get("graph");
      RDFNode s = result.get("s");
      RDFNode p = result.get("p");
      RDFNode o = result.get("o");
      System.out.println(graphName + " { " + s + " " + p + " " + o + " . }");
    }
  }

}
