package metroinsight.citadel.metadata.impl;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import metroinsight.citadel.common.ErrorMessages;
import metroinsight.citadel.metadata.MetadataService;
import virtuoso.rdf4j.driver.VirtuosoRepository;

public class VirtuosoRdf4jService implements MetadataService{
  private final Vertx vertx;
  private final ServiceDiscovery discovery;
  ValueFactory factory;
  RepositoryConnection conn;

  // Prefixes
  final String CITADEL = "http://metroinsight.io/citadel#";
  final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  final String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
  final String EX = "http://example.com#";
  final String BIF = "http://www.openlinksw.com/schema/sparql/extensions#";
  String queryPrefix;

  Map<String, IRI> propertyMap;
  Map<String, String> valPropMap;
  // TODO: Maybe add a map between uesr-prop to rdf property. e.g., "pointType" ->
  // rdf:type.

  // Common Variables
  IRI a;
  IRI hasUnit;
  IRI hasName;
  IRI context;

  // Units
  List<String> units;
  List<String> types;
  String graphname;

  public VirtuosoRdf4jService(Vertx vertx, String virtHostname, Integer virtPort, String graphname, String username,
      String password, ServiceDiscovery discovery) {
    String url = String.format("jdbc:virtuoso://%s:%d/log_enable=0", virtHostname, virtPort);
    VirtuosoRepository repository = new VirtuosoRepository(url, username, password);
    factory = repository.getValueFactory();
    conn = repository.getConnection();
    context = repository.getValueFactory().createIRI(graphname);
    this.graphname = graphname;
    this.vertx = vertx;
    this.discovery = discovery;
    initSchema();
  }

  private void initSchema() {
    // Init prefixes for SPARQL
    queryPrefix = String.format("PREFIX citadel: <%s>\n", CITADEL) +
                  String.format("PREFIX rdf: <%s>\n", RDF) +
                  String.format("PREFIX rdfs: <%s>\n", RDFS) +
                  String.format("PREFIX ex: <%s>\n", EX) +
                  "PREFIX bif: <bif:>\n" + 
                  "PREFIX xsd: <xsd:>\n";
    
    // Init common variables
    units = new ArrayList<String>();
    types = new ArrayList<String>();

    a = factory.createIRI(RDF, "type");
    hasUnit = factory.createIRI(CITADEL, "unit");
    hasName = factory.createIRI(CITADEL, "name");

    // Init Namespace Map
    // This may be automated once we get a schema file (in Turtle).
    propertyMap = new HashMap<String, IRI>();
    propertyMap.put("pointType", factory.createIRI(RDF, "type"));
    propertyMap.put("subClassOf", factory.createIRI(RDFS, "subClassOf"));
    propertyMap.put("unit", factory.createIRI(CITADEL, "unit"));
    propertyMap.put("owner", factory.createIRI(CITADEL, "owner"));
    propertyMap.put("name", factory.createIRI(CITADEL, "name"));
    valPropMap = new HashMap<String, String>();
    valPropMap.put("name", "string");
    valPropMap.put("unit", "citadel");
    valPropMap.put("pointType", "citadel");

    // Init units
    // types
    List<String> citadelTypes = new ArrayList<String>(units);
    citadelTypes.addAll(types);
    for (int i = 0; i < citadelTypes.size(); i++) {
      String v = citadelTypes.get(i);
      propertyMap.put(v, factory.createIRI(CITADEL, v));
    }
  }

  private Value withPrefixValue(String prop, String id) {
    if (valPropMap.containsKey(prop)) {
      String valType = valPropMap.get(prop);
      if (valType.equals("string")) {
        return factory.createLiteral(id);
      } else if (valType.equals("citadel")) {
        return factory.createIRI(CITADEL, id);
      } else {
        System.out.println("TODO: Unknown value type");
        return factory.createIRI(EX, id);
      }
    } else {
      return factory.createLiteral(id);
    }
  }

  private IRI withPrefixProp(String id) {
    return propertyMap.getOrDefault(id, factory.createIRI(EX, id));
  }

  // public static final String VIRTUOSO_INSTANCE = "mc64";
  public static final String VIRTUOSO_INSTANCE = "localhost";
  public static final int VIRTUOSO_PORT = 1111;
  public static final String VIRTUOSO_USERNAME = "dba";
  public static final String VIRTUOSO_PASSWORD = "dba";

  public static void log(String mess) {
    System.out.println("   " + mess);
  }

  TupleQueryResult doTupleQuery(String query) throws Exception {
    try {
      TupleQuery resultsTable = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
      resultsTable.setIncludeInferred(false);
      TupleQueryResult bindings = resultsTable.evaluate();
      return bindings;
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  static void exec_query(Statement st, String query) throws Exception {
    String s = trimStr(query);
    if (s.length() > 0) {
      if (s.charAt(s.length() - 1) == ';') {
        s = s.substring(0, s.length() - 1);
      }
      st.execute(s);
    }

  }

  static String trimStr(String s) {
    int last = s.length() - 1;
    for (int i = last; i >= 0 && Character.isWhitespace(s.charAt(i)); i--) {
    }
    return s.substring(0, last).trim();
  }

  private JsonArray findByStringValue(String tag, String value) throws Exception {
    try {
      if (value.startsWith("\"") && value.endsWith("\"")) {
        value = value.substring(1, value.length() - 1);
      }
      String qStr = queryPrefix + 
                    "SELECT ?s FROM <" + context + "> WHERE {\n" +
                    String.format("?s citadel:%s ?o . ?o bif:contains \"'%s'\". }\n", tag, value);
      TupleQueryResult res = doTupleQuery(qStr);
      JsonArray results = new JsonArray();
      while (res.hasNext()) {
        BindingSet tup = res.next();
        Binding aaa = tup.getBinding("s");
        results.add(aaa.toString());
      }
      return results;
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
    try {
      long totalStartTime = System.nanoTime();
      if (jsonMetadata.getString("name").contains(" ")) {
        throw new Exception("Empty space is not allowed in name.");
      }
      else if (jsonMetadata.getString("unit").contains(" ")) {
        throw new Exception("Empty space is not allowed in unit.");
      }
      // Check if the name already exists
      String nameStr = jsonMetadata.getString("name");
      /*
      ParameterizedSparqlString pss = getDefaultPss();
      pss.setCommandText("select ?s where {?s citadel:name ?name .}");
      pss.setParam("name", name);
      ResultSet res = sparqlQuery(pss.toString());
      */
      long startTime = System.nanoTime();
      Value name = withPrefixValue("name", nameStr); // TODO: Change name to Literal later
      //ResultSet res = findByStringValue("name", nameStr);
      JsonArray res = findByStringValue("name", name.stringValue());
      long endTime = System.nanoTime();
      System.out.println(String.format("Name check time: %f", ((float)endTime - (float)startTime)/1000000));
      //Node name = withPrefixValue("name", nameStr); // TODO: Change name to Literal later
      if (res.size() > 0) {
        resultHandler.handle(Future.failedFuture(ErrorMessages.EXISTING_POINT_NAME));
      } else {
        // Create the point
        startTime = System.nanoTime();
        String uuid = jsonMetadata.getString("uuid");//UUID.randomUUID().toString();
        IRI point = factory.createIRI(EX, uuid);
        Value pointType = withPrefixValue("pointType", jsonMetadata.getString("pointType"));
        conn.add(point, a, pointType);
        Value unit = withPrefixValue("unit", jsonMetadata.getString("unit"));
        conn.add(point,  hasUnit, unit, context);
        conn.add(point,  hasName, name, context);
        Iterator<String> tagIter = jsonMetadata.fieldNames().iterator();
        while (tagIter.hasNext()) {
          String tag = tagIter.next();
          String value = jsonMetadata.getString(tag);
          // Check if tag corresponds to predefines properties which should be handeled above.
          if (!tag.equals("unit") && !tag.equals("name") && !tag.equals("uuid") && !tag.equals("pointType")) {
            conn.add(point, withPrefixProp(tag), withPrefixValue(tag, value), context);
          }
        }
        endTime = System.nanoTime();
        System.out.println(String.format("Virtuoso creation time: %f", ((float)endTime - (float)startTime)/1000000));
        long totalEndTime = System.nanoTime();
        System.out.println(String.format("Virtuoso createPoint time: %f", ((float)totalEndTime - (float)totalStartTime)/1000000));
        resultHandler.handle(Future.succeededFuture(uuid));
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getPoint(String uuid, Handler<AsyncResult<JsonObject>> resultHandler) {
    try {
      String qStr = queryPrefix + 
                    String.format("SELECT ?p ?o WHERE {<%s> ?p ?o}", factory.createIRI(EX, uuid));
      TupleQueryResult results = doTupleQuery(qStr);
      if (!results.hasNext()) {
        resultHandler.handle(Future.failedFuture("Not existing UUID"));
      } else {
        JsonObject metadata = new JsonObject();
        while (results.hasNext()) {
          BindingSet result = results.next();
          String p = result.getValue("p").toString().split("#")[1];
          String o = result.getValue("o").toString().split("#")[1]; // TODO: If it is a string?
          if (p.equals("type")) {
            p = "pointType";
          }
          metadata.put(p, o);
        }
        metadata.put("uuid", uuid);
        resultHandler.handle(Future.succeededFuture(metadata));
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
    try {
      // Construct a SPARQL query.
      String qStr = queryPrefix + "SELECT ?s FROM <" + context + "> WHERE {\n";
      Iterator<String> fieldIter = query.fieldNames().iterator();
      while (fieldIter.hasNext()) {
        String tagStr = fieldIter.next();
        IRI tagNode = withPrefixProp(tagStr);
        Value valNode = withPrefixValue(tagStr, query.getString(tagStr));
        Boolean isIri = true;
        try {
          IRI aaa = (IRI) valNode;
        } catch (Exception e) {
          isIri = false;
        }
        if (isIri) {
          qStr += String.format("?s <%s> <%s> . \n", tagNode, valNode);
        } else {
          qStr += String.format("?s <%s> %s . \n", tagNode, valNode);
        }
      }
      qStr += "?s ?p ?o .\n}";
      TupleQueryResult results = doTupleQuery(qStr);
      JsonArray uuids = new JsonArray();
      while (results.hasNext()) {
        uuids.add(results.next().getValue("s").toString().split("#")[1]);
      }
      resultHandler.handle(Future.succeededFuture(uuids));
    }catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void upsertMetadata(String uuid, JsonObject newMetadata, Handler<AsyncResult<Void>> rh) {
    try {
      IRI point = factory.createIRI(EX, uuid);
      Iterator<String> keys = newMetadata.fieldNames().iterator();
      while (keys.hasNext()) {
        String key = keys.next();
        IRI prop = withPrefixProp(key);
        Object value = newMetadata.getString(key);
        if (value instanceof List) { // TODO: Check this is working. If not working, use try catch.
          Iterator<String> valueIter = ((List<String>) value).iterator();
          while (valueIter.hasNext()) {
            conn.add(point, prop, withPrefixProp(valueIter.next()), context);
          }
        } else if (value instanceof String) {
          conn.add(point, prop, withPrefixValue(key, (String) value));
        }
      }
      rh.handle(Future.succeededFuture());
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e));
    }
  }

  public static void main(String[] args) {
  }
}
