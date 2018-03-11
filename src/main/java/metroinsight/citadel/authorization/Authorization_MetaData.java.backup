package metroinsight.citadel.authorization;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/*
 * inserts user token into the HBase and query it and retrieves it at runtime.
 * links user token to UserID.
 * UserID is gmail if Google Oauth is used. 
 * Else it can Admin created userID (To-DO)
 * 
 */
public class Authorization_MetaData {

  // String
  String hbaseSitePath;

  public final String userToken = "userToken";
  static TableName table_meta;
  static String family_ds = "ds";
  static String family_user = "user";
  static String family_policy = "policy";// this family will be storing like: userid and policy with it,
  // Since userid is changing and growing or shrinking, we don't have a string
  // array below.
  String[] owner_qualifier = { "token", "userId" };
  String[] datastream_qualifier = { "ownerToken", "ownerId" };// owner qualifier is fixed, every user is stored as a
                                                              // separate userID qualifies with the dataStream.

  Connection connection = null;
  Table table = null;

  public Authorization_MetaData(String hbaseSitePath) {
    Buffer confBuffer = Vertx.vertx().fileSystem().readFileBlocking("./src/main/resources/conf/citadel-conf.json");
    JsonObject configs = new JsonObject(confBuffer);
    String tableName = configs.getString("auth.hbase.tablename");
    table_meta = TableName.valueOf(tableName);
    this.hbaseSitePath = hbaseSitePath;
    create_connection();
    create_table();
  }

  void create_connection() {
    try {

      // System.out.println("Create_connection called");

      Configuration config = HBaseConfiguration.create();
      config.addResource(new Path(hbaseSitePath));
      HBaseAdmin.checkHBaseAvailable(config);
      connection = ConnectionFactory.createConnection(config);

      // System.out.println("Create_connection done");

    } // end try
    catch (Exception e) {
      e.printStackTrace();
    } // end catch

  }// end create connection

  /*
   * creates the required table structure in HBase
   */
  void create_table() {
    try {
      Admin admin = connection.getAdmin();
      HTableDescriptor desc = new HTableDescriptor(table_meta);
      desc.addFamily(new HColumnDescriptor(family_ds));
      desc.addFamily(new HColumnDescriptor(family_user));
      desc.addFamily(new HColumnDescriptor(family_policy));
      if (!admin.tableExists(table_meta)) {
        admin.createTable(desc);
        System.out.println("Table created");
      } else {
        System.out.println("Table exists");
      }

      table = connection.getTable(table_meta);

    } // end try

    catch (Exception e) {
      e.printStackTrace();
    }
  }// end create_table

  /*
   * Given a userID as input, returns backs the token if it exists
   */
  public String get_token(String userID) {
    String token = "";
    try {
      byte[] row_id = Bytes.toBytes(userID);
      Get g = new Get(row_id);
      Result r = table.get(g);
      if (r.containsColumn(family_user.getBytes(), Bytes.toBytes(owner_qualifier[0]))) {

        byte[] value = r.getValue(family_user.getBytes(), Bytes.toBytes(owner_qualifier[0]));
        token = Bytes.toString(value);
        // System.out.println("Token exits- Token is:"+token+" : Row-ID is
        // :"+r.toString());
        System.out.println("Token Exist");
        return token;
      } // end if
      else {
        System.out.println("Token doesn't exits:" + r.toString());
        // the token doesn't exist for this user. This user is not registered with us.
        return token;
      }
      // token = UUID.randomUUID().toString();//"115239272283116371008";
    } // end try

    catch (Exception e) {
      e.printStackTrace();
    }

    return token;

  }// end get_token()

  String insert_token(String userID, String token)// given a userID inserts token into the database
  { // should check before by other fxns to see id userID already doesn't exist in
    // DB,
    // else it will duplicate the ID

    try {

      // rowid is userID, family is token and value is token-value
      String rowid = userID;// UUID.randomUUID().toString();
      byte[] row_id = Bytes.toBytes(rowid);
      Put p = new Put(row_id);
      p.addColumn(family_user.getBytes(), Bytes.toBytes(owner_qualifier[0]), Bytes.toBytes(token));

      // rowid is token-value, family is userID and value is token-
      rowid = token;
      row_id = Bytes.toBytes(rowid);
      Put p2 = new Put(row_id);
      p2.addColumn(family_user.getBytes(), Bytes.toBytes(owner_qualifier[1]), Bytes.toBytes(userID));
      table.put(p);
      table.put(p2);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return token;
  }// end insert_token

  /*
   * MODEL:
   * rowID(dsId-value),Column(family_ds),qualifier(ownerToken),(token-value)
   * rowID(dsId-value),Column(family_ds),qualifier(ownerId),(ownerId-value)
   */
  // inserts datastream id and ownerToken details into the metadata table
  public void insert_ds_owner(String dsId, String ownerToken, String ownerId) {
    try {
      // rowid is dsId, family is ds, qualifier is ownerToken and value is
      // owner-token-value
      String rowid = dsId;
      byte[] row_id = Bytes.toBytes(rowid);
      Put p = new Put(row_id);
      p.addColumn(family_ds.getBytes(), Bytes.toBytes(datastream_qualifier[0]), Bytes.toBytes(ownerToken));
      table.put(p);

      // rowid is dsId, family is ds, qualifier is ownerId and value is ownerId-value
      Put p2 = new Put(row_id);
      p2.addColumn(family_ds.getBytes(), Bytes.toBytes(datastream_qualifier[1]), Bytes.toBytes(ownerId));
      table.put(p2);

    } // end try
    catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("In Authorization_MetaData insert_ds_owner");
  }// end insert_ds_owner

  /*
   * MODEL:rowid (dsId-value),Column(Policy), qualifier(userId-value),
   * (Policy-Value)
   * 
   */
  public void insert_policy(String dsId, String userId, String policy) {
    try {
      // rowid is dsId, family is ds, qualifier(userId-value), (Policy-Value)
      String rowid = dsId;
      byte[] row_id = Bytes.toBytes(rowid);
      Put p = new Put(row_id);
      p.addColumn(family_policy.getBytes(), Bytes.toBytes(userId), Bytes.toBytes(policy));
      table.put(p);

    } // end try
    catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("In Authorization_MetaData insert_policy");

  }// end insert_policy()
  
  /*
   * MODEL:rowid (dsId-value),Column(Policy), qualifier(userId-value),
   * (Policy-Value)
   */
  public Map<String, String> get_policy_uuids(String userId) {
    try {
      Map<String, String> policies = new HashMap<String, String>();
      Scan scan = new Scan();
      // Scanning the required columns
      scan.addColumn(Bytes.toBytes(family_policy), Bytes.toBytes(userId));
      // Getting the scan result
      ResultScanner scanner = table.getScanner(scan);

      // Reading values from scan result
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        String DsId = Bytes.toString(result.getRow());
        String policy = Bytes.toString(result.getValue(Bytes.toBytes(family_policy), Bytes.toBytes(userId)));
        System.out.println("DsId :" + DsId + " : " + policy); // TODO: Needs to be handled by a logger.
        policies.put(DsId, policy);
      }
      // closing the scanner
      scanner.close();
      return policies;
    } // end try
    catch (Exception e) {
      e.printStackTrace(); // TODO: IMPORTANT: This needs to be hanlded properly.
      return null; 
    }
  }

  /*
   * MODEL:rowid (dsId-value),Column(Policy), qualifier(userId-value),
   * (Policy-Value)
   */
  public String get_policy(String dsId, String userId) {
    String policy = "";

    try {
      byte[] row_id = Bytes.toBytes(dsId);
      Get g = new Get(row_id);
      Result r = table.get(g);
      if (r.containsColumn(family_policy.getBytes(), Bytes.toBytes(userId))) {

        byte[] value = r.getValue(family_policy.getBytes(), Bytes.toBytes(userId));
        policy = Bytes.toString(value);
        // System.out.println("Token exits- Token is:"+token+" : Row-ID is
        // :"+r.toString());
        System.out.println("User Policy Exists: " + policy);
        return policy;
      } // end if
      else {
        System.out.println("User Policy Exists: " + r.toString());
        // the token doesn't exist for this user. This user is not registered with us.
        return policy;
      }

    } // end try

    catch (Exception e) {
      e.printStackTrace();
    }

    return policy;
  }// end get_policy

  /*
   * input is: dsID return: Token of the Owner of dsId
   */
  public String get_ds_owner_token(String dsId) {
    String token = "";
    try {
      byte[] row_id = Bytes.toBytes(dsId);
      Get g = new Get(row_id);
      Result r = table.get(g);
      if (r.containsColumn(family_ds.getBytes(), Bytes.toBytes(datastream_qualifier[0]))) {

        byte[] value = r.getValue(family_ds.getBytes(), Bytes.toBytes(datastream_qualifier[0]));
        token = Bytes.toString(value);
        // System.out.println("Token exits- Token is:"+token+" : Row-ID is
        // :"+r.toString());
        System.out.println("Owner Token Exist: " + token);
        return token;
      } // end if
      else {
        System.out.println("Owner Token doesn't exits: " + r.toString());
        // the token doesn't exist for this user. This user is not registered with us.
        return token;
      }

    } // end try

    catch (Exception e) {
      e.printStackTrace();
    }

    return token;
  }

  public String get_ds_owner_id(String dsId) {
    String ownerId = "";
    try {
      byte[] row_id = Bytes.toBytes(dsId);
      Get g = new Get(row_id);
      Result r = table.get(g);
      if (r.containsColumn(family_ds.getBytes(), Bytes.toBytes(datastream_qualifier[1]))) {

        byte[] value = r.getValue(family_ds.getBytes(), Bytes.toBytes(datastream_qualifier[1]));
        ownerId = Bytes.toString(value);
        // System.out.println("Token exits- Token is:"+token+" : Row-ID is
        // :"+r.toString());
        System.out.println("OwnerId Exist: " + ownerId);
        return ownerId;
      } // end if
      else {
        System.out.println("OwnerId doesn't exits: " + r.toString());
        // the token doesn't exist for this user. This user is not registered with us.
        return ownerId;
      }

    } // end try

    catch (Exception e) {
      e.printStackTrace();
    }

    return ownerId;
  }

  /*
   * given a userToken return the userID if it exists in table
   */
  public String get_userID(String userToken) {
    String userID = "";

    try {
      byte[] row_id = Bytes.toBytes(userToken);
      Get g = new Get(row_id);
      Result r = table.get(g);
      if (r.containsColumn(family_user.getBytes(), Bytes.toBytes(owner_qualifier[1]))) {

        byte[] value = r.getValue(family_user.getBytes(), Bytes.toBytes(owner_qualifier[1]));
        userID = Bytes.toString(value);
        // System.out.println("Token exits- Token is:"+token+" : Row-ID is
        // :"+r.toString());
        System.out.println("userId Exist: " + userID);
        return userID;
      } // end if
      else {
        System.out.println("userID doesn't exits:" + r.toString());
        // the token doesn't exist for this user. This user is not registered with us.
        return userID;
      }

    } // end try

    catch (Exception e) {
      e.printStackTrace();
    }

    return userID;

  }// end get_userID

  public static void main(String[] args) {

    /*
     * creates the tables, should be run first.
     */

    String hbaseSitePath = "/home/jbkoh/repo/citadel/conf/hbase-site.xml";
    Authorization_MetaData met = new Authorization_MetaData(hbaseSitePath);

    // String token="";
    // token=met.insert_token("sand.iitr@gmail.com");
    // System.out.println("Token Inserted is:"+token);
    // token=met.get_token("sand.iitr@gmail.com");
    // System.out.println("Token Queried is:"+token);
    /*
     * Printing the uuids for a particular user
     */
    System.out.println(met.get_policy_uuids("citadel.ucla@gmail.com"));

  }// end main

}// end class Authorization_MetaData
