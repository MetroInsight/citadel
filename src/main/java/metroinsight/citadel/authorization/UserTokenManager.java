package metroinsight.citadel.authorization;

import java.util.Base64;
import java.util.UUID;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.vertx.core.json.JsonObject;

public class UserTokenManager {

  //static Authorization_MetaData hmetadata = new Authorization_MetaData();
  static Authorization_MetaData hmetadata = null;
  static byte[] keyJWT = null;
  static SecretKey secretKeyJWT = null;
  static JsonObject configs;

  public static void initialize(JsonObject importedConfigs) {
    configs = importedConfigs;
    
    hmetadata = new Authorization_MetaData(configs.getString("auth.hbase.sitefile"));

    // System.out.println("initialize() called");
    if (hmetadata.connection == null) {
      hmetadata.create_connection();
      // called only during once when connection is null
      hmetadata.create_table();// will create table only if not already exist.
    } // end if
    if (keyJWT == null) {
      keyJWT = Base64.getDecoder().decode("E1ECdGKpx5elfSMyoFLzlQ==");/* Loading from the conf file */
      secretKeyJWT = new SecretKeySpec(keyJWT, 0, keyJWT.length, "AES");
    } // end if

  }// end initialize()

  public static String generateToken(String email) {
    String token = hmetadata.get_token(email);// email is the userID

    if (token.equals(""))// empty token, user is logging first time.
    {
      token = getJWT();// generate a unique JWT token for user

      System.out.println("Creating New Token");
      token = hmetadata.insert_token(email, token);
    } // end if

    return token;
  }// end generateToken()

  public static String getJWT() {
    String id = UUID.randomUUID().toString();
    String compactJws = Jwts.builder()
        // .setSubject("sub")/*Use in future*/
        // .setAudience("email")/*Use in future*/
        .setId(id)/* Ensures every JWT is unique */
        // .setIssuedAt(d)/* in future may be add date*/
        .signWith(SignatureAlgorithm.HS256, secretKeyJWT).compact();

    return compactJws;
  }

}// end class UserTokenManager
