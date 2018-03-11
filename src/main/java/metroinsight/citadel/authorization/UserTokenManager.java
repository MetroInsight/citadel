package metroinsight.citadel.authorization;

import java.util.Base64;
import java.util.UUID;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.authorization.authmetadata.AuthorizationMetadata;
import metroinsight.citadel.authorization.authmetadata.impl.AuthMetadataMongodb;

public class UserTokenManager {

  AuthorizationMetadata authMetadata;
  byte[] keyJWT = null;
  SecretKey secretKeyJWT = null;
  JsonObject configs;

  public UserTokenManager(JsonObject importedConfigs) {
    configs = importedConfigs;
    authMetadata = new AuthMetadataMongodb();
    if (keyJWT == null) {
      keyJWT = Base64.getDecoder().decode("E1ECdGKpx5elfSMyoFLzlQ==");/* Loading from the conf file */
      secretKeyJWT = new SecretKeySpec(keyJWT, 0, keyJWT.length, "AES");
    }
  }

  private String generateToken(String userId) {
    String token = getJWT();
    authMetadata.insertToken(userId, token);
    return token;
  }
  
  public String getToken(String email) {
    String token = authMetadata.getToken(email);
    if (token == null) {
      token = generateToken(email);
    }
    return token;
  }

  private String getJWT() {
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
