package metroinsight.citadel.authorization.authmetadata;

import java.util.Map;

public interface AuthorizationMetadata {
  public String getToken(String userId);
  public void insertToken(String userId, String token);
  public void insertDsOwner(String dsId, String ownerId);
  public String getDsOwnerId(String dsId);
  public String getUserId(String userToken); // TODO: This seems to be a problematic function

  // TODO: Below no needs to implement
  public void insert_policy(String dsId, String userId, String policy);
  public Map<String, String> get_policy_uuids(String userId);
  public String get_policy(String dsId, String userId);
  public String get_ds_owner_token(String dsId);
}
