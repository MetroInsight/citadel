package metroinsight.citadel.authorization;

import java.util.UUID;

public class UserTokenManager {

	public static String generateToken(String email)
	{
		String token = UUID.randomUUID().toString();//"115239272283116371008";
		return token;
	}//end generateToken()
	
}//end class UserTokenManager
