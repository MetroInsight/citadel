package metroinsight.citadel.authorization;

import java.util.UUID;

import org.locationtech.geomesa.hbase.data.HBaseMetadataAdapter;



public class UserTokenManager {

	static Authorization_MetaData hmetadata=new Authorization_MetaData();
	
	public static void initialize()
	{
		//System.out.println("initialize() called");
		if(hmetadata.connection==null)
		{
			hmetadata.create_connection();
			
			//called only during once when connection is null
			hmetadata.create_table();//will create table only if not already exist.
			
			
		}//end if
	}//end initialize()
	
	public static String generateToken(String email)
	{
		String token=hmetadata.get_token(email);//email is the userID
		
		if(token.equals(""))//empty token, user is logging first time.
		{
			System.out.println("Creating New Token");
			token=hmetadata.insert_token(email);
		}//end if
		
		return token;
	}//end generateToken()
	
}//end class UserTokenManager
