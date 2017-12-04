package metroinsight.citadel.policy;

import java.util.ArrayList;

import metroinsight.citadel.authorization.Authorization_MetaData;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class PolicyManagement {

	Authorization_MetaData Auth_meta_data_policy; //Authorization_MetaData class contains functions of authorization and access control
	
	public PolicyManagement()
	{
		Auth_meta_data_policy=new Authorization_MetaData();
	}
	
	/*
	 * Register a Policy
	 * {Token:ownerToken, What:[DS_ID], Whom [UserIDs]}
	 */
	public void registerPolicy(RoutingContext rc) {
		
		System.out.println("In registerPolicy PolicyManagement.java");
	    JsonObject body = rc.getBodyAsJson();
	    System.out.println("Post: "+body);
	    
	    if(body.containsKey("userToken")&&body.containsKey("what")&&body.containsKey("whom"))
	    {
	    	
	    	// token of the owner
	    	String userToken = body.getString("userToken");
	    	//verify the DSIDs in the What are actually owned by the owner
	    	JsonArray what = body.getJsonArray("what");
	    	
	    	JsonArray whom = body.getJsonArray("whom");
	    	
	    	if(what.size()==0||whom.size()==0)//if no ds_id is passed owner is not verified
	    	{
	    		System.out.println("In RegisterPolicy: What or Whom is empty, which is not allowed");
	    		return;
	    	}
	    	
	    	boolean owner_verified=true;
	    	
	    	for(int i=0;i<what.size();i++)
	    	{
	    		String ds_id=what.getString(i);//ds_id==uuid
	    		if(ds_id.equals(""))//it is empty, can check with the size of it
	    		{
	    			System.out.println("dsId cannot be empty");
	    			owner_verified=false; //if one of the datastream is not owner's datastream we don't proceed ahead, May be change this behavior in future
	    			System.out.println("uuid cannot be empty.");
	    			return;
	    			//break;
	    		}
	    		//System.out.println(ds_id);
	    		String ownerToken=Auth_meta_data_policy.get_ds_owner_token(ds_id);//this means ds_id exists and token also exists, else ownerToken=""	    		
	    		
	    		if(!ownerToken.equals(userToken))
	    			{
	    			 owner_verified=false; //if one of the datastream is not owner's datastream we don't proceed ahead, May be change this behavior in future
	    			 System.out.println("Either uuid:"+ds_id+", doesn't exist or you don't have privelegs to assign policy to it.");
	    			 return;
	    			 //break;
	    			}
	    		
	    	}//end for(int i=0;i<what.size();i++)
	    	
	    	ArrayList<String> userTokenList=new ArrayList<String>();//Creating arraylist  
	    	
	    	if(owner_verified)
	    	{
	    		/*
	    		 * owner is verified. Means for all the DS_ID, ownertoken is valid
	    		 * Now: go to the whom construct, and check, if these users exist in Citadel
	    		 */
	    	    boolean users_verified=true;
	    	    
	    		for(int i=0;i<whom.size();i++)
	    		{
	    			String userId =whom.getString(i);
	    			if(userId.equals(""))
	    		     {
	    				System.out.println("userId cannot be empty");
	    				users_verified=false; 
	    				return;
	    				//break;
	    		     }
	    			 
	    			//check is userId exist in Citadel
	    			//given a userId check if its token exist in the Citadel
	    			String user_token=Auth_meta_data_policy.get_token(userId);
	    			
	    			if(user_token.equals(""))
	    			{
	    				System.out.println("userID: "+userId+" doesn't exist in Citadel");
	    				users_verified=false; 
	    				return;
	    				//break;
	    				
	    			}//end if(user_token.equals(""))
	    			
	    			userTokenList.add(user_token);//storing these tokens for later use
	    			
	    		}//end for(int i=0;i<whom.size();i++)
	    		
	    		/*
	    		 * All users were verified correctly, and exist in Citadel
	    		 */
	    		if(users_verified)
	    		{
	    			
	    			
	    		   System.out.println("In registerPolicy, owner verified PolicyManagement.java");
	 	    	   JsonObject result = new JsonObject();
	 	     	   result.put("result", "SUCCESS");
	 	     	   String length = Integer.toString(result.toString().length());
	 	     		rc.response()
	 	     		  .putHeader("content-TYPE", "application/text; charset=utf=8")
	 	     		  .putHeader("content-length",  length)
	 	     		  .setStatusCode(201)
	 	     		  .write(result.toString());
	 	     		
	    		}//end if(users_verified)
	    	   
	     		
	    	}//if(owner_verified)
	    	else
	    	{
	    		System.out.println("You don't have priveleges to assign policies to all the dataStreams in what construct");
	    		return;
	    	}
	    	
	    }//end if(body.containsKey("userToken")&&body.containsKey("what")&&body.containsKey("whom"))
	    else
	    {
	    	System.out.println("Policy registration parameters are missing");
	    	return;
	    }
	    
	    
     		
		
	}//end registerPolicy
	
}//end PolicyManagement
