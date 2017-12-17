package metroinsight.citadel.policy;

import java.util.ArrayList;

import metroinsight.citadel.authorization.Authorization_MetaData;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.model.BaseContent;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class PolicyRestApi extends RestApiTemplate{

	Authorization_MetaData Auth_meta_data_policy; //Authorization_MetaData class contains functions of authorization and access control
	
	public PolicyRestApi()
	{
		Auth_meta_data_policy=new Authorization_MetaData();
	}
	
	/*
	 * Register a Policy
	 * {Token:ownerToken, What:[DS_ID], Whom [UserIDs]}
	 */
	public void registerPolicy(RoutingContext rc) {
		
		HttpServerResponse resp = getDefaultResponse(rc);
		BaseContent content = new BaseContent();
		
		System.out.println("In registerPolicy PolicyRestApi.java");
	    JsonObject body = rc.getBodyAsJson();
	    System.out.println("Post: "+body);
	    
	    try
	    {
	    	
	    	
	    if(body.containsKey("userToken")&&body.containsKey("policy"))
	    {
	    	
	    	// token of the owner
	    	String userToken = body.getString("userToken");
	    	JsonObject policy = body.getJsonObject("policy");
	    	
	    	System.out.println("In registerPolicy Policy is:"+policy);
	    	
	    	//verify the DSIDs in the What are actually owned by the owner
	    	JsonArray sensors = policy.getJsonArray("sensors");
	    	
	    	JsonArray users = policy.getJsonArray("users");
	    	
	    	if(sensors.size()==0||users.size()==0)//if no ds_id is passed owner is not verified
	    	{
	    		System.out.println("In RegisterPolicy: What or Whom is empty, which is not allowed");
	    		sendErrorResponse(resp, 400, "Parameters are missing");
	    	}
	    	
	    	String ownerId = Auth_meta_data_policy.get_userID(userToken);
	    	if(ownerId.equals(""))
    		{
    			System.out.println("Token is not valid");
    			sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
    		}
	    	
	    	boolean owner_verified=true;//it is true, if for all DSID in what construct are owner by ownerId
	    	
	    	for(int i=0;i<sensors.size();i++)
	    	{
	    		String ds_id=sensors.getString(i);//ds_id==uuid
	    		if(ds_id.equals(""))//it is empty, can check with the size of it also later: TODO
	    		{
	    			System.out.println("dsId cannot be empty");
	    			owner_verified=false; //if one of the datastream is not owner's datastream we don't proceed ahead, May be change this behavior in future
	    			System.out.println("uuid cannot be empty.");
	    			sendErrorResponse(resp, 400, "Parameters are missing");
	    			//break;
	    		}
	    		
	    		//Given the DsID fetch the ownerId
	    		String ownerId_ds=Auth_meta_data_policy.get_ds_owner_id(ds_id);
	    		
	    		if(ownerId_ds.equals("")||!ownerId_ds.equals(ownerId))
	    			{
	    			 owner_verified=false; //if one of the datastream is not owner's datastream we don't proceed ahead, May be change this behavior in future
	    			 System.out.println("Either uuid:"+ds_id+", doesn't exist or you don't have privelegs to assign policy to it.");
	    			 sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
	    			 //break;
	    			}
	    		
	    	}//end for(int i=0;i<what.size();i++)
	    	
	    	ArrayList<String> userIdList=new ArrayList<String>();//Creating arraylist of users for which we have to register policy
	    	
	    	if(owner_verified)
	    	{
	    		System.out.println("In registerPolicy, owner verified PolicyManagement.java");
	    		
	    		/*
	    		 * owner is verified. Means for all the DS_ID, ownerId is valid
	    		 * Now: go to the whom construct, and check, if these users exist in Citadel
	    		 */
	    	    boolean users_verified=true;
	    	    
	    		for(int i=0;i<users.size();i++)
	    		{
	    			String userId =users.getString(i);
	    			if(userId.equals(""))
	    		     {
	    				System.out.println("userId cannot be empty");
	    				users_verified=false; 
	    				sendErrorResponse(resp, 400, "Parameters are missing");	
	    				//break;
	    		     }
	    			 
	    			//check is userId exist in Citadel
	    			//given a userId check if its token exist in the Citadel
	    			String user_token=Auth_meta_data_policy.get_token(userId);
	    			
	    			if(user_token.equals(""))
	    			{
	    				System.out.println("userID: "+userId+" doesn't exist in Citadel");
	    				users_verified=false; 
	    				sendErrorResponse(resp, 400, users.getString(i)+" : doesn't exist in Citadel");	
	    				
	    					//break;
	    				
	    			}//end if(user_token.equals(""))
	    			
	    			userIdList.add(user_token);//storing these tokens for later use
	    			
	    		}//end for(int i=0;i<whom.size();i++)
	    		
	    		/*
	    		 * All users were verified correctly, and exist in Citadel
	    		 */
	    		if(users_verified)
	    		{
	    			
	    		   System.out.println("In registerPolicy, users are verified, PolicyManagement.java");
	    		   
	    		   //for all usersIds and all DsId's insert the policy
	    		   for(int i=0;i<sensors.size();i++)
	    		   {
	    			   for(int j=0;j<users.size();j++)
	    			   {
	    				   String dsId=sensors.getString(i);
	    				   String userId=users.getString(j);
	    				   
	    				   Auth_meta_data_policy.insert_policy(dsId, userId, "true");//this is default policy with no-space time constraints, with constraints we need to update this
	    			   }
	    			   
	    		   }//end for
	    		   
	    		   System.out.println("In registerPolicy, Policies are registered, PolicyManagement.java");
	    		   
	    		   /*
	 	    	   JsonObject result = new JsonObject();
	 	     	   result.put("result", "SUCCESS");
	 	     	   String length = Integer.toString(result.toString().length());
	 	     		rc.response()
	 	     		  .putHeader("content-TYPE", "application/text; charset=utf=8")
	 	     		  .putHeader("content-length",  length)
	 	     		  .setStatusCode(201)
	 	     		  .write(result.toString());
	 	     		 */
	    		   resp.setStatusCode(200);
		    	   content.setSucceess(true);
		    	   String cStr = content.toString();
 		    	   String cLen = Integer.toString(cStr.length());
 		    	   resp.putHeader("content-length", cLen)
 		      	   .write(cStr); 
	 	     		
	    		}//end if(users_verified)
	    	   
	     		
	    	}//if(owner_verified)
	    	else
	    	{
	    		System.out.println("You don't have priveleges to assign policies to all the dataStreams in what construct");
	    		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
	    	}
	    	
	    }//end if(body.containsKey("userToken")&&body.containsKey("what")&&body.containsKey("whom"))
	    else
	    {
	    	System.out.println("Policy registration parameters are missing");
	    	sendErrorResponse(resp, 400, "Parameters are missing");
	    }
	    
	    }//end try
	    catch(Exception e)
	    {
	    	e.printStackTrace();
	    	sendErrorResponse(resp, 400, "Internal server error occured, please contact developers.");
	    }
     		
		
	}//end registerPolicy
	
}//end PolicyManagement
