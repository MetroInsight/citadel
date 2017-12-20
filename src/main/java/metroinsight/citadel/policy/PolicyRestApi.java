package metroinsight.citadel.policy;

import java.util.ArrayList;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

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
		
		boolean proceed=true;
		
		try
	    {
			
		System.out.println("In registerPolicy PolicyRestApi.java");
	    JsonObject body = rc.getBodyAsJson();
	    System.out.println("Post: "+body);
	    	
	    	
	    if(proceed&&body.containsKey("userToken")&&body.containsKey("policy"))
	    {
	    	
	    	// token of the owner
	    	String userToken = body.getString("userToken");
	    	JsonObject policy = body.getJsonObject("policy");
	    	
	    	System.out.println("In registerPolicy Policy is:"+policy);
	    	
	    	//verify the DSIDs in the What are actually owned by the owner
	    	JsonArray sensors = policy.getJsonArray("sensors");
	    	
	    	JsonArray users = policy.getJsonArray("users");
	    	
	    	if(proceed&&(sensors.size()==0||users.size()==0))//if no ds_id is passed
	    	{
	    		System.out.println("In RegisterPolicy: sensors is empty, which is not allowed");
	    		sendErrorResponse(resp, 400, "Parameters are missing");
	    		proceed=false;
	    	}//end if(sensors.size()==0||users.size()==0)
	    	
	    	String ownerId="";
	    	
	    	System.out.println("userToken is:"+userToken);
	    	
	    	if(proceed&&!userToken.equals(""))
	    	ownerId = Auth_meta_data_policy.get_userID(userToken);
	    	
	    	
	    	if(proceed&&ownerId.equals(""))
    		{
    			System.out.println("Token is not valid");
    			sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
    			proceed=false;
    		}
	    	
	    	boolean owner_verified=true;//it is true, if for all DSID in what construct are owner by ownerId
	    	
	    	for(int i=0;proceed&&i<sensors.size();i++)
	    	{
	    		String ds_id=sensors.getString(i);//ds_id==uuid
	    		if(ds_id.equals(""))//it is empty, can check with the size of it also later: TODO
	    		{
	    			System.out.println("dsId cannot be empty");
	    			owner_verified=false; //if one of the datastream is not owner's datastream we don't proceed ahead, May be change this behavior in future
	    			System.out.println("uuid cannot be empty.");
	    			sendErrorResponse(resp, 400, "Parameters are missing");
	    			proceed=false;
	    		}
	    		
	    		
	    		//Given the DsID fetch the ownerId
	    		String ownerId_ds="";
	    		if(proceed)
	    		ownerId_ds=Auth_meta_data_policy.get_ds_owner_id(ds_id);
	    		
	    		//ownerId_ds.equals("") means ds_id is not a valid ds_id 
	    		if(proceed && (ownerId_ds.equals("")||!ownerId_ds.equals(ownerId)))
	    			{
	    			 owner_verified=false; //if one of the datastream is not owner's datastream we don't proceed ahead, May be change this behavior in future
	    			 System.out.println("Either uuid:"+ds_id+", doesn't exist or you don't have privelegs to assign policy to it.");
	    			 sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
	    			 proceed=false;
	    			}
	    		
	    	}//end for(int i=0;i<sensors.size();i++)
	    	
	    	ArrayList<String> userIdList=new ArrayList<String>();//Creating arraylist of users for which we have to register policy
	    	
	    	if(proceed&&owner_verified)
	    	{
	    		System.out.println("In registerPolicy, owner verified PolicyManagement.java");
	    		
	    		/*
	    		 * owner is verified. Means for all the DS_ID, ownerId is valid
	    		 * Now: go to the whom construct, and check, if these users exist in Citadel
	    		 */
	    	    boolean users_verified=true;
	    	    
	    		for(int i=0;proceed&&i<users.size();i++)
	    		{
	    			String userId =users.getString(i);
	    			if(userId.equals(""))
	    		     {
	    				System.out.println("userId cannot be empty");
	    				users_verified=false; 
	    				sendErrorResponse(resp, 400, "Parameters are missing, userId cannot be empty");	
	    				proceed=false;
	    		     }
	    			 
	    			
	    			//check is userId exist in Citadel
	    			//given a userId check if its token exist in the Citadel
	    			String user_token="";
	    			
	    			if(proceed)
	    			 user_token=Auth_meta_data_policy.get_token(userId);
	    			
	    			if(proceed&&user_token.equals(""))
	    			{
	    				System.out.println("userID: "+userId+" doesn't exist in Citadel");
	    				users_verified=false; 
	    				sendErrorResponse(resp, 400, users.getString(i)+" : doesn't exist in Citadel");
	    				proceed=false;
	    				
	    			}//end if(user_token.equals(""))
	    			
	    			//check if userId equals ownerId, this is not allowed???, policy cannot be defined for owner
	    			if(proceed && userId.equals(ownerId))
	    			{
	    				System.out.println("userID: "+userId+" equals ownerID");
	    				users_verified=false; 
	    				sendErrorResponse(resp, 400, users.getString(i)+" : and owner are the same");
	    				proceed=false;
	    				
	    			}//end if(proceed && userId.equals(ownerId))
	    			
	    			if(proceed)
	    			userIdList.add(user_token);//storing these tokens for later use
	    			
	    		}//end for(int i=0;i<whom.size();i++)
	    		
	    		/*
	    		 * All users were verified correctly, and exist in Citadel
	    		 */
	    		if(proceed&&users_verified)
	    		{
	    			
	    		   System.out.println("In registerPolicy, users are verified, PolicyManagement.java");
	    		   
	    		   //for all usersIds and all DsId's insert the policy
	    		   for(int i=0;i<sensors.size();i++)
	    		   {
	    			   for(int j=0;j<users.size();j++)
	    			   {
	    				   String dsId=sensors.getString(i);
	    				   String userId=users.getString(j);
	    				   
	    				   String PolicyString="true";
	    				   
	    				   JsonArray allowedPolygons=new JsonArray();
	    				   JsonArray denyPolygons = new JsonArray();
	    				   
	    				   if(policy.containsKey("where"))
	    				   {
	    					   
	    					   
	    					   JsonObject where = policy.getJsonObject("where");
	    					   System.out.println("Where is:"+where);
	    					   
	    					   if(proceed&&where.containsKey("allowedPolygons"))
	    					   {
	    						
	    						    
	    						   try{  /*Allowed Polygons can be a mess*/
	    							   allowedPolygons=where.getJsonArray("allowedPolygons");
	    						       }catch(Exception e)
	    						   {
	    						    	 System.out.println("allowedPolygons format incorrect");
	    							     sendErrorResponse(resp, 400, "allowedPolygons format incorrect");	 
	    							     proceed=false;
	    						   }
	    						   
	    						   System.out.println("allowedPolygons" + allowedPolygons);
	    						   
	    						   /*
	    						    * 
	    						    * Verify that all allowed polygon actually form a continuous polygons
	    						    * Very Important, else all future queries with this policy will not work
	    						    */
	    						  
	    						   for(int p=0; proceed&&p<allowedPolygons.size();p++)
	    						   {
	    							   JsonArray polygon = allowedPolygons.getJsonArray(p);
	    						       boolean verify = false;
	    						      
	    						       try{
	    						       verify = VerifyPolygonsStructure(polygon);}
	    						       catch(Exception e)
	    						       {
	    						    	   System.out.println("Exception shouldn't occur here"); 
	    						    	  e.printStackTrace(); 
	    						       }
	    
	    								
	    						       if(proceed&&verify==false)//polygon is not in correct format
	    						       {
	    						    	   System.out.println("allowedPolygons format incorrect");
	    						    	   System.out.println(polygon+", Verify is: "+verify);
	    						    	   sendErrorResponse(resp, 400, polygon.toString()+": in allowedPolygons format incorrect");
	    						    	   proceed=false;
	    						       }					       
	    						       
	    						   }//end for(int p=0; p<allowedPolygons.size();p++)
	    						   
	    						   /*
	    						    * end  Verify that allowed polygon actually form a continuous polygons
	    						    */
	    						  
	    						   /*
	    						    * Allowed polygons correct, Now check for denypolygons
	    						    */
	    						  
	    						   
	    					   }//end if(where.containsKey("allowedPolygons"))
	    					   
	    					   
	    					   
	    					   if(proceed&&where.containsKey("denyPolygons"))
	    					   {
	    						
	    						   
	    						   try{  /*Allowed Polygons can be a mess*/
	    							   denyPolygons=where.getJsonArray("denyPolygons");
	    						       }catch(Exception e)
	    						   {
	    						    	 System.out.println("denyPolygons format incorrect");
	    							     sendErrorResponse(resp, 400, "denyPolygons format incorrect");	 
	    							     proceed=false;
	    						   }
	    						   
	    						   System.out.println("denyPolygons" + denyPolygons);
	    						   
	    						   /*
	    						    * 
	    						    * Verify that all deny polygon actually form a continuous polygons
	    						    * Very Important, else all future queries with this policy will not work
	    						    */
	    						  
	    						   for(int p=0; proceed&&p<denyPolygons.size();p++)
	    						   {
	    							   JsonArray polygon = denyPolygons.getJsonArray(p);
	    						       boolean verify = false;
	    						      
	    						       try{
	    						       verify = VerifyPolygonsStructure(polygon);}
	    						       catch(Exception e)
	    						       {
	    						    	   System.out.println("Exception shouldn't occur here"); 
	    						    	  e.printStackTrace(); 
	    						       }
	    
	    								
	    						       if(proceed&&verify==false)//polygon is not in correct format
	    						       {
	    						    	   System.out.println("denyPolygons format incorrect");
	    						    	   System.out.println(polygon+", Verify is: "+verify);
	    						    	   sendErrorResponse(resp, 400, polygon.toString()+": in denyPolygons format incorrect");
	    						    	   proceed=false;
	    						       }					       
	    						       
	    						   }//end for(int p=0; p<denyPolygons.size();p++)
	    						   
	    						   /*
	    						    * end  Verify that denyPolygons polygon actually form a continuous polygons
	    						    */
	    						  
	    						   
	    						   /*
	    						    *  denyPolygons Done, now attempt to insert this policy
	    						    */
	    						   //Policy=policy.toString();
	    						   
	    					   }//end if(where.containsKey("denyPolygons"))
	    					   
	    					   /*
	    					    * In Where something meaningful should exist
	    					    */
	    					   
	    				   }//end if(policy.containsKey("where"))
	    				  
	    				   /*
	    				    * Begin processing of when
	    				    */
	    				   
	    				   JsonArray allowTimes=new JsonArray();
	    				   JsonArray denyTimes=new JsonArray();
	    				   
	    				   if(proceed&&policy.containsKey("when"))
	    				   {
	    					   /*
	    					    * Processing the allowTimes
	    					    */
	    					   JsonObject when=policy.getJsonObject("when");
	    					   
	    					   System.out.println("when is:"+when);
	    					   
	    					   if(proceed&&when.containsKey("allowTimes"))
	    					   {
	    					   try{  /*Allowed Polygons can be a mess*/
	    						   allowTimes=when.getJsonArray("allowTimes");
    						       }catch(Exception e)
    						   {
    						    	 System.out.println("allowTimes format incorrect");
    							     sendErrorResponse(resp, 400, "allowTimes format incorrect");	 
    							     proceed=false;
    						   }
    						   
    						   System.out.println("allowTimes" + allowTimes);
    						   
    						   for(int t=0;proceed&&t<allowTimes.size();t++)
    						   {
    							   JsonObject time = allowTimes.getJsonObject(t);
    							   
    							   boolean verifytime=verifyTimeStructure(time);
    							   
    							   if(proceed&&verifytime==false)
    							   {
    								System.out.println(time+", allowTimes format incorrect");
      							     sendErrorResponse(resp, 400, time+" in, allowTimes format is incorrect");	 
      							     proceed=false;   
    							   }
    							   
    							   System.out.println(time+", Verifytime is :"+verifytime);
    							   
    							   
    						   }//end for(int t=0;proceed&&t<allowTimes.size();t++)
    						   
	    					   }//end  if(when.containsKey("allowTimes"))
	    					   
    						   
	    					   /*
	    					    *end Processing the allowTimes 
	    					    */
	    					   
	    					   
	    					   
	    					   /*
	    					    * begin processing of denyTimes
	    					    */
	    					   
	    					   if(proceed&&when.containsKey("denyTimes"))
	    					   {
	    					   try{  /*Allowed Polygons can be a mess*/
	    						   denyTimes=when.getJsonArray("denyTimes");
    						       }catch(Exception e)
    						   {
    						    	 System.out.println("denyTimes format incorrect");
    							     sendErrorResponse(resp, 400, "denyTimes format incorrect");	 
    							     proceed=false;
    						   }
    						   
    						   System.out.println("denyTimes" + denyTimes);
    						   
    						   for(int t=0;proceed&&t<denyTimes.size();t++)
    						   {
    							   JsonObject time = denyTimes.getJsonObject(t);
    							   
    							   boolean verifytime=verifyTimeStructure(time);
    							   
    							   if(proceed&&verifytime==false)
    							   {
    								System.out.println(time+", denyTimes format incorrect");
      							     sendErrorResponse(resp, 400, time+" in, denyTimes format is incorrect");	 
      							     proceed=false;   
    							   }
    							   
    							   System.out.println(time+", Verifytime is :"+verifytime);
    							   
    							   
    						   }//end for(int t=0;proceed&&t<allowTimes.size();t++)
    						   
	    					   }//end  if(when.containsKey("allowTimes"))
	    					   
	    					   
	    					   /*
	    					    * end processing of allowTimes
	    					    */
	    					   
	    				   }//end if(policy.containsKey("when"))
	    				   
	    				   /*
	    				    * End processing of when
	    				    */
	    				   
	    				   if(proceed&&(allowedPolygons.size()>0||denyPolygons.size()>0||allowTimes.size()>0||denyTimes.size()>0))
	    					 {
	    					   PolicyString=policy.toString();
	    					   System.out.println("PolicyString is:"+PolicyString);
	    					 }//end if(proceed&&(allowedPolygons.size()>0||denyPolygons.size()>0||allowTimes.size()>0))
	    				   
	    				   //Inserting the policy below, policy string should be updated to mentioned the correct policy
	    				   if(proceed)
	    				   { 
	    					   //everything is correct, let us insert this policy
	    					   Auth_meta_data_policy.insert_policy(dsId, userId, PolicyString);//this is default policy with no-space time constraints, with constraints we need to update this
	    				   }//end if(proceed)
	    				   
	    				   
	    				   
	    			   }//end for(int j=0;j<users.size();j++)
	    			   
	    		   }//end for(int i=0;i<sensors.size();i++)
	    		   
	    		   if(proceed)
	    		   System.out.println("In registerPolicy, Policies are registered, PolicyManagement.java");
	    		   
	    		   if(proceed)
	    		   {
	    		   resp.setStatusCode(200);
		    	   content.setSucceess(true);
		    	   String cStr = content.toString();
 		    	   String cLen = Integer.toString(cStr.length());
 		    	   resp.putHeader("content-length", cLen)
 		      	   .write(cStr).end(); 
 		    	   
 		    	  proceed=false;
	    		   }//end if(proceed)
	    		   
	    		}//end if(users_verified)
	    	   
	     		
	    	}//if(owner_verified)
	    	else if(proceed)
	    	{
	    		System.out.println("You don't have priveleges to assign policies to all the dataStreams in what construct");
	    		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
	    		proceed=false;
	    	}
	    	
	    }//end if(body.containsKey("userToken")&&body.containsKey("what")&&body.containsKey("whom"))
	    else if(proceed)
	    {
	    	System.out.println("Policy registration parameters are missing");
	    	sendErrorResponse(resp, 400, "Parameters are missing");
	    	proceed=false;
	    }
	    
	    }//end try
	    catch(Exception e)
	    {
	    	e.printStackTrace();
	    	
	    	if(proceed)
	    	{sendErrorResponse(resp, 400, "Internal server error occured, please contact developers.");
	    	proceed=false;
	    	}
	    	
	    }
     		
		
	}//end registerPolicy
	
	/*
	 * verify that time in when policy construct is proper
	 */
	private boolean verifyTimeStructure(JsonObject time) {
		// TODO Auto-generated method stub
		try
		{
		if(!time.containsKey("start")&&!time.containsKey("end"))
		return false;
		
		long start=time.getLong("start");
		long end = time.getLong("end");
		
		if(start>0 && end>0 && end >start)
		return true;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return false;
		
	}//end private boolean verifyTimeStructure(JsonObject time)

	//Verify that input polygon actually form a continuous polygons
	boolean VerifyPolygonsStructure(JsonArray polygon)
	{
		if(polygon.size()==0)
			return false;
		
		boolean verify=true;
		
		try
		{
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		Coordinate[] coords  = new Coordinate[polygon.size()];
		
		//System.out.println("Space polygon allowed:"+polygon);
		
			  for(int j=0;j<polygon.size();j++)
			  {
				  JsonObject pos=polygon.getJsonObject(j);
				  double lat=pos.getDouble("lat");
				  double lng=pos.getDouble("lng");    
				  coords[j]=new Coordinate(lat, lng);	 
			  }//end for(int j=0;j<polygon.size();j++)
			  
			  
			  LinearRing ring = geometryFactory.createLinearRing( coords );
			  LinearRing holes[] = null; // use LinearRing[] to represent holes
			  Polygon pol = geometryFactory.createPolygon(ring, holes );
		  
		   }//end try
		catch(Exception e)
		{
			//polygon is not a valid closed polygon
			//e.printStackTrace();
			verify=false;
			return verify;
		}
		
		return verify;
		
	}//end VerifyPolygonsStructure()
	
}//end PolicyManagement
