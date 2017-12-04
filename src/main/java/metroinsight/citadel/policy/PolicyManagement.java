package metroinsight.citadel.policy;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class PolicyManagement {

	/*
	 * Register a Policy
	 * {Token:ownerToken, What:[DS_ID], Whom [UserIDs]}
	 */
	public void registerPolicy(RoutingContext rc) {
		
		System.out.println("In registerPolicy PolicyManagement.java");
	    JsonObject body = rc.getBodyAsJson();
	    
	    System.out.println("Post: "+body);
	    
	    JsonObject result = new JsonObject();
     	  result.put("result", "SUCCESS");
     	  String length = Integer.toString(result.toString().length());
     		rc.response()
     		  .putHeader("content-TYPE", "application/text; charset=utf=8")
     		  .putHeader("content-length",  length)
     		  .setStatusCode(201)
     		  .write(result.toString());
     		
		
	}//end registerPolicy
	
}//end PolicyManagement
