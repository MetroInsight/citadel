package metroinsight.citadel.authorization;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.templ.JadeTemplateEngine;

import java.util.Collections;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken.Payload;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;


public class GoogleLogin {

	//String usergmail="";
	//String username="";
	
	
	 public void DisplayToken(RoutingContext rc) {
		 
		 System.out.println("Display Token Called");
		 String body=rc.getBodyAsString();
		 //System.out.println("Body is: "+body); 
		 //verifyToken("eyJhbGciOiJSUzI1NiIsImtpZCI6IjMwM2IyODU1YTkxNDM4NTcwY2E3Mjg1MDQ5MTc0MWU5NmJkOTllZjgifQ.eyJhenAiOiI4Mjk0ODQ4NDkwNDctY29pM2VtdGJra3M5b2dsamFhaWxpMmVydTViNm03dnAuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJhdWQiOiI4Mjk0ODQ4NDkwNDctY29pM2VtdGJra3M5b2dsamFhaWxpMmVydTViNm03dnAuYXBwcy5nb29nbGV1c2VyY29udGVudC5jb20iLCJzdWIiOiIxMTUyMzkyNzIyODMxMTYzNzEwMDgiLCJlbWFpbCI6ImNpdGFkZWwudWNsYUBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiYXRfaGFzaCI6IjhGT2Nlc2tOXzN2OC1qel9VYWhEMlEiLCJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwiaWF0IjoxNTA1MTc4MzcwLCJleHAiOjE1MDUxODE5NzAsIm5hbWUiOiJTYW5kZWVwIFNhbmRoYSIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vLUVzQmJVUnZmdXJZL0FBQUFBQUFBQUFJL0FBQUFBQUFBQUFBL0FQSnlwQTAzSGtrdUdQeTNJSTY3d3M3U2NEZ1ZjUndQTVEvczk2LWMvcGhvdG8uanBnIiwiZ2l2ZW5fbmFtZSI6IlNhbmRlZXAiLCJmYW1pbHlfbmFtZSI6IlNhbmRoYSIsImxvY2FsZSI6ImVuIn0.U68b1QRl_UKEzpuh00d2_reOqZNc7RmlGNCPeFfvRvwBjvpiiuoH7uknurB8BWi5t8ExUMdSXTGJcSC5fmaDBC6YKURg-_12S5Ub1UdIgW0YLj7BXSwunwpRN8ZE113Z-QkjpjCrf4MgI32CiGPwL6EccIJKcN7-B4m4X0UaL07gwptSJqHG5DgGYcYl9bHsZ4DQyrJulfdDHBb4rTQYBvDcSWBOuDylRrjnDE-WhU3tvr7i38iPHy8T3c4_9tO_25t4vd0LLFhAeCAcX49g1T_aX7Bh2gDtTFySzusb7zmoFII_DsYMKt77weYKGnBzV9aQZAcOaDu4lLvddsVQ5w");
		 boolean verified=verifyToken(body,rc);
		 
		 
		 
	 }//end DisplayToken
	
	private void createSession(String email, String name,RoutingContext rc) {
		// TODO Auto-generated method stub
		
		System.out.println("Create Session Called");
		
		Session session = rc.session();
		if(session!=null)
		{
		 System.out.println("Session id in createSession is:"+session.id());
	     session.put("usergmail", email);
	     session.put("username", name);
	     session.put("login", true);//variable to tell user is logged
		}
		else
			System.out.println("Session is null");
	}//end create session

	private boolean verifyToken(String Token,RoutingContext rc)
	{
		System.out.println("Verify Token Called");
		
		JsonFactory jsonFactory = new JacksonFactory();
		HttpTransport transport = new NetHttpTransport();
		
		 
		GoogleIdTokenVerifier verifier = new GoogleIdTokenVerifier.Builder(transport, jsonFactory)
			    .setAudience(Collections.singletonList("829484849047-coi3emtbkks9ogljaaili2eru5b6m7vp.apps.googleusercontent.com"))
			    // Or, if multiple clients access the backend:
			    //.setAudience(Arrays.asList(CLIENT_ID_1, CLIENT_ID_2, CLIENT_ID_3))
			    .build();

		try
		{
	    GoogleIdToken idToken = verifier.verify(Token);
		if (idToken != null) {
			
		  System.out.println("Token Validated");
			
		  Payload payload = idToken.getPayload();
		  
		  // Print user identifier
		  String userId = payload.getSubject();
		  //User ID: 115239272283116371008
		  System.out.println("User ID: " + userId);

		  // Get profile information from payload
		  String email = payload.getEmail();
		  boolean emailVerified = Boolean.valueOf(payload.getEmailVerified());
		  String name = (String) payload.get("name");
		  String pictureUrl = (String) payload.get("picture");
		  String locale = (String) payload.get("locale");
		  String familyName = (String) payload.get("family_name");
		  String givenName = (String) payload.get("given_name");

		  // Use or store profile information
		  // ...
		  
		 // System.out.println("Email:"+email);
		 // System.out.println("Name:"+name);
		  
		  
		  
		  
		  //creating session for the user, after token is verified
		  createSession(email,name,rc);
		  
		  
		  return true;
		  
		} else {
		  System.out.println("Invalid ID token.");
		  
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return false;
	}//end verifyToken

	
	 
	 public void DisplayLoginJade(RoutingContext rc) 
	 {
		 System.out.println("DisplayLoginJade Called");
		 
		 boolean login = false;
		 Session session = rc.session();
		 
		 if(session!=null)
		 {
	     System.out.println("Session id in DisplayLoginJade:"+session.id());
	     
			     try{
			    	 
			    	 System.out.println("Testing:"+session.data());
			    	
				     login =session.get("login");
				     }
				     catch(Exception e)
				     {
				    	 //e.printStackTrace();
				     }
	    	 
		 }
		 else
			 System.out.println("session is null in DisplayLoginJade");
		 //end check
		 
		 
		 
		 JadeTemplateEngine engine = JadeTemplateEngine.create();
		// and now delegate to the engine to render it.
		 
		 
		 if(login==false)
		 {
			 System.out.println("User is not logged in DisplayLoginJade");
		   //asking user to login in case session is null	 
	      engine.render(rc, "templates/login.jade", res -> {
	        if (res.succeeded()) {
	          rc.response().end(res.result());
	        } else {
	          rc.fail(res.cause());
	        }
	      });
		 }//end if(login==false)
	     
		 
		 else if(login)//user is logged in
		 {
			 System.out.println("User is logged in DisplayLoginJade");
			 
			 DisplayIndexJade(rc);
			 
			 /*
			 //below statement should not return null, if login is true
			 String email=session.get("usergmail");
			 
			 
			 
			 String userToken=UserTokenManager.generateToken(email);
			 
			 rc.put("username", email);
			 rc.put("tokens",userToken);
			 
			 //asking user to login in case session is null	 
		      engine.render(rc, "templates/index.jade", res -> {
		        if (res.succeeded()) {
		          rc.response().end(res.result());
		        } else {
		          rc.fail(res.cause());
		        }
		      }); 
		      */
		 }
		 
		 
	 }//end DisplayIndexJade
	 
	 
	 public void DisplayIndexJade(RoutingContext rc) 
	 {
		 System.out.println("DisplayindexJade Called");
		 
		 boolean login = false;
		 Session session = rc.session();
		 
		 if(session!=null)
		 {
	     System.out.println("Session id in DisplayIndexJade:"+session.id());
	     
			     try{
			    	 
			    	 System.out.println("Testing:"+session.data());
			    	
				     login =session.get("login");
				     }
				     catch(Exception e)
				     {
				    	// e.printStackTrace();
				     }
	    	 
		 }
		 else
			 System.out.println("session is null in DisplayIndexJade");
		 //end check
		 
		 
		 
		 JadeTemplateEngine engine = JadeTemplateEngine.create();
		// and now delegate to the engine to render it.
		 
		 
		 if(login==false)
		 {
			 
			 System.out.println("User is not logged in DisplayIndexJade");
			 
			 DisplayLoginJade(rc);//note there can be loop possible, be careful things are not cyclic
			 /*
		   //asking user to login in case session is null	 
	      engine.render(rc, "templates/login.jade", res -> {
	        if (res.succeeded()) {
	          rc.response().end(res.result());
	        } else {
	          rc.fail(res.cause());
	        }
	      });
	      */
			 
		 }//end if(login==false)
	     
		 else if(login)//user is logged in
		 {
			 //below statement should not return null, if login is true
			 String email=session.get("usergmail");
			 
			 System.out.println("User is logged in DisplayIndexJade");
			 
			 String userToken=UserTokenManager.generateToken(email);
			 
			 rc.put("username", email);
			 rc.put("tokens",userToken);
			 
			 //asking user to login in case session is null	 
		      engine.render(rc, "templates/index.jade", res -> {
		        if (res.succeeded()) {
		          rc.response().end(res.result());
		        } else {
		          rc.fail(res.cause());
		        }
		      }); 
		 }
		 
		 
	 }//end DisplayIndexJade
	 
	 
	 public void LogOut(RoutingContext rc) 
	 {
		 Session session = rc.session();
		 
		 //invalidating the session
		 if(session!=null)
		 {
	     session.destroy();
	     session=null;
		 }
		 System.out.println("LogOut Called");
		 //again loading index page with session == null
		 DisplayLoginJade(rc);
		 
	 }//end LogOut
	 
}//end class
