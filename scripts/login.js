
 function SendToken(token)
    {
    	 //console.log("ID Token: " + id_token);
        var xhr = new XMLHttpRequest();
        xhr.open('POST', ipaddress+':' + port + '/api/token');
        
        xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
        xhr.onload = function() {
          console.log('Signed in as: ' + xhr.responseText);
        };
        xhr.send(token); 
         
    }

 
function onSignIn(googleUser) {
        // Useful data for client-side scripts:
    	  
        var profile = googleUser.getBasicProfile();
        
        console.log("ID: " + profile.getId()); // Don't send this directly to your server!
        console.log('Full Name: ' + profile.getName());
        console.log('Given Name: ' + profile.getGivenName());
        console.log('Family Name: ' + profile.getFamilyName());
        console.log("Image URL: " + profile.getImageUrl());
        console.log("Email: " + profile.getEmail());
        
        
        // The ID token you need to pass to your backend:
    	  var id_token = googleUser.getAuthResponse().id_token;
        
        console.log("Sending ID Token: " + id_token);
        SendToken(id_token); 
        setTimeout(function(){
	     window.location= ipaddress+":"+port+"/index";
        },50);
        
      }
      
      
      function signOut() {
    	  
    	   console.log('signOut in login.js');

    	   var xhr = new XMLHttpRequest();
           
            xhr.open('POST', ipaddress+':'+port+'/logout');
            //xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
            xhr.onload = function() {
              console.log('Signed out called: ' + xhr.responseText);
            };
            xhr.send(); 
            
            
            gapi.auth2.getAuthInstance().signOut().then(function () {
    	      console.log('User signed out.');
    	      window.location= ipaddress+":"+port+"/login";
    	    }).catch(function(error){
    	    	console.log('Error occured in signed out.');
    	    	console.log(error.message);
    	    }); 
    	    
    	    
          //redirecting to login on Citadel
           // window.location= "http://citadel.westus.cloudapp.azure.com:8080/login";
    	  }

      function onLoad() {
    	  console.log('onLoad called');
          gapi.load('auth2', function() {
            gapi.auth2.init();
          });
          
          
        }
      
      
