package com.facebook.ads.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

import com.facebook.ads.main.App;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableRow;

public class CreativeCreate {

	public static final String URL = "https://graph.facebook.com";
	public static final String VERSION = "v2.9";
	public static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";
	
	public static String postCreative(TableRow row) {

    	 try {

    		 String account_id;
    		 String website;
    		 String ad_text;
    		 String parse_campaign_id;
    		 String referrer;
    		 String creative_name;
    		 String FbPageId;
    		 String product_set_id;
    		 String InstagramId = null;
    		 String parse_client_id;
    		 
    		 try{ account_id = (String) row.getF().get(0).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Account_ID i.e. " + row.getF().get(0).getV() + "."); return null;}
 			 try{ website = (String) row.getF().get(1).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Website i.e. " + row.getF().get(1).getV() + "."); return null;}
 			 try{ ad_text = (String) row.getF().get(2).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Ad Text i.e. " + row.getF().get(2).getV() + "."); return null;}
 			 try{ product_set_id = (String) row.getF().get(3).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Products i.e. " + row.getF().get(3).getV() + "."); return null;}
 			 try{ parse_campaign_id = (String) row.getF().get(4).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Parse Campaign ID i.e. " + row.getF().get(4).getV() + "."); return null;}
 			 try{ referrer = (String) row.getF().get(5).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Referrer i.e. " + row.getF().get(5).getV() + "."); return null;}
 			 try{ creative_name = (String) row.getF().get(6).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Referrer i.e. " + row.getF().get(6).getV() + "."); return null;}
  			 try{ FbPageId = (String) row.getF().get(7).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for FBPageId i.e. " + row.getF().get(7).getV() + "."); return null;}
   			 try{ InstagramId = (String) row.getF().get(8).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Referrer i.e. " + row.getF().get(8).getV() + ".");}
   			 try{ parse_client_id = (String) row.getF().get(10).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Parse Client ID i.e. " + row.getF().get(10).getV() + "."); parse_client_id = null; }

   			String url = "https://graph.facebook.com/v2.9/act_"+account_id+"/adcreatives";

    		 HttpClient client = new DefaultHttpClient();
    		 HttpPost post = new HttpPost(url);
    		 
    		 JSONObject objectStory = new JSONObject();
    		 JSONObject linkData = new JSONObject();
    		 linkData.put("link", website);                                                                                                                                                                                                                                                                                                                                                                                                                                 
    		 linkData.put("message", ad_text);
    		 linkData.put("name", "{{product.name | titleize}}");
    		 linkData.put("description", "{{product.price}}");
    		 linkData.put("call_to_action", "{\"type\":\"SHOP_NOW\"}");
    		 
    		 objectStory.put("template_data", linkData);
    		 objectStory.put("page_id", FbPageId);
    		 objectStory.put("instagram_actor_id", InstagramId);
 			
    		 List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    		 urlParameters.add(new BasicNameValuePair("access_token", SESSION_TOKEN));
    		 urlParameters.add(new BasicNameValuePair("name", creative_name));
    		 urlParameters.add(new BasicNameValuePair("product_set_id", product_set_id));
    		 urlParameters.add(new BasicNameValuePair("object_story_spec", objectStory.toString()));
    		 urlParameters.add(new BasicNameValuePair("url_tags", "utm_source="+referrer+"&utm_medium=fb&utm_campaign="+parse_campaign_id)); 
    		  
    		 post.setEntity(new UrlEncodedFormEntity(urlParameters, "utf-8"));
    		 
    		 HttpResponse response = client.execute(post);
    		 System.out.println("\nSending 'POST' request to URL : " + url);
    		 System.out.println("Post parameters : " + post.getEntity());
    		 System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

       		 BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

    		 StringBuffer result = new StringBuffer();
    		 String line = "";
    		 while ((line = rd.readLine()) != null) {
    			 result.append(line);
    		 }

    		 System.out.println(result.toString());
    		 
    		 Rows logsRow = new Rows();
 			
 			 HashMap<String, Object> logsMap = new HashMap<String, Object>();
 			
  		     logsMap.put("hostname", website);
  			 logsMap.put("parse_client_id", parse_client_id);
 			 logsMap.put("account_id", account_id);
 			 logsMap.put("operation", "CREATE");
 			 logsMap.put("table_name", "CREATIVE_CREATE");
 			 logsMap.put("creative_name", creative_name);
 			 logsMap.put("product_set_id", product_set_id);
 			 logsMap.put("status_code", response.getStatusLine().getStatusCode());
 			 logsMap.put("response_message", result.toString());
 			 logsMap.put("created_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
 			
 			 logsRow.setJson(logsMap);
 			 App.logChunk.add(logsRow);
 			
    		 if(response.getStatusLine().getStatusCode() >= 500){	 
    			 
    			 System.out.println("Response Message : Creative Create Ended with a 500 Response Error.");
    			 
    			 return null;
    			 
    		 }
    		 else if(response.getStatusLine().getStatusCode() >= 400 && response.getStatusLine().getStatusCode() < 500){

    			 System.out.println("Response Message : Creative Create Ended with a 400 Series Response Status. Please check the Response String.");
    			 return null;
    			 
    		 }
    		 else{
	    	
    			 JSONObject response1 = new JSONObject(result.toString());
	    		 
	    		 if (!response1.isNull("id")) {
	    			 
	    			 System.out.println("Response Message : The Creative is Created Successfully.");
	    			 return response1.getString("id");
	    			 
	    		 }
	    		 
	    		 else{
	    			 
	    			 System.out.println("Response Message : Couldn't find the Creative ID in the response from Facebook.");
	    			 return null;
	    			 
	    		 }
    		 
    		 }
    	 
    	 } catch (Exception e) {
    		 System.out.println("Exception - Class CreativeCreate : PostCreative");
    		 System.out.println(e);
    		 return null;
    	 }

     }
	
}