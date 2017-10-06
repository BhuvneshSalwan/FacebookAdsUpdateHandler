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

public class AdUpdate {

	public static final String URL = "https://graph.facebook.com";
	public static final String VERSION = "v2.9";
	public static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";
	
	public static Boolean updateAds(String creative_id, TableRow rowAds) {

		try {
			
			String ad_id;
   		 	String hostname;
   		 	String parse_client_id;
			
   		 	try{ ad_id = String.valueOf(rowAds.getF().get(0).getV()); }catch(Exception e){ System.out.println("Exception while processing the value for Ad ID i.e. " + rowAds.getF().get(0).getV() + "."); return false;}
   		 	try{ hostname = String.valueOf(rowAds.getF().get(1).getV()); }catch(Exception e){ System.out.println("Exception while processing the value for Hostname i.e. " + rowAds.getF().get(1).getV() + "."); hostname = null;}
   		 	try{ parse_client_id = String.valueOf(rowAds.getF().get(2).getV()); }catch(Exception e){ System.out.println("Exception while processing the value for Parse Client ID i.e. " + rowAds.getF().get(2).getV() + "."); parse_client_id = null;}
   		 	
   		 	String url1 = "https://graph.facebook.com/v2.9/" + ad_id;

			HttpClient client1 = new DefaultHttpClient();
			HttpPost post1 = new HttpPost(url1);

			List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
			
			JSONObject jsoncreative = new JSONObject();
			jsoncreative.put("creative_id", creative_id);
			urlParameters.add(new BasicNameValuePair("creative", jsoncreative.toString()));
			urlParameters.add(new BasicNameValuePair("access_token", SESSION_TOKEN));
			
			System.out.println(urlParameters.toArray().toString());
			
			post1.setEntity(new UrlEncodedFormEntity(urlParameters, "utf-8"));
				
			HttpResponse responseClients = client1.execute(post1);
				
			System.out.println("\nSending 'POST' request to URL : " + url1);
			System.out.println("Post parameters : " + post1.getEntity());
			System.out.println("Response Code : " + responseClients.getStatusLine().getStatusCode());

			BufferedReader read = new BufferedReader(new InputStreamReader(responseClients.getEntity().getContent()));

			StringBuffer result1 = new StringBuffer();
			String line1 = "";
			while ((line1 = read.readLine()) != null) {
				result1.append(line1);
			}

			System.out.println(result1.toString());

			Rows logsRow = new Rows();
	 			
 			HashMap<String, Object> logsMap = new HashMap<String, Object>();
 			
 			logsMap.put("hostname", hostname);
 			logsMap.put("parse_client_id", parse_client_id);
 			logsMap.put("operation", "UPDATE");
 			logsMap.put("table_name", "AD_UPDATE");
 			logsMap.put("ad_id", ad_id);
 			logsMap.put("creative_id", creative_id);
 			logsMap.put("status_code", responseClients.getStatusLine().getStatusCode());
 			logsMap.put("response_message", result1.toString());
 			logsMap.put("created_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
 			
 			logsRow.setJson(logsMap);
 			App.logChunk.add(logsRow);
 			
			JSONObject response = new JSONObject(result1.toString());

			if (responseClients.getStatusLine().getStatusCode() >= 400 && responseClients.getStatusLine().getStatusCode() < 500) {
				
				System.out.println("Response Message : Ad Creation Failed with an Error Code. Please refer to the Response String.");
				
				return false;
					
			} else {

				if (response.has("id")) {
					
					System.out.println("Response Message : Ad Created Successfully with ID : " + response.getString("id"));
					
					return true;
				}
				
				else{
					
					System.out.println("Response Message : Please refer the response string to check if ad created or not.");
					
					return false;
					
				}

			}

		} catch (Exception e) {
			System.out.println("Exception : AdCreate Class : createAds");
			System.out.println(e);
			return false;
		}

	}
	
}