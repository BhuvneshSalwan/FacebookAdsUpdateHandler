package com.facebook.ads.main;

import java.util.ArrayList;

import com.facebook.ads.common.AdUpdate;
import com.facebook.ads.common.CreativeCreate;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.bigquery.main.BQOperations;
import com.google.bigquery.main.GAuthenticate;

/**
 * Hello world!
 *
 */
public class App 
{

	public static ArrayList<Rows> logChunk = new ArrayList<Rows>();
	
	public static void main(String[] args){

		Bigquery bigquery = GAuthenticate.getAuthenticated();
		
		System.out.println(bigquery);
		
		if(null != bigquery){
			
			if(BQOperations.StructureValidate(bigquery, "creative_create")){
	    	
				System.out.println("Response Message : Validated the Structure of Table : creative_create in the Big Query.");
				
				ArrayList<com.google.api.services.bigquery.model.TableRow> list = BQOperations.getCreativeRows(bigquery);
				
				if(null != list && list.size() > 0){

					for(int arr_i = 0; arr_i < list.size(); arr_i++){
					
						com.google.api.services.bigquery.model.TableRow row = list.get(arr_i);
						
						String creative_id = CreativeCreate.postCreative(row);
						
						if(null != creative_id){
						
							ArrayList<com.google.api.services.bigquery.model.TableRow> listAds = BQOperations.getAdRows(bigquery, String.valueOf(row.getF().get(9).getV()));
							
							if(null != listAds){
								
								for(com.google.api.services.bigquery.model.TableRow rowAds : listAds){
									
									if(AdUpdate.updateAds(creative_id, rowAds)){
										
										System.out.println("Response Message : Ad Updated Successfully.");
										
									}
									else{
										System.out.println("Response Message : Ad Updation failed for the MySQL Creative ID : " + String.valueOf(row.getF().get(0).getV()));
									}
									
								}
								
							}
							else{
								System.out.println("Response Message : No Adset was there in the Ad Create Table.");
							}
							
						}
						else{
							System.out.println("Response Message : Some ERROR while creating Creative as Creative in AD is NULL.");
						}
						
					}
				
					if( null != logChunk && logChunk.size() > 0){
			    		
			    		if(BQOperations.insertDataRows(bigquery, logChunk)){
			    			System.out.println("Response Message : Logs Added Successfully.");
			    		}else{
			    			System.out.println("Response Message : Error while saving Logs.");
			    		}
			    		
			    	}
					
				}
				
				else{
				
					System.out.println("Response Message : No Updation is requested in Adset Create Table.");
				
				}
				
			}
					
			else{
				
				System.out.println("Response Message : Couldn't validate the Structure of Table : adset_create in the Big Query.");
				System.exit(0);	
			
			}
		    			
		}
		
		else{
			
			System.out.println("Response Message : Couldn't Establish connection with Big Query.");
			System.exit(0);
		
		}
		 
	}
	
}