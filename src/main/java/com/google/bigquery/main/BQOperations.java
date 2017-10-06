package com.google.bigquery.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;

public class BQOperations {
	
	private static final String PROJECT_ID = "stellar-display-145814";
	private static final String DATASET_ID = "dsp_output";

	public static Boolean StructureValidate(Bigquery bigquery, String TABLE_ID){
		try {
			TableList tables = bigquery.tables().list(PROJECT_ID, DATASET_ID).execute();	
			System.out.println(tables);
			if(null != tables && null != tables.getTables()){
				for(Tables table : tables.getTables()){
					if(table.getId().equalsIgnoreCase(PROJECT_ID + ":" + DATASET_ID + "." + TABLE_ID)){
						return true;
					}
				}
			}	
			return false;
		} catch (Exception e) {
			System.out.println(e);
			return false;	
		}
	}
	
	public static ArrayList<TableRow> getCreativeRows(Bigquery bigquery){
		
		try{
		
			String querysql = "SELECT account_id,hostname,ad_text,product_set_id,parse_campaign_id,referrer,creative_name,fb_page_id,instagram_id,sql_creative_id,parse_client_id FROM [stellar-display-145814:dsp_output.creative_create];";			     
			JobReference jobId = startQuery(bigquery, PROJECT_ID, querysql);

			// Poll for Query Results, return result output
			Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

			// Return and display the results of the Query Job
			return (ArrayList<TableRow>) getQueryResults(bigquery, PROJECT_ID, completedJob);
    
		}catch(Exception e){
			System.out.println("Exception : BQoperations : getCreativeRows");
			System.out.println(e);
			return null;
		}
			
	}
	  
	public static ArrayList<TableRow> getAdRows(Bigquery bigquery, String sql_creative_id){
		
		try{
		
			String querysql = "SELECT ad_id,hostname,parse_client_id FROM [stellar-display-145814:dsp_output.ad_update] where sql_creative_id = \""+ sql_creative_id +"\";";			     
			JobReference jobId = startQuery(bigquery, PROJECT_ID, querysql);

			// Poll for Query Results, return result output
			Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

			// Return and display the results of the Query Job
			return (ArrayList<TableRow>) getQueryResults(bigquery, PROJECT_ID, completedJob);
    
		}catch(Exception e){
			System.out.println("Exception : BQoperations : getAdRows");
			System.out.println(e);
			return null;
		}
			
	}

    public static JobReference startQuery(Bigquery bigquery, String projectId,
		                                        String querySql) throws IOException {
		    System.out.format("\nSelection Query Job: %s\n", querySql);

		    Job job = new Job();
		    JobConfiguration config = new JobConfiguration();
		    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		    config.setQuery(queryConfig);

		    job.setConfiguration(config);
		    queryConfig.setQuery(querySql);

		    Insert insert = bigquery.jobs().insert(projectId, job);
		    insert.setProjectId(projectId);
		    JobReference jobId = insert.execute().getJobReference();

		    System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

		    return jobId;
	}

    private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
		      throws IOException, InterruptedException {
		    // Variables to keep track of total query time
		    long startTime = System.currentTimeMillis();
		    long elapsedTime;

		    while (true) {
		      Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
		      elapsedTime = System.currentTimeMillis() - startTime;
		      System.out.format("Job status (%dms) %s: %s\n", elapsedTime,
		          jobId.getJobId(), pollJob.getStatus().getState());
		      if (pollJob.getStatus().getState().equals("DONE")) {
		        return pollJob;
		      }
		      Thread.sleep(1000);
		    }

    }

    private static List<TableRow> getQueryResults(Bigquery bigquery, String projectId, Job completedJob) {

    	try{
    	
    		GetQueryResultsResponse queryResult = bigquery.jobs()
		        .getQueryResults(
		            projectId, completedJob
		            .getJobReference()
		            .getJobId()
		        ).execute();
		    
		    int totRows = queryResult.getTotalRows().intValue();
		    
		    System.out.println("Total Rows fetched are : "+totRows);
		    
		    if(totRows > 0){
		    	
		    	return queryResult.getRows();
			    
		    }
		    
		    else{
		    	
		    	return null;
		    	
		    }
		    
    	}catch(Exception e){
    		
    		System.out.println(e);
    		
    		return null;
    		
    	}
		    
	}
    
	public static Boolean insertDataRows(Bigquery bigquery, ArrayList<Rows> datachunk) {
		
		try {
			
			for(Rows row : datachunk){
				System.out.println(row.getJson().toString());
			}
			
			System.out.println(bigquery);
	
			TableDataInsertAllRequest content = new TableDataInsertAllRequest();
			content.setKind("bigquery#tableDataInsertAllRequest");
			content.setRows(datachunk);
			
			TableDataInsertAllResponse response = bigquery.tabledata().insertAll(PROJECT_ID, "docker_logs", "docker_facebook_logs", content).execute();
		
			System.out.println(response.toPrettyString());
			
			return true;
		
		} catch (Exception e) {
		
			System.out.println(e);
		
			return false;
			
		}
	
	}
	
}