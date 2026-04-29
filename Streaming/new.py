import requests
import json
#import snowflake.connector as snow
import datetime
from dateutil.tz import tzutc
from dateutil import parser
import pytz
import azure.functions as func
from azure.storage.blob import BlobClient
import logging
from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceModifiedError,ResourceNotFoundError
from azure.core import MatchConditions
import time

import re

def pipe_get_activity_name(child_data,pipeline_name,run_id):
    logging.info("The size of activity log is {} ".format(len(child_data)))
    logging.info("The name of pipline for  ticket  created is {} ".format(pipeline_name))
    lv_create_inc_flag=0
    for record1 in child_data:
        if len(record1.strip()) != 0:
                record_dict_child = json.loads(record1)
                # logging.info("Pipeline Name in Blob Record: {}".format(record_dict["pipelineName"]))
                # Check if status is failed or timed out# Check if pipeline run ID matches# Check if pipeline name matches
                if(record_dict_child["status"] in  ["Failed","TimedOut"] and record_dict_child["pipelineRunId"]==run_id and record_dict_child["pipelineName"]==pipeline_name):
                    #lv_create_inc_flag=1
                    logging.info(" the pipeline name is {} ".format(record_dict_child["pipelineName"])) # Log pipeline name   
                    data_factory_name = record_dict_child["billingResourceId"][-10:]# Extract data factory name
                    pipeline_name=record_dict_child["pipelineName"]# Extract pipeline name
                    activity_name=record_dict_child["activityName"]# Extract activity name
                     # Check if 'Error' key exists in record_dict_child["properties"]
                    if "Error" in record_dict_child["properties"]:# If "Error" exists, extract error code and message from the properties
                        error_code = record_dict_child["properties"]["Error"].get("errorCode","ERROR-0000")  # Extract error message
                        error_message = record_dict_child["properties"]["Error"].get("message", "The pipeline is failed")
                    else:# If "Error" doesn't exist, assign default values for error code and message
                        error_code = "ERROR-0000"
                        error_message = "The pipeline is failed"
    
                    event_time = record_dict_child["time"]
                    #error_code=record_dict_child["properties"]["Error"]["errorCode"]# Extract error message
                    #error_message=record_dict_child["properties"]["Error"]["message"]
                    #event_time=record_dict_child["time"]
                     # Create a dictionary with extracted information
                    result_dict={"datafactory":data_factory_name,"pipelineName":pipeline_name,"activityName":activity_name,"error_code":error_code,"errormessage":error_message,"eventtime":event_time}
                    return result_dict# Return the dictionary
                else:
                    continue# Move to the next record if conditions are not met
    return -1   # Return -1 if no matching record is found         
    
def check_if_activitylog_exists(child_data,pipeline_name,run_id):# Function to check if an activity log exists for a given pipeline run
    for record1 in child_data:# Iterate through each record in the child data
        if len(record1.strip()) != 0:# Check if the record is not empty
                record_dict_child = json.loads(record1)# Convert the record to a dictionary
                # logging.info("Pipeline Name in Blob Record: {}".format(record_dict["pipelineName"]))
                if(record_dict_child["status"] in  ["Failed","TimedOut"] and record_dict_child["pipelineRunId"]==run_id and record_dict_child["pipelineName"]==pipeline_name):# Check if status is failed or timed out
                    return False

# Function to download activity blob data
def download_activity_blob(start_date_time,subscription,child_container_name,blob_resource_group,blob_account_url,access_key,blob_df_name,incident_business_unit):
    current_year_ch = start_date_time.strftime("%Y")
    current_month_ch = start_date_time.strftime("%m")
    current_day_ch = start_date_time.strftime("%d")
    current_hour_ch = start_date_time.strftime("%H")
    current_minutes_ch = "00"
    retry_delay=30
    max_retries=10
    # Get ADF pipeline/activity logs for last updated date in Azure Table
    child_container_name="insights-logs-activityruns" # Set child container name
    #blob_location = container_name + "/resourceId=/SUBSCRIPTIONS/" + subscription + "/RESOURCEGROUPS/" + blob_resource_group + "/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/" + blob_df_name + "/y=" + current_year_ch +"/m=" + current_month_ch+ "/d=" + current_day_ch + "/h=" + current_hour_ch+ "/m=" + current_minutes_ch
    blob_location_child=child_container_name + "/resourceId=/SUBSCRIPTIONS/" + subscription + "/RESOURCEGROUPS/" + blob_resource_group + "/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/" + blob_df_name + "/y=" + current_year_ch +"/m=" + current_month_ch+ "/d=" + current_day_ch + "/h=" + current_hour_ch+ "/m=" + current_minutes_ch
    blob_name = "PT1H.json"

    blob_child=BlobClient(account_url=blob_account_url,container_name=blob_location_child,blob_name=blob_name,credential=access_key, max_single_get_size=256*1024*1024,max_chunk_get_size=256*1024*1024)
    logging.info(f"Checking logs in blob location {blob_location_child}")
    #data = [] #activity_logs_data
    child_data=[]# Initialize empty list for child data
    if((blob_child.exists() == False)):# Check if blob exists
        logging.info("Blob Not Found: {}".format(blob_location_child))
        return -1
    try:
        logging.info("Activity logs are being downloaded")
        for retry_count in range(max_retries):
        #data = blob.download_blob().readall().decode("UTF-8").split("\n")    
        #child_data=blob_child.download_blob().readall().decode("UTF-8").split("\n")
            try:
                logging.info(f"Downloading activity logs (Attempt {retry_count + 1}/{max_retries})")
                child_data=blob_child.download_blob().readall().decode("UTF-8").split("\n")
                return child_data
            except ResourceNotFoundError as ex:
                logging.error(f"Blob not found: {ex}")
                return -1
            except Exception as ex:
                logging.error(f"Error downloading blob: {ex}")
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)    
    except ResourceModifiedError as r:
        exception_msg = "{} \n {}".format(r, r.__traceback__)
        logging.error("{}: {}".format(incident_business_unit, exception_msg))
        logging.info("{}: Exception ResourceModifiedError occurred while downloading child log".format(incident_business_unit))
        # data = blob.download_blob(etag=etag, match_condition=MatchConditions.IfModified).readall().decode("UTF-8").split("\n")
        #data = blob.download_blob(etag='*', match_condition=MatchConditions.IfNotModified).readall().decode("UTF-8").split("\n")
        child_data = blob_child.download_blob(etag='*', match_condition=MatchConditions.IfNotModified).readall().decode("UTF-8").split("\n")

        logging.info("{}: Exception ResourceModifiedError handled for child log".format(incident_business_unit))
    except Exception as e:
        exception_msg = "{} \n {}".format(e, e.__traceback__)
        logging.error("{}: {}".format(incident_business_unit, exception_msg))
        
    
    return child_data        


def main(body: dict) -> dict:

    utc_date_time = datetime.datetime.now(tzutc())
    logging.info("UTC Date Time: {}".format(utc_date_time))
    environment = body.get("ENVIRONMENT")
    incident_business_unit = body.get('INCIDENT_BUSINESS_UNIT') #"RBD", "RHC", "CCB". "Hard-Coded in Activity while invoking Function"
    blob_account_url = body.get('BLOB_ACCOUNT_URL') #"https://rhrhcbidev.blob.core.windows.net/". replace the storage name to rhrhcbidev or rhrbdbidev or rhccbbidev
    subscription = body.get('SUBSCRIPTION') #"74F71200-AE14-486E-A4F2-8F16649FEDD5" is the subscription for RHC. "Hard-Coded in Activity while invoking Function"
    container_name = body.get('CONTAINER_NAME') #"insights-logs-activityruns"
    blob_resource_group = body.get('BLOB_RESOURCE_GROUP') #"RHC-BI-DEV"
    blob_df_name = body.get('BLOB_RESOURCE_DF_NAME') #"RHC-BI-DEV" or "RHC-PI-DEV"
    service_now_instance = body.get('SN_INSTANCE') #"reyesdev" or "reyestest" or "reyesprod" #Global Parameter
    snowflake_db = body.get('SF_DB') #"RHC_DEV" or "RHC_UAT" or "RHC_PRD" #Global Parameter
    snowflake_user = body.get('RHC_DB_UID') #"rhc_etl_dev" or "rhc_etl_uat"  or "rhc_etl_prd"
    service_now_pwd = body.get('SERVICE_NOW_PWD')
    access_key = body.get('BLOB_ACCESS_KEY')
    rhc_blob_access_key = body.get('RHC_BLOB_ACCESS_KEY')
    snowflake_pwd = body.get('SNOWLAKE_PWD')

    service_now_api_url = "https://{}.service-now.com/api/now/table/incident".format(service_now_instance)
    service_now_uid = "BI-API"
    snowflake_warehouse = "RHC_ETL_XS_WH" #"RHC_BI_DEVELOPER_WH"

    # select existing records from table for given business unit
    rhc_account_name = "rhrhcbi{}".format(environment.lower())
    logging.info("Selecting data from Azure Table in {} storage account".format(rhc_account_name))    
    connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(rhc_account_name, rhc_blob_access_key)
    table_client = TableClient.from_connection_string(conn_str="{}".format(connection_string), table_name="ServiceNowLogTriggers")
    filter_criteria = "PartitionKey eq '{}' and RowKey eq '{}'".format(incident_business_unit, incident_business_unit)
    entities = table_client.query_entities(filter_criteria, select = "TriggerDateTime")
    for entity in entities:
        for key in entity.keys():
            logging.info("Key: {}, Value: {}".format(key, entity[key]))
            start_date_time = entity[key]

    start_date_time = datetime.datetime.strptime(start_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    start_date_time = start_date_time + datetime.timedelta(days=0, hours=0, minutes=0, seconds=0, microseconds=1)
    end_date_time = start_date_time + datetime.timedelta(days=0, hours=0, minutes=4, seconds=59, microseconds=999999)
    
    start_date_time = datetime.datetime.strftime(start_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    start_date_time = datetime.datetime.strptime(start_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    end_date_time = datetime.datetime.strftime(end_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_date_time = datetime.datetime.strptime(end_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    trigger_date_time = datetime.datetime.strftime(end_date_time,"%Y-%m-%dT%H:%M:%S.%fZ")

    logging.info("Start Date Time: {}".format(start_date_time))
    logging.info("End Date Time: {}".format(end_date_time))
    logging.info("Trigger Date Time: {}".format(trigger_date_time))
    logging.info(f"Business unit : {incident_business_unit}, ADF: {blob_df_name}, Start DateTime: {start_date_time}, End DateTime: {end_date_time}\
        , Trigger DateTime: {trigger_date_time}")

    logging.info("Updating Azure Table")    

    entity = json.dumps({'PartitionKey': incident_business_unit, 'RowKey': incident_business_unit, 'TriggerDateTime' : trigger_date_time})      
    entity = json.loads(entity)
    
    current_year = start_date_time.strftime("%Y")
    current_month = start_date_time.strftime("%m")
    current_day = start_date_time.strftime("%d")
    current_hour = start_date_time.strftime("%H")
    current_minutes = "00"

    # Get ADF pipeline/activity logs for last updated date in Azure Table
    child_container_name="insights-logs-activityruns"
    blob_location = container_name + "/resourceId=/SUBSCRIPTIONS/" + subscription + "/RESOURCEGROUPS/" + blob_resource_group + "/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/" + blob_df_name + "/y=" + current_year +"/m=" + current_month+ "/d=" + current_day + "/h=" + current_hour+ "/m=" + current_minutes
    #blob_location_child=child_container_name + "/resourceId=/SUBSCRIPTIONS/" + subscription + "/RESOURCEGROUPS/" + blob_resource_group + "/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/" + blob_df_name + "/y=" + current_year +"/m=" + current_month+ "/d=" + current_day + "/h=" + current_hour+ "/m=" + current_minutes
    blob_name = "PT1H.json"

    blob = BlobClient(account_url=blob_account_url,container_name=blob_location,blob_name=blob_name,credential=access_key,max_single_get_size=256*1024*1024, max_chunk_get_size=256*1024*1024)
    #blob_child=BlobClient(account_url=blob_account_url,container_name=blob_location_child,blob_name=blob_name,credential=access_key, max_chunk_get_size=32*1024*1024)
    logging.info(f"Checking logs in blob location {blob_location}")
    data = [] #activity_logs_data
    #child_data=[]
    

    if((blob.exists() == False)):
        logging.info("Blob Not Found: {}".format(blob_location))

    else:
        
        #etag = blob.get_blob_properties().etag
        #etag_child=blob_child.get_blob_properties().etag
        #logging.info("{} Etag of blob client: {}".format(incident_business_unit, etag))
        #logging.info("{} Etag of activity log blob client: {}".format(incident_business_unit, etag_child))

        try:
            logging.info("Pipeline logs are being downloaded")
            data = blob.download_blob().readall().decode("UTF-8").split("\n")
            #child_data=blob_child.download_blob().readall().decode("UTF-8").split("\n")
        except ResourceModifiedError as r:
            exception_msg = "{} \n {}".format(r, r.__traceback__)
            logging.error("{}: {}".format(incident_business_unit, exception_msg))
            logging.info("{}: Exception ResourceModifiedError occurred while4 downloading pipeline log".format(incident_business_unit))
            # data = blob.download_blob(etag=etag, match_condition=MatchConditions.IfModified).readall().decode("UTF-8").split("\n")
            data = blob.download_blob(etag='*', match_condition=MatchConditions.IfNotModified).readall().decode("UTF-8").split("\n")
            #data = blob_child.download_blob(etag_child='*', match_condition=MatchConditions.IfNotModified).readall().decode("UTF-8").split("\n")

            logging.info("{}: Exception ResourceModifiedError handled".format(incident_business_unit))
        except Exception as e:
            exception_msg = "{} \n {}".format(e, e.__traceback__)
            logging.error("{}: {}".format(incident_business_unit, exception_msg))
            raise Exception(e)    
        
        headers_dict = {
                        "Content-type" : "application/json",
                        "Accept" : "application/json"
                    }
        if(incident_business_unit == "RBD"):
            pipeline_business_unit = "RBD"
            business_unit_text="RBD"
        elif(incident_business_unit == "CCB"):   
            pipeline_business_unit = "COKE"
            business_unit_text="GLCCD"
        else:
            pipeline_business_unit = "RHCORP"
            business_unit_text="RH"
        logging.info("The length of the Business unit name is : {}".format(business_unit_text))    

        
        # str_select_statement = "SELECT PIPELINE_NAME FROM {}.BI_OPS.PIPELINES_SLAS WHERE BUSINESS_UNIT = '{}'".format(snowflake_db, pipeline_business_unit)
        # logging.info("Fetching data from PIPELINE_SLAS Snowflake table")
        # db_connection = snow.connect(
        #                 user=snowflFake_user,
        #                 password=snowflake_pwd,
        #                 account="reyesholdings.east-us-2.azure",
        #                 warehouse=snowflake_warehouse,
        #                 database=snowflake_db,
        #                 schema="BI_OPS")

        # # snowflake cursor object
        # cur_values = db_connection.cursor()

        # cur_values.execute(str_select_statement)
        # cur_values.get_results_from_sfqid(cur_values.sfqid)
        # pipeline_slas=cur_values.fetchall()
        # pipeline_slas_list = []
        # pipelines_covered_list = []
        # # logging.info("Adding data fetched from PIPELINE_SLAS table to list object")
        # for rec in pipeline_slas:
        #     pipeline_slas_list.append(rec[0].upper())
        # # logging.info("Added all pipelines from PIPELINE_SLAS table to list object")            
        # logging.info("Closing Cursor")
        # cur_values.close()
        # logging.info("Closing DB Connection")
        # db_connection.close()
         # select existing records from table for given business unit
    #rhc_account_name = "rhrhcbi{}".format(environment.lower())
    #logging.info("Selecting data from Azure Table in {} storage account".format(rhc_account_name))    
    #connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(rhc_account_name, rhc_blob_access_key)
        table_client_slas = TableClient.from_connection_string(conn_str="{}".format(connection_string), table_name="pipelineslastable")# Create a table client using the connection string and table name
        filter_criteria_slas = "BUSINESS_UNIT eq '{}'".format(pipeline_business_unit)# Define filter criteria for querying entities from the table
        entities_slas = table_client_slas.query_entities(filter_criteria_slas, select = "PIPELINE_NAME")# Query entities from the table based on the filter criteria and select only the PIPELINE_NAME attribute
        pipeline_slas_list=[]# Initialize lists to store pipeline names and covered pipelines
        pipelines_covered_list = []
        child_data_1=None
        for entity_slas in entities_slas:# Iterate through entities retrieved from the table
            for key in entity_slas.keys():
                #logging.info("Key: {}, Value: {}".format(key, entity_slas[key]))
                pipeline_slas_list.append(entity_slas[key].upper()) # Append pipeline names to the pipeline_slas_list
        logging.info("The length of the json data is : {}".format(pipeline_slas_list))

        for record in data:# Iterate through records in the data
            # logging.info("Blob Record Length: {}".format(len(record.strip())))
            if len(record.strip()) != 0:# Check if the record is not empty
                record_dict = json.loads(record)# Parse the record as JSON
                # logging.info("Pipeline Name in Blob Record: {}".format(record_dict["pipelineName"]))
                if record_dict["status"] == "Failed":# Check if the pipeline status is "Failed"
                    event_time=record_dict["time"][:26] + "Z"# Format event time and log pipeline failure
                    logging.info("Pipeline {} Failed at {}: ".format(record_dict["pipelineName"], event_time))
                    #if datetime.datetime.strptime(event_time,"%Y-%m-%dT%H:%M:%S.%fZ") >= start_date_time and datetime.datetime.strptime(event_time,"%Y-%m-%dT%H:%M:%S.%fZ")<= end_date_time:
                    if datetime.datetime.strptime(event_time,"%Y-%m-%dT%H:%M:%S.%fZ") >= start_date_time and datetime.datetime.strptime(event_time,"%Y-%m-%dT%H:%M:%S.%fZ")<= end_date_time:# Check if event time falls within the specified time range
                        logging.info("inside the modified condition")
                        #Pipeline {} Failed at {}: ".format(record_dict["pipelineName"], record_dict["time"][:26]))
                        if record_dict["pipelineName"].upper().strip() in pipeline_slas_list:# Check if the failed pipeline is in the pipeline_slas_list
                            logging.info("Failed Pipeline {} found in PIPELINE_SLAS table: ".format(record_dict["pipelineName"]))
                            if record_dict["pipelineName"].upper().strip() not in pipelines_covered_list:# Check if the pipeline is not already covered
                                logging.info("Creating incident because pipeline {} is not found in list of pipelines for which incident is already created in this execution".format(record_dict["pipelineName"]))
                                pipelines_covered_list.append(record_dict["pipelineName"].upper().strip())# Add pipeline to the list of covered pipelines
                                #{"datafactory":data_factory_name,"pipelineName":pipeline_name,"activityName":activity_name,"error_code":error_code,"errormessage":error_message,"eventtime":event_time}
                                fail=False
                                i=1
                                rec_count=0
                            
                                while(not fail and rec_count<=23):
                                    logging.info("Currently reading the log  {} ".format(start_date_time))
                                    if child_data_1 is  None or  check_if_activitylog_exists(child_data_1,record_dict["pipelineName"],record_dict["runId"]):#Checks if child_data_1 is  None (i.e., if failure pipeline data is already present in previous log)
                                        logging.info("downloading the activity log because it is first time or data not avilable")
                                        child_data_1=download_activity_blob(start_date_time,subscription,child_container_name,blob_resource_group,blob_account_url,access_key,blob_df_name,incident_business_unit)#If the activity log does not exist (according to the previous check)
                                    logging.info("Using the previously downloaded activity log")
                                    if child_data_1!=-1: # Checks if the downloaded blob exists (i.e., child_data_1 is not equal to -1).
                                        result_dict_data=pipe_get_activity_name(child_data_1,record_dict["pipelineName"],record_dict["runId"])#If the blob exists, it processes the data
                                    else:
                                        rec_count=rec_count+1
                                        start_date_time = start_date_time + datetime.timedelta(days=0, hours=-1, minutes=0, seconds=0, microseconds=1)
                                        logging.info("Start Date Time: {}".format(start_date_time))
                                        continue
                                    if  result_dict_data!=-1: # checking for failed log existance and if not then read new blob
                                        data_factory_name = result_dict_data.get("datafactory")
                                        pipeline_name=result_dict_data.get("pipelineName")
                                        activity_name=result_dict_data.get("activityName")
                                        error_code=result_dict_data.get("error_code","ERROR-0000")# Daefault value "ERROR-0000" if not present
                                        error_message=result_dict_data.get("errormessage","The pipeline is failed")# Default message if not present
                                        event_time=result_dict_data.get("eventtime")
                                        fail=data_factory_name and pipeline_name and activity_name# Checking if all required field are present to indicate a failed log
                                    

                                    else:
                                        fail=False
                                        rec_count=rec_count+1
                                        start_date_time = start_date_time + datetime.timedelta(days=0, hours=-1, minutes=0, seconds=0, microseconds=1)
                                        logging.info("Start Date Time: {}".format(start_date_time))

    
                                if not fail:
                                    logging.info("The parent failed pipeline data is not found in activity logs for previous 24 hours")
                                   # logging.info("Error message {} ".format(str(e)))
                                    continue

                                logging.info("Failed Pipeline: {}".format(pipeline_name))
                                logging.info("Failed Activity: {}".format(activity_name))
                                logging.info("Failed Time Before Conversion: {}".format(event_time[:26]))
                                local_time_zone = pytz.timezone('America/Chicago')
                                event_time = parser.parse(event_time).astimezone(local_time_zone)
                                logging.info("Failed Time After Conversion: {}".format(event_time))

                                short_description = "ADF {} - ETL CHK - {} PIPELINE FAILED AT {} ACTIVITY".format(pipeline_business_unit.upper(), pipeline_name.upper(), activity_name.upper())

                                description = "Data Factory: " + data_factory_name + "\r\n"
                                description = description + "Pipeline Name: " + pipeline_name + "\r\n"
                                #description = description + "Activity Name: " + activity_name + "\r\n"
                                description = description + "Event Time: " + datetime.datetime.strftime(event_time,'%Y-%m-%d %H:%M:%S') + "\r\n"
                                description = description + "Error Code: " + error_code + "\r\n"
                                description = description + "Error Message: " + error_message
                    
                                body_dict =  {"short_description":"{}".format(short_description),
                                        "caller_id":"{}".format(service_now_uid),
                                        "assignment_group":"BI - Ops",
                                        "contact_type":"Proactive Monitoring",
                                        "u_contact_number":"6303108313",
                                        "impact":"3 - One Site",
                                        "urgency":"3 - Minor Inconvenience",
                                        "category":"Application",
                                        "subcategory":"General Issue/Error",
                                        "cmdb_ci":"Snowflake",
                                        "description":"{}".format(description),
                                        "u_impacted_business_unit": "{}".format(business_unit_text),
                                        
                                        }
                                str_body = json.dumps(body_dict)

                                api_response = requests.post(url=service_now_api_url, auth=(service_now_uid, service_now_pwd), headers=headers_dict, data=str_body)
                                logging.info("API Response: {}".format(api_response))
                                #get_incident= requests.get(url=service_now_api_url, auth=(service_now_uid, service_now_pwd), headers=headers_dict)

                                #logging.info(" The incident json is ".format(get_incident))
                                logging.info("Incident created for {}".format(short_description))
                            else:
                                logging.info("Incident already created for pipeline {}".format(record_dict["pipelineName"]))
                        else:
                            logging.info("Failed Pipeline {} not found in PIPELINE_SLAS table, continuing... ".format(record_dict["pipelineName"]))
    table_client.update_entity(mode = UpdateMode.REPLACE, entity = entity)# Update entity in Azure Table
    logging.info("Azure Table updated successfully")
    logging.info("Returning body")
    
    return body
