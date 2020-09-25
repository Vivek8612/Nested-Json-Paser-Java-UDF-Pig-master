set tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
Register 'hdfs:///lib/idm-pig/idm-pig-hadoop2-1.1.0.jar';
Register 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar';
Register 'hdfs:///lib/avro/1.7.5/avro-1.7.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/vkumar/udf/get_account_ids_v5.py' using jython as fn_acc_id;
REGISTER 'hdfs:///user/preddy/CapOne/cust_json_parser.py' using jython as cust_json_praser;
REGISTER 'hdfs:///user/vkumar/udf/get_all_account_details_v4.py' using jython as get_all_account_details_v4;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
%DECLARE MONTH '{12}'
%DECLARE DAYS '{14}'
--%DECLARE HOURS '{00,01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16}'

--data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/hour=$HOURS/*/*.avro' using LOAD_IDM;
data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

tmp = LIMIT data 10;
DUMP tmp;

f1 = FILTER data BY (
					specificEventType == 'WebCustomEvent' or 
                    specificEventType == 'SessionTieEvent' or 
                    specificEventType == 'WebPageLoadEvent' or 
                    specificEventType == 'OnlineInvitationOfferEvent' or 
                    specificEventType == 'OnlineSessionStartEvent' or 
                    specificEventType == 'WebSessionStartEvent' or 
                    specificEventType == 'TargetDeterminationEvent' or 
                    specificEventType == 'OnlineInvitationResponseEvent' or 
                    specificEventType == 'OnlineScreenLoadEvent' or 
                    specificEventType == 'OnlineSurveySubmitEvent' or 
                    specificEventType == 'OnlineInteractionRequestEvent' or 
                    specificEventType == 'OnlineScreenUnloadEvent' or 
                    specificEventType == 'OnlineInteractionConnectionToAgentEvent' or 
                    specificEventType == 'OnlineSessionEngageEvent' or 
                    specificEventType == 'OnlineSessionEndEvent' or 
                    specificEventType == 'WebEnvironmentNotSupportedEvent' or 
                    specificEventType == 'OnlineInteractionRequestInQueueEvent' or 
                    specificEventType == 'ChatAvailabilityCheckEvent' or 
                    specificEventType == 'OnlineInteractionTransferEvent';
tmp = LIMIT f1 10;
DUMP tmp;



--f1 = FILTER data BY specificEventType == 'WebPageLoadEvent';
--tmp = LIMIT f1 10;
--DUMP tmp;

f2 = FOREACH f1 GENERATE 
							specificEventType as eventType,
                            header.eventType as eventName,
                            header.timeEpochMillisUTC as time, 
							header.associativeTag as vi, 
							header.channelSessionId as bsid, 
							body#'sourceCat' as sourceCat,
							custom#'sourceCat' as sourceCat_cust,
							body#'Queue' as Queue,
							custom#'Queue' as Queue_cust,
							body#'custRefId' as Cust_RefId,
							custom#'custRefId' as Cust_RefId_cust,
							body#'profileReferenceID' as Prof_RefId,
							custom#'profileReferenceID' as Prof_RefId_cust,
							body#'Browser' as Browser, 
							custom#'Browser' as Browser_cust, 
							body#'DeviceDetails' as DeviceDetails,
							custom#'DeviceDetails' as DeviceDetails_cust,
							body#'City' as City,
							custom#'City' as City_cust,
							body#'ipAddress' as IP,
							custom#'ipAddress' as IP_cust;
  
tmp = LIMIT f2 10;
DUMP tmp;


f3 = FILTER f2 BY 
				(Queue == 'cap1enterprise-account-default-queue-bank-service-english' or 
                Queue == 'cap1enterprise-account-default-queue-bank360Bank-servicing-English' or 
                Queue_cust == 'cap1enterprise-account-default-queue-bank-service-english' or 
                Queue_cust == 'cap1enterprise-account-default-queue-bank360Bank-servicing-English' or 
                sourceCat == 'cap1enterprise-account-default-queue-bank-service-english' or 
                sourceCat == 'cap1enterprise-account-default-queue-bank360Bank-servicing-English' or 
                sourceCat_cust == 'cap1enterprise-account-default-queue-bank-service-english' or 
                sourceCat_cust == 'cap1enterprise-account-default-queue-bank360Bank-servicing-English');
tmp = LIMIT f3 10;
DUMP tmp;


f4 = GROUP f3 BY bsid;

f5 = FOREACH f4 GENERATE FLATTEN (group), COUNT(f3); 
tmp = LIMIT f5 10;
DUMP tmp;

