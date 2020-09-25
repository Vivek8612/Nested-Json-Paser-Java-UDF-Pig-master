--set tez.staging-dir '/tmp/vkumar/staging';
--SET tez.queue.name 'dsg'; 

Register 'hdfs:///lib/avro/1.7.5/avro-1.7.5.jar';
REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar';
Register 'hdfs:///lib/idm-pig/idm-pig-hadoop2-1.1.0.jar';
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/preddy/CapOne/queue_UDF.py' using jython as func;
Register 'hdfs:///user/vkumar/udf/timeFunctions.py' using jython as time;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

----------------------------------------------------------------------Nov 2017------------------------------------------------------------
%DECLARE YEAR '2017'

%DECLARE MONTH '{11,12}'

--%DECLARE DAY '{03}'

--------------------------------------------
---------      Loading Data      -----------
--------------------------------------------
starGateUUID = LOAD '/user/vkumar/IVR_UUID_01st_Nov17to31st_Dec17_IDMLogs.csv' using PigStorage(',') As (CHANNELSESSIONID: chararray);

data = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/*/*/*/*.avro' using LOAD_IDM;
tmp = LIMIT data 10;
DUMP tmp;

--------------------------------------------
---------    Loading StartCall   -----------
--------------------------------------------
startFilter = FILTER data BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE 
								SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), '.', 1)) as time,
                                --ToDate((long)header.timeEpochMillisUTC) as startTime,
                                header.channelSessionId as uuid, 
                                body#'ani',
                                body#'dnis';
tmp = LIMIT startInfo 10;
DUMP tmp;


ivrStartCall = JOIN starGateUUID BY CHANNELSESSIONID,
				startInfo BY uuid;
tmp = LIMIT ivrStartCall 10;
DUMP tmp;


--------------------------------------------
---------     Loading AppLog     -----------
--------------------------------------------
appLogFilter = FILTER data BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE 
									SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), '.', 1)) as time, 
                                    --ToDate((long)header.timeEpochMillisUTC),
                                    header.channelSessionId as uuid, 
                                    body#'label' as logtag, 
                                    body#'optMessage' as value;
tmp = LIMIT appLogInfo 10;
DUMP tmp;


--------------------------------------------
---------      Loading AccID     -----------
--------------------------------------------
accIdVaraible_01 = FILTER appLogInfo BY logtag MATCHES '.*com.tellme.proprietary.capitalone.bank.customerIdentityInq.AccountID.*';
tmp = LIMIT accIdVaraible_01 10;
DUMP tmp;
--accIdVaraible_01_Count = FOREACH (GROUP accIdVaraible_01 ALL) GENERATE COUNT_STAR(accIdVaraible_01); 
--DUMP accIdVaraible_01_Count;

infoJoin = JOIN ivrStartCall BY CHANNELSESSIONID,
				accIdVaraible_01 BY uuid;
tmp = LIMIT infoJoin 10;
DUMP tmp;

/*infoJoinUID = FOREACH infoJoin GENERATE 
										$0, $1, $2, $3, $4, $5, $6, $7,
										(($1 is null or $1 == '') ? (($5 is null or $5 == '') ? null : $5) : $1) as uniqueUUID;
tmp = LIMIT infoJoinUID 10;
DUMP tmp;
*/
--------------------------------------------
---------  Loading CallVariable  -----------
--------------------------------------------
callVaraible_01 = FILTER appLogInfo BY logtag == 'com..tellme.proprietary.capitalone.bank.callvariable1';
tmp = LIMIT accIdVaraible_01 10;
DUMP tmp;

callVariableJoin = JOIN infoJoin BY CHANNELSESSIONID,
						callVaraible_01 BY uuid;
tmp = LIMIT callVariableJoin 10;
DUMP tmp;

/*callVariableJoinUID = FOREACH callVariableJoin GENERATE 
										$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
										(($8 is null or $8 == '') ? (($10 is null or $10 == '') ? null : $10) : $8) as uniqueUUID_01;
tmp = LIMIT callVariableJoinUID 10;
DUMP tmp;
*/
--------------------------------------------
---------  Loading TranMenuUUID  -----------
--------------------------------------------
transactionMenu_01 = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.bank.access_transactions.access_transactions_menu';
tmp = LIMIT transactionMenu_01 10;
DUMP tmp;

transactionMenuJoin = JOIN callVariableJoin BY CHANNELSESSIONID,
							transactionMenu_01 BY uuid;
tmp = LIMIT transactionMenuJoin 10;
DUMP tmp;

/*transactionMenuJoinUID = FOREACH transactionMenuJoin GENERATE 
										$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
										(($13 is null or $13 == '') ? (($15 is null or $15 == '') ? null : $15) : $13) as uniqueUUID_02;
tmp = LIMIT transactionMenuJoinUID 10;
DUMP tmp;
*/

--------------------------------------------
---------Loading TransferFundUUID-----------
--------------------------------------------
TransferFund_01 = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.bank.transfer_funds_source';
tmp = LIMIT TransferFund_01 10;
DUMP tmp;

TransferFundJoin = JOIN transactionMenuJoin BY CHANNELSESSIONID,
						TransferFund_01 BY uuid;
tmp = LIMIT TransferFundJoin 10;
DUMP tmp;

/*TransferFundJoinUID = FOREACH TransferFundJoin GENERATE 
										$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
										(($18 is null or $18 == '') ? (($20 is null or $20 == '') ? null : $20) : $18) as uniqueUUID_02;
tmp = LIMIT TransferFundJoinUID 10;
DUMP tmp;

*/
--------------------------------------------
---------Loading TransferFundUUID-----------
--------------------------------------------
aniSpoofing_01 = FILTER appLogInfo BY logtag == 'capitalone.bank.ANISpoofing.Parameters'; 	
tmp = LIMIT aniSpoofing_01 10;
DUMP tmp;

aniSpoofingJoin = JOIN TransferFundJoin BY CHANNELSESSIONID,
						aniSpoofing_01 BY uuid;

STORE aniSpoofingJoin INTO 'IVR_UUID_01st_Nov17to31st_Dec17_IDMLogs.tsv';

