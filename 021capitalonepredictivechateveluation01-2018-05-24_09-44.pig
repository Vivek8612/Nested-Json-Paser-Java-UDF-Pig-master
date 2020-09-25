/*SET tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 
/*
--------------------------------------------
---------   Setting Memory Size  -----------
--------------------------------------------
set mapreduce.map.memory.mb    38400
set mapreduce.reduce.memory.mb 38400
set tez.am.resource.memory.mb  8192
*/
--------------------------------------------
---------    Registering jars    -----------
--------------------------------------------
REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/jjonnada/extract_intent.py' using org.apache.pig.scripting.jython.JythonScriptEngine as extract;
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/preddy/CapOne/get_account_ids_v4.py' using jython as fn_acc_id;
REGISTER 'hdfs:///user/preddy/CapOne/webpage_UDF_v2.py' using jython as webpage_udf;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

--------------------------------------------
---------      Loading Data      -----------
--------------------------------------------
%DECLARE YEAR '2018'
%DECLARE MONTH01 '{04}'
%DECLARE MONTH02 '{05}'
%DECLARE MONTH03 '{06}'

%DECLARE WDAY01 '{26,27,28,29,30}'
%DECLARE WDAY02 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

%DECLARE SDAY01 '{26,27,28,29,30}'
%DECLARE SDAY02 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'
%DECLARE SDAY03 '{01,02,03,04}'

wdata01 = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH01/day=$WDAY01/*/*/*.avro' using LOAD_IDM;
wdata02 = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH02/day=$WDAY02/*/*/*.avro' using LOAD_IDM;
wdata = UNION wdata01, wdata02;

sdata01 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH01/day=$SDAY01/*/*/*.avro' using LOAD_IDM;
sdata02 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH02/day=$SDAY02/*/*/*.avro' using LOAD_IDM;
sdata03 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH03/day=$SDAY03/*/*/*.avro' using LOAD_IDM;

sdata04 = UNION sdata01, sdata02;
sdata = UNION sdata04, sdata03;

--------------------------------------------
---------  Loading HotLeadData   -----------
--------------------------------------------
HotLeadFilter = FILTER wdata BY specificEventType == 'TargetDeterminationEvent';

HotLeadInfo = FOREACH HotLeadFilter GENERATE 
											header.timeEpochMillisUTC as time,
											header.channelSessionId as csid,
											custom#'profileReferenceID' as prID:chararray,
											custom#'pageURL' as pageURL:chararray,
											custom#'RuleId' as ruleID;

HotLeadInfoFilter = FILTER HotLeadInfo BY (ruleID == 'PR_Ease_Predictive_CG_V01' or ruleID == 'PR_Ease_Predictive_CG_V01_cg' or ruleID == 'PR_Ease_Predictive_CG_V02' or ruleID == 'PR_Ease_Predictive_CG_V02_cg' or ruleID == 'PR_Ease_Predictive_V01' or ruleID == 'PR_Ease_Predictive_V01_cg' or ruleID == 'PR_Ease_Predictive_V02' or ruleID == 'PR_Ease_Predictive_V02_cg');

HotLeadInfoDistinct = DISTINCT HotLeadInfoFilter;

STORE HotLeadInfoDistinct INTO '/user/test/capitalone/cap1_ease_hotlead_26Apr_29May_20180606.tsv';

--------------------------------------------
---------  Loading User Profile  -----------
--------------------------------------------
userProfileFilter = FILTER wdata BY specificEventType == 'WebCustomEvent';
userProfileInfo = FOREACH userProfileFilter GENERATE 
						header.channelSessionId as bsid, 
						custom#'userProfile' as user_profile:chararray;

userProfile = FILTER userProfileInfo BY (user_profile !='' and user_profile is not null);

STORE userProfile INTO '/user/test/capitalone/cap1_ease_userprofile_26Apr_29May_20180606.tsv';

--------------------------------------------
-------- Processing for SPEECH MAP----------
--------------------------------------------
userProfile_Preprocess = FOREACH userProfile GENERATE bsid, user_profile;

userProfile_Filtered = FILTER userProfile_Preprocess BY (user_profile !='' and user_profile is not null);

userProfile_Extracted = FOREACH userProfile_Filtered GENERATE bsid, fn_acc_id.extract_account_ids($0,$1) as bsid_acc_ids;

bsid_account_ids = FOREACH userProfile_Extracted GENERATE $0 as csid;
bsid_account_ids_01 = DISTINCT bsid_account_ids;

userProfile_Extracted_01 = FOREACH userProfile_Extracted GENERATE FLATTEN($1);
userProfile_Extracted_01 = DISTINCT userProfile_Extracted_01;
userProfile_Extracted_02 = FOREACH userProfile_Extracted_01 GENERATE $0 as csidNprID, $1 as accound_id;

--------------------------------------------
---------   Loading SPEECH Logs  -----------
--------------------------------------------
startFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE header.timeEpochMillisUTC as startTime,header.channelSessionId as uuid, body#'ani',body#'dnis';

appLogFilter = FILTER sdata BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE 
										header.timeEpochMillisUTC as time,
										header.channelSessionId as uuid, 
										body#'label' as logtag, 
										body#'optMessage' as message;

custIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.customerid';
acctIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.accountid';
agentTransferFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.disposition.agenttransfer'; 

infoJoin = JOIN 
				startInfo BY uuid, 
				custIdFilter BY uuid, 
				acctIdFilter BY uuid,
				agentTransferFilter BY uuid;


speechInfo = FOREACH infoJoin GENERATE 
									$0 as startTime,
									$1 as uuid,
									$2 as ani, 
									$3 as dnis,
									$7 as custId,
									$11 as acctId,
									$14 as agentTransferState;


speechInfoDistinct = DISTINCT speechInfo;

speechInfoAccountIdJoin = JOIN 
								userProfile_Extracted_02 BY $1, 
								speechInfo BY acctId;

STORE speechInfoAccountIdJoin INTO '/user/test/capitalone/cap1_ease_ivragenttransfer_26Apr_29May_20180606.tsv';

