SET tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

--------------------------------------------
---------   Setting Memory Size  -----------
--------------------------------------------
set mapreduce.map.memory.mb    38400
set mapreduce.reduce.memory.mb 38400
set tez.am.resource.memory.mb  8192

--------------------------------------------
---------    Registering jars    -----------
--------------------------------------------
REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';

REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/jjonnada/extract_intent.py' using org.apache.pig.scripting.jython.JythonScriptEngine as extract;

REGISTER 'hdfs:///user/preddy/CapOne/webpage_UDF_v2.py' using jython as webpage_udf;
REGISTER 'hdfs:///user/preddy/CapOne/get_account_ids_v4.py' using jython as fn_acc_id;
REGISTER 'hdfs:///user/vkumar/udf/web2ivr_UDF_v3.py' using jython as web2ivr_udf;


DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

--------------------------------------------
---------      Loading Data      -----------`
--------------------------------------------
%DECLARE YEAR '2018'
%DECLARE MONTH_01 '{01}'
%DECLARE MONTH_02 '{02}'
%DECLARE DAY '{24,25,26,27,28,29,30}'
%DECLARE SDAY_01 '{24,25,26,27,28,29,30,31}'
%DECLARE SDAY_02 '{01}'

data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH_01/day=$DAY/*/*/*.avro' using LOAD_IDM;

sdata_01 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH_01/day=$SDAY_01/*/*/*.avro' using LOAD_IDM;
sdata_02 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH_02/day=$SDAY_02/*/*/*.avro' using LOAD_IDM;
sdata = UNION sdata_01, sdata_02;

----------------------
-- Account IDs
---------------------
f1 = FILTER data BY specificEventType == 'WebCustomEvent';
f2 = FOREACH f1 GENERATE header.channelSessionId as bsid, custom#'userProfile' as user_profile;
f3 = FILTER f2 BY (user_profile !='' and user_profile is not null);
f4 = FOREACH f3 GENERATE bsid, fn_acc_id.extract_account_ids($0,$1) as bsid_acc_ids;

bsid_account_ids = FOREACH f4 GENERATE $0 as bsid;
bsid_account_ids = DISTINCT bsid_account_ids;

f5 = FOREACH f4 GENERATE FLATTEN($1);
f5 = DISTINCT f5;

----------------------
-- WEB Logs
---------------------
pageLoadFilter = FILTER data BY specificEventType == 'WebPageLoadEvent';
pageLoadInfo = FOREACH pageLoadFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid, body#'pageTitle' as pageTitle, body#'pageURL' as pageUrl, body#'referrerURL' as referrerURL, custom#'Queue' as queue;
pageLoadGroup = GROUP pageLoadInfo BY $1;
pageLoadGroupSorted = FOREACH pageLoadGroup {
                                            distinctData = DISTINCT pageLoadInfo;
                                            sortData = ORDER distinctData BY $0;
                                            GENERATE group as csid, sortData as pageDetails;
};


/*
pageLoadGroupSortedMASRF = FOREACH pageLoadGroupSorted GENERATE *, webpage_udf.convertBagToStr($1) as queue_concat;
pageLoadGroupSortedMASRF2 = FILTER pageLoadGroupSortedMASRF BY ($2 matches '.*=/statements__.*') OR ($2 matches '.*=/statements') 
OR ($2 matches '.*=/moreaccountservicesmodal__.*') OR ($2 matches '.*=/moreaccountservicesmodal') 
OR ($2 matches '.*capitalone.com/profile__.*') OR ($2 matches '.*capitalone.com/profile') 
OR ($2 matches '.*=/pay__.*') OR ($2 matches '.*=/pay') 
OR ($2 matches '.*capitalone.com/settings__.*') OR ($2 matches '.*capitalone.com/settings') 
OR ($2 matches '.*=/payment__.*') OR ($2 matches '.*=/payment') 
OR ($2 matches '.*=/reportfraud__.*') OR ($2 matches '.*=/reportfraud') 
OR ($2 matches '.*capitalone.com/security__.*') OR ($2 matches '.*capitalone.com/security') 
OR ($2 matches '.*capitalone.com/alerts__.*') OR ($2 matches '.*capitalone.com/alerts') 
OR ($2 matches '.*capitalone.com/verify__.*') OR ($2 matches '.*capitalone.com/verify') 
OR ($2 matches '.*=/missingcard__.*') OR ($2 matches '.*=/missingcard');
pageLoadGroupSortedMASRF3 = FOREACH pageLoadGroupSortedMASRF2 GENERATE $0 as csid, $1 as pageDetails;
*/
pageLoadAccountJoin = JOIN bsid_account_ids BY $0, pageLoadGroupSorted BY $0;
pageLoadAccountJoinDistinct = DISTINCT pageLoadAccountJoin;

----------------------
-- SPEECH Logs
---------------------
startFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE header.timeEpochMillisUTC as startTime,header.channelSessionId as uuid, body#'ani',body#'dnis';

appLogFilter = FILTER sdata BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as uuid, body#'label' as logtag, body#'optMessage' as message;

custIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.customerid';
acctIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.accountid';


infoJoin = JOIN startInfo BY uuid, custIdFilter BY uuid, acctIdFilter BY uuid;


speechInfo = FOREACH infoJoin GENERATE $0 as startTime,$1 as uuid,$2 as ani, $3 as dnis,$7 as custId,$11 as acctId;
speechInfoDistinct = DISTINCT speechInfo;
speechInfoAccountIdJoin = JOIN f5 BY $1, speechInfo BY acctId;

----------------------
-- DISPOSITION Filter
---------------------
speechInfoAccountIdJoin_uuid = FOREACH speechInfoAccountIdJoin GENERATE $3 as uuid;
speechInfoAccountIdJoin_uuid = DISTINCT speechInfoAccountIdJoin_uuid;

dispFilter = FILTER appLogFilter BY header.eventType == 'SpeechPlatformEvent';
dispFilter2 = FOREACH dispFilter GENERATE header.channelSessionId AS uuid, header.timeEpochMillisUTC as event_time, header.optSequence as sequence, body#'label' as logtag, body#'optMessage' as message;

speechAccountDispJoin = JOIN dispFilter2 BY $0, speechInfoAccountIdJoin_uuid BY $0;
speechAccountDispJoinDistinct = DISTINCT speechAccountDispJoin;

speechAccountDispGroup = GROUP speechAccountDispJoinDistinct BY $0;
speechAccountDispGroupSorted = FOREACH speechAccountDispGroup {
                by_seq = ORDER speechAccountDispJoinDistinct BY sequence ASC;
    outval = extract.get_intent_from_applog(by_seq);
    --generate group as uuid_intent, outval.intent, outval.disposition,outval.logtype;
    generate group as uuid_intent, outval.disposition;
};

--speechAccountAgentTransfer = FILTER speechAccountDispGroupSorted BY $1 == 'agenttransfer';
speechAccountAgentTransfer = FOREACH speechAccountDispGroupSorted GENERATE $0, $1;
speechAccountAgentTransfer = DISTINCT speechAccountAgentTransfer;

----------------------
-- Merging Web & IVR
---------------------
--speechInfoPageLoadJoin = JOIN pageLoadAccountJoinDistinct BY $0, speechInfoAccountIdJoin BY $0;
-- (speechInfoAccountIdJoin) --> f5(bsid, acc_id), speechInfo(startTime, uuid, ani, dnis, custId, acctId)
-- (speechAccountAgentTransfer) --> (bsid, disp)
-- (speechInfoAccountIdJoin2) --> (bsid, acc_id, startTime, uuid, ani, dnis, custId, acctId, bsid, disp)
-- (pageLoadAccountJoinDistinct) --> (bsid, bsid, pageDetails)
-- (speechInfoPageLoadJoin) -->  (bsid, bsid, pageDetails, bsid, acc_id, startTime, uuid, ani, dnis, custId, acctId, bsid, disp)
speechInfoAccountIdJoin2 = JOIN speechInfoAccountIdJoin BY $3, speechAccountAgentTransfer BY $0;

speechInfoPageLoadJoin = JOIN pageLoadAccountJoinDistinct BY $0, speechInfoAccountIdJoin2 BY $0;

webSpeechInfo = FOREACH speechInfoPageLoadJoin GENERATE $0 as web_csid, $2 as webPages, $4 as w_acctid, $9 as w_cid, $5 as speechStartTime, $6 as speech_uuid, $12 as disposition;
webSpeechInfo2 = FOREACH webSpeechInfo GENERATE web_csid, web2ivr_udf.get_web2ivr_details(webPages, speechStartTime) as web2ivr_map, 
w_acctid, w_cid, speechStartTime, speech_uuid, disposition;

webSpeechInfo3 = FOREACH  webSpeechInfo2 GENERATE 
web_csid, 
web2ivr_map#'max_page_num' as max_page_num, 
web2ivr_map#'last_pg_url' as last_pg_url, 
web2ivr_map#'last_pg_time' as last_pg_time, 
web2ivr_map#'last_pg_queue' as last_pg_queue, 
web2ivr_map#'omni_flag' as omni_flag, 
w_acctid, 
w_cid, 
speechStartTime, 
speech_uuid,
disposition;

webSpeechInfo3 = FILTER webSpeechInfo3 BY ($2 is not null and TRIM($2) != '' );

-------------------------
-- Removing FP BSIDs
------------------------
webSpeechInfo3_fmt = FOREACH webSpeechInfo3 GENERATE *, (speechStartTime-last_pg_time)/1000 as time_diff_sec;

webSpeechInfo3_fmt2 = FOREACH (GROUP webSpeechInfo3_fmt BY web_csid) {
                                                                       sort_data = ORDER webSpeechInfo3_fmt BY web_csid, time_diff_sec;
                                                                       first = LIMIT sort_data 1;
                                                                       GENERATE FLATTEN(first);
                                                                     };

webSpeechInfo3_fmt3 = FOREACH (GROUP webSpeechInfo3_fmt2 BY speech_uuid) {
                                                                           sort_data = ORDER webSpeechInfo3_fmt2 BY speech_uuid, time_diff_sec;
                                                                           first = LIMIT sort_data 1;
                                                                           GENERATE FLATTEN(first);
                                                                         };

DESCRIBE webSpeechInfo3_fmt3;


STORE webSpeechInfo3_fmt3 INTO 'cap1_ease_we2ivr_agent_transfer_11_17jan_2018_20012018.tsv';
