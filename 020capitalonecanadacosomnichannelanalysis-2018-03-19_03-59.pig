--------------------------------------------
---------      tez Settings      -----------
--------------------------------------------
SET tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

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

REGISTER 'hdfs:///user/preddy/CapOne/queue_UDF.py' using jython as func;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

--------------------------------------------
---------      Loading Data      -----------
--------------------------------------------
%DECLARE YEAR '2018'
%DECLARE MONTH '{03}'
%DECLARE MONTH01 '{04}'
-- %DECLARE DAYS '{15}'
%DECLARE WDAYS '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

%DECLARE SDAYS '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'
%DECLARE SDAYS01 '{01,02,03,04,05,06,07}'

-- %DECLARE DAYS '{01}'

-- web_log_data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
-- web_log_data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

-- data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$WDAYS/*/*/*.avro' using LOAD_IDM;

-- sdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;
sdata01 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$SDAYS/*/*/*.avro' using LOAD_IDM;
sdata02 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH01/day=$SDAYS01/*/*/*.avro' using LOAD_IDM;
sdata = UNION sdata01, sdata02;

--------------------------------------------
---------  Loading User Profile  -----------
--------------------------------------------
-- infoGeo = FOREACH data GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid_raw,specificEventType,body,custom;
-- reqFilterGeo = FILTER infoGeo BY $2=='OnlineSessionStartEvent';
-- reqFilterExtractGeo = FOREACH reqFilterGeo GENERATE csid_raw, body#'ipCountry' as ipCountry;
-- csidCanada = FILTER reqFilterExtractGeo BY $1 =='Canada';
-- csidCanada = FOREACH csidCanada GENERATE csid_raw;
-- csidCanada2 = FOREACH csidCanada GENERATE *, STRSPLIT (csid_raw,'@@',2) as uniqId;
-- csidCanada3 = FOREACH csidCanada2 GENERATE uniqId.$0 as csid;
-- csidCanadaDistinct = DISTINCT csidCanada3;


customEventFilter = FILTER data BY specificEventType == 'WebCustomEvent' and body#'customEventType' == '100010';

--tmp = LIMIT customEventFilter 10;
--DUMP tmp;

accountInfo = FOREACH customEventFilter GENERATE header.channelSessionId as csid, custom#'accid' as w_acctid, custom#'ci' as w_cid;
accountInfo = FILTER accountInfo BY (csid !='' and csid is not null);

--tmp = LIMIT accountInfo 10;
--DUMP tmp;


--------------------------------------------
---------  Loading PageLoadData  -----------
--------------------------------------------
pageLoadFilter = FILTER data BY specificEventType == 'WebPageLoadEvent';
pageLoadInfo = FOREACH pageLoadFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid, body#'pageTitle' as pageTitle, body#'pageURL' as pageUrl, body#'referrerURL' as referrerURL, custom#'Queue' as queue;
pageLoadGroup = GROUP pageLoadInfo BY $1;
pageLoadGroupSorted = FOREACH pageLoadGroup {
											distinctData = DISTINCT pageLoadInfo;
                                            sortData = ORDER distinctData BY $0 DESC;
											first = LIMIT sortData 1;
                                            GENERATE group as csid, FLATTEN(first);
};
--tmp = LIMIT pageLoadGroupSorted 10;
--DUMP tmp;


--pageLoadGroupSortedCanada = FOREACH pageLoadGroupSorted GENERATE *, func.convertBagToStr($1) as queue_concat;
pageLoadGroupSortedCanada2 = FILTER pageLoadGroupSorted BY ($6 matches '.*Canada.*');
--tmp = LIMIT pageLoadGroupSortedCanada2 10;
--DUMP tmp;


pageLoadGroupSortedCanada2 = FOREACH pageLoadGroupSortedCanada2 GENERATE 
																		$0 as csid, 
																		$1 as time, 
																		$3 as pageTitle,
																		$4 as pageUrl,
																		$6 as queue;

pageLoadGroupSortedCanada3 = FOREACH pageLoadGroupSortedCanada2 GENERATE $0 as csid;
pageLoadGroupSortedCanada4 = DISTINCT pageLoadGroupSortedCanada3;

WebSessionCount = FOREACH (GROUP pageLoadGroupSortedCanada4 ALL) GENERATE group, COUNT_STAR(pageLoadGroupSortedCanada4);
DUMP WebSessionCount;

--------------------------------------------
--------  Processing for WEB MAP  ----------
--------------------------------------------
pageLoadAccountJoin = JOIN pageLoadGroupSortedCanada2 BY $0 LEFT OUTER,
							accountInfo BY $0;

--tmp = LIMIT pageLoadAccountJoin 10;
--DUMP tmp;


pageLoadAccountJoinDistinct = FILTER pageLoadAccountJoin BY ($5 !='' and $5 is not null);
pageLoadAccountJoinDistinct = DISTINCT pageLoadAccountJoinDistinct;
tmp = LIMIT pageLoadAccountJoinDistinct 10;
DUMP tmp;

WebSessionWithAccID_01 = FOREACH pageLoadAccountJoinDistinct GENERATE $0 as csid;
WebSessionWithAccID_02 = DISTINCT WebSessionWithAccID_01;
WebSessionWithAccID = FOREACH (GROUP WebSessionWithAccID_02 ALL) GENERATE group, COUNT_STAR(WebSessionWithAccID_02);
DUMP WebSessionWithAccID;


--------------------------------------------
---------   Loading SPEECH Logs  -----------
--------------------------------------------
startFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE header.timeEpochMillisUTC as startTime,header.channelSessionId as uuid, body#'ani',body#'dnis';

appLogFilter = FILTER sdata BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as uuid, body#'label' as logtag, body#'optMessage' as message;

custIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.customerid';
acctIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.accountid';

infoJoin = JOIN 
				startInfo BY uuid, 
				custIdFilter BY uuid, 
				acctIdFilter BY uuid;


speechInfo = FOREACH infoJoin GENERATE $0 as startTime,$1 as uuid,$2 as ani, $3 as dnis,$7 as custId,$11 as acctId;
speechInfoDistinct = DISTINCT speechInfo;

speechInfoPageLoadJoin = JOIN 
								pageLoadAccountJoinDistinct BY $6, 
								speechInfoDistinct BY acctId;

tmp = LIMIT speechInfoPageLoadJoin 10;
DUMP tmp;


--------------------------------------------
-------- Processing for SPEECH MAP----------
--------------------------------------------
webSpeechInfo = FOREACH speechInfoPageLoadJoin GENERATE 
														$0 as web_csid, 
														$1 as webPageLoadTime, 
														$2 as pageTitle,
														$3 as pageURL,
														$4 as queue,
														$6 as w_acctid, 
														$7 as w_cid, 
														$8 as speechStartTime, 
														$9 as speech_uuid,
														$10 as ani,
														$11 as dnis,
														$12 as custId,
														$13 as acctId,
														($8-$1)/1000 as time_diff_sec;

tmp = LIMIT webSpeechInfo 10;
DUMP tmp;


--------------------------------------------
------- Applying DISPOSITION Filter---------
--------------------------------------------

speechInfoAccountIdJoin_uuid = FOREACH webSpeechInfo GENERATE $8 as uuid;
speechInfoAccountIdJoin_uuid01 = FILTER speechInfoAccountIdJoin_uuid BY (uuid !='' and uuid is not null);
speechInfoAccountIdJoin_uuid02 = DISTINCT speechInfoAccountIdJoin_uuid01;

tmp = LIMIT speechInfoAccountIdJoin_uuid02 10;
DUMP tmp;

dispFilter = FILTER appLogFilter BY header.eventType == 'SpeechPlatformEvent';
dispFilter2 = FOREACH dispFilter GENERATE 
										header.channelSessionId as uuid, 
										header.timeEpochMillisUTC as event_time, 
										header.optSequence as sequence, 
										body#'label' as logtag, 
										body#'optMessage' as message;

speechAccountDispJoin = JOIN 
							dispFilter2 BY $0,
							speechInfoAccountIdJoin_uuid02 BY $0;

speechAccountDispJoin01 = FILTER speechAccountDispJoin BY ($5 != '' and $5 is not null and $5 != 'null' and $5 != 'UNKNOWN');
							
speechAccountDispJoinDistinct = DISTINCT speechAccountDispJoin01;

speechAccountDispGroup = GROUP speechAccountDispJoinDistinct BY $0;
--tmp = LIMIT speechAccountDispGroup 10;
--DUMP tmp;


speechAccountDispGroupSorted = FOREACH speechAccountDispGroup {
																by_seq = ORDER speechAccountDispJoinDistinct BY sequence ASC;
																outval = extract.get_intent_from_applog(by_seq);
																generate group as uuid_intent, outval.intent, outval.disposition,outval.logtype;
																--generate group as uuid_intent, outval.disposition;
};


--tmp = LIMIT speechAccountDispGroupSorted 10;
--DUMP tmp;

--speechAccountAgentTransfer = FILTER speechAccountDispGroupSorted BY $2 == 'agenttransfer';
--speechAccountAgentTransfer = FOREACH speechAccountAgentTransfer GENERATE $0;
speechAccountAgentTransfer = DISTINCT speechAccountDispGroupSorted;

tmp = LIMIT speechAccountAgentTransfer 10;
DUMP tmp;


--------------------------------------------
--------     Merging Web & IVR     ---------
--------------------------------------------
speechInfoAccountIdJoin2 = JOIN 
								webSpeechInfo BY $8, 
								speechAccountAgentTransfer BY $0;

tmp = LIMIT speechInfoAccountIdJoin2 10;
DUMP tmp;


--------------------------------------------
--------     Removing FP BSIDs     ---------
--------------------------------------------

webSpeechInfo_fmt = FOREACH speechInfoAccountIdJoin2 GENERATE
																$0 as web_csid,
																$1 as webPageLoadTime,
																$2 as pageTitle,
																$3 as pageURL,
																$4 as queue,
																$5 as w_acctid,
																$6 as w_cid,
																$7 as speechStartTime,
																$8 as speech_uuid,
																$9 as ani,
																$10 as dnis,
																$11 as custId,
																$12 as acctId,
																$13 as time_diff_sec,
																$14 as uuid_intent,
																$15 as intent,
																$16 as disposition,
																$17 as logtype;

webSpeechInfo_fmt_01 = DISTINCT webSpeechInfo_fmt;

webSpeechInfo3_fmt3 = FOREACH (GROUP webSpeechInfo_fmt_01 BY speech_uuid) {
                                                                           sort_data = ORDER webSpeechInfo_fmt_01 BY speech_uuid, time_diff_sec;
                                                                           first = LIMIT sort_data 1;
                                                                           GENERATE FLATTEN(first);
                                                                         };

tmp = LIMIT webSpeechInfo3_fmt3 10;
DUMP tmp;

STORE webSpeechInfo3_fmt3 INTO 'capone_omni_channel_01Mar2018-31Mar2018.tsv';

