REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
%DECLARE MONTH '{07}'
-- %DECLARE DAYS '{15}'
%DECLARE DAYS '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18}'

-- web_log_data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
-- web_log_data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

-- data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

-- sdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;
sdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

infoGeo = FOREACH data GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid,specificEventType,body,custom;
reqFilterGeo = FILTER infoGeo BY $2=='OnlineSessionStartEvent';
reqFilterExtractGeo = FOREACH reqFilterGeo GENERATE csid, body#'ipCountry' as ipCountry;
csidCanada = FILTER reqFilterExtractGeo BY $1 =='Canada';
csidCanada = FOREACH csidCanada GENERATE csid;
csidCanadaDistinct = DISTINCT csidCanada;

customEventFilter = FILTER data BY specificEventType == 'WebCustomEvent' and body#'customEventType' == '100010';
accountInfo = FOREACH customEventFilter GENERATE header.channelSessionId, custom#'accid' as w_acctid, custom#'ci' as w_cid;
pageLoadFilter = FILTER data BY specificEventType == 'WebPageLoadEvent';
pageLoadInfo = FOREACH pageLoadFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid, body#'pageTitle' as pageTitle, body#'pageURL' as pageUrl, body#'referrerURL' as referrerURL;
pageLoadGroup = GROUP pageLoadInfo BY $1;
pageLoadGroupSorted = FOREACH pageLoadGroup {
                                                                distinctData = DISTINCT pageLoadInfo;
                                                                sortData = ORDER distinctData BY $0;
                                                                GENERATE group as csid, sortData as pageDetails;
};

csidCanadaJoin = JOIN pageLoadGroupSorted BY $0, csidCanadaDistinct BY $0;
csidCanadaJoin = FOREACH csidCanadaJoin GENERATE $0, $1, $2, $3, $4, $5;
pageLoadAccountJoin = JOIN csidCanadaJoin BY $0, accountInfo BY $0;
pageLoadAccountJoinDistinct = DISTINCT pageLoadAccountJoin;

clickEventFilter = FILTER data BY body#'customEventType' == '400100';
clickEventInfo = FOREACH clickEventFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid,custom#'ClientTimestamp' as clientTimeStamp, custom#'Queue' as queue,custom#'skill' as skill,com.twitter.elephantbird.pig.piggybank.JsonStringToMap(custom#'Ed') as EdData;
clickEventGroup = GROUP clickEventInfo BY $1;
clickEventGroupSorted = FOREACH clickEventGroup {
                                                                distinctData = DISTINCT clickEventInfo;
                                                                sortData = ORDER distinctData BY time;
                                                                GENERATE group as csid,sortData as clickEventDetails;
                                        };

pageEventFilter = FILTER data BY body#'customEventType' == '400110';
pageEventInfo = FOREACH pageEventFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid, custom#'ClientTimestamp' as clientTimeStamp, custom#'StartOT' as startOT,custom#'TargetGroupId' as targetGroupId,custom#'Queue' as queue, custom#'SubmitOT' as submitOT, custom#'status' as status, custom#'skill' as skill, com.twitter.elephantbird.pig.piggybank.JsonStringToMap(custom#'page') as pageInfo, com.twitter.elephantbird.pig.piggybank.JsonStringToMap(custom#'visitor') as visitorInfo, com.twitter.elephantbird.pig.piggybank.JsonStringToMap(custom#'session') as sessionInfo;
pageEventGroup = GROUP pageEventInfo BY $1;
pageEventGroupSorted = FOREACH pageEventGroup {
                                                                distinctData = DISTINCT pageEventInfo;
                                                                sortData = ORDER distinctData BY time;
                                                                GENERATE group as csid,sortData as pageEventDetails;
                                        };
webEventJoin = JOIN clickEventGroupSorted BY $0, pageEventGroupSorted BY $0;
pageLoadWebEventJoin = JOIN pageLoadAccountJoinDistinct BY $0 LEFT OUTER, webEventJoin BY $0;

startFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE header.timeEpochMillisUTC as startTime,header.channelSessionId as uuid, body#'ani',body#'dnis';
callendFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallEndEvent';
callEndInfo = FOREACH callendFilter GENERATE header.timeEpochMillisUTC as endTime, header.channelSessionId as uuid, body#'terminationType', body#'durationMillis';
appLogFilter = FILTER sdata BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as uuid, body#'label' as logtag, body#'optMessage' as message;
appLogGroup = GROUP appLogInfo BY uuid;
appLogGroupSorted = FOREACH appLogGroup {
                                                                distinctData = DISTINCT appLogInfo;
                                                                sortData = ORDER distinctData BY time;
                                                                GENERATE group as uuid,sortData as appLogDetails;
                                        };
taskFilter = FILTER sdata BY specificEventType == 'SpeechPlatformTaskEvent';
taskInfo = FOREACH taskFilter GENERATE header.timeEpochMillisUTC as time, header.channelSessionId as uuid, body#'task' as taskLabel ,body#'disposition' as disposition,body#'optReason' as reason , body#'optMessage' as message;
taskGroup = GROUP taskInfo BY uuid;
taskGroupSorted = FOREACH taskGroup {
                                                        distinctData = DISTINCT taskInfo;
                                                        sortData = ORDER distinctData BY time;
                                                        GENERATE group as uuid, sortData as taskDetails;
                                                        };
recoFilter = FILTER sdata BY specificEventType == 'SpeechPlatformRecognitionEvent';
recoInfo = FOREACH recoFilter GENERATE header.timeEpochMillisUTC as time ,header.channelSessionId as uuid, body#'vxmlField' as node, body#'outcome' as outcome,body#'confidential' as confidential,body#'optResult'#'optDurationMillis' as recoDuration, body#'resultMode' as mode, body#'optResult'#'optResults' as results,body#'optResult'#'optVxmlResult'#'optUtterance' as utterance, body#'optResult'#'optVxmlResult'#'optMatchValue' as matchValue, body#'optResult'#'optVxmlResult'#'confidence' as confidence, body#'optRecoParams'#'rejectthreshold' as threshold;
recoGroup = GROUP recoInfo BY uuid;
recoGroupSorted = FOREACH recoGroup {
                                                        distinctData = DISTINCT recoInfo;
                                                        sortData = ORDER distinctData BY time;
                                                        GENERATE group as uuid, sortData as recoDetails;
                                                        };
custIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.customerid';
acctIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.accountid';
infoJoin = JOIN startInfo BY uuid, custIdFilter BY uuid, acctIdFilter BY uuid, callEndInfo BY uuid, appLogGroupSorted BY $0, taskGroupSorted BY $0, recoGroupSorted BY $0;
speechInfo = FOREACH infoJoin GENERATE $0 as startTime,$1 as uuid,$2 as ani, $3 as dnis,$7 as custId,$11 as acctId,$12 as endTime, $14 as endType,$15 as duration, $17 as appLogDetails, $19 as taskDetails, $21 as recoDetails;
speechInfoDistinct = DISTINCT speechInfo;
speechInfoPageLoadJoin = JOIN pageLoadWebEventJoin BY $3, speechInfoDistinct BY acctId;

webSpeechInfo = FOREACH speechInfoPageLoadJoin GENERATE $0 as web_csid, $1 as webPages, $3 as w_acctid, $4 as w_cid, $8 as pageEventDetails,$9 as speechStartTime, $10 as speech_uuid;

STORE webSpeechInfo INTO 'cap1_omni_channel_1may_17may_2017_json' USING JsonStorage();
