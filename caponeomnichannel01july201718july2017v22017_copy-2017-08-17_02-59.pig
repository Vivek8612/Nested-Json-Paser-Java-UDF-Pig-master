REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

REGISTER 'hdfs:///user/preddy/CapOne/queue_UDF.py' using jython as func;


DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
%DECLARE MONTH '{08}'
-- %DECLARE DAYS '{15}'
%DECLARE DAYS '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16}'
-- %DECLARE DAYS '{01}'

-- web_log_data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
-- web_log_data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

-- data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

-- sdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;
sdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

-- infoGeo = FOREACH data GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid_raw,specificEventType,body,custom;
-- reqFilterGeo = FILTER infoGeo BY $2=='OnlineSessionStartEvent';
-- reqFilterExtractGeo = FOREACH reqFilterGeo GENERATE csid_raw, body#'ipCountry' as ipCountry;
-- csidCanada = FILTER reqFilterExtractGeo BY $1 =='Canada';
-- csidCanada = FOREACH csidCanada GENERATE csid_raw;
-- csidCanada2 = FOREACH csidCanada GENERATE *, STRSPLIT (csid_raw,'@@',2) as uniqId;
-- csidCanada3 = FOREACH csidCanada2 GENERATE uniqId.$0 as csid;
-- csidCanadaDistinct = DISTINCT csidCanada3;


customEventFilter = FILTER data BY specificEventType == 'WebCustomEvent' and body#'customEventType' == '100010';
accountInfo = FOREACH customEventFilter GENERATE header.channelSessionId, custom#'accid' as w_acctid, custom#'ci' as w_cid;
pageLoadFilter = FILTER data BY specificEventType == 'WebPageLoadEvent';
pageLoadInfo = FOREACH pageLoadFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid, body#'pageTitle' as pageTitle, body#'pageURL' as pageUrl, body#'referrerURL' as referrerURL, custom#'Queue' as queue;
pageLoadGroup = GROUP pageLoadInfo BY $1;
pageLoadGroupSorted = FOREACH pageLoadGroup {
                                                                distinctData = DISTINCT pageLoadInfo;
                                                                sortData = ORDER distinctData BY $0;
                                                                GENERATE group as csid, sortData as pageDetails;
};


pageLoadGroupSortedCanada = FOREACH pageLoadGroupSorted GENERATE *, func.convertBagToStr($1) as queue_concat;
pageLoadGroupSortedCanada2 = FILTER pageLoadGroupSortedCanada BY ($2 matches '.*Canada.*');
pageLoadGroupSortedCanada2 = FOREACH pageLoadGroupSortedCanada2 GENERATE $0 as csid, $1 as pageDetails;

pageLoadAccountJoin = JOIN pageLoadGroupSortedCanada2 BY $0, accountInfo BY $0;
pageLoadAccountJoinDistinct = DISTINCT pageLoadAccountJoin;


startFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE header.timeEpochMillisUTC as startTime,header.channelSessionId as uuid, body#'ani',body#'dnis';

appLogFilter = FILTER sdata BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as uuid, body#'label' as logtag, body#'optMessage' as message;

custIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.customerid';
acctIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.accountid';

infoJoin = JOIN startInfo BY uuid, custIdFilter BY uuid, acctIdFilter BY uuid;


speechInfo = FOREACH infoJoin GENERATE $0 as startTime,$1 as uuid,$2 as ani, $3 as dnis,$7 as custId,$11 as acctId;
speechInfoDistinct = DISTINCT speechInfo;
speechInfoPageLoadJoin = JOIN pageLoadAccountJoinDistinct BY $3, speechInfoDistinct BY acctId;


webSpeechInfo = FOREACH speechInfoPageLoadJoin GENERATE $0 as web_csid, $1 as webPages, $3 as w_acctid, $4 as w_cid, $5 as speechStartTime, $6 as speech_uuid;

STORE webSpeechInfo INTO 'capone_omni_channel_01July2017-18July2017_json' USING JsonStorage();
