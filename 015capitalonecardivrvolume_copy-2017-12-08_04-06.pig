set tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 
REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar';
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

%DECLARE MONTH '02'

data = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/*/*/*/*.avro' using LOAD_IDM;
tmp = LIMIT data 10;
DUMP tmp;

appLogFilter = FILTER data BY specificEventType == 'SpeechPlatformAppLogEvent';
tmp = LIMIT appLogFilter 10;
DUMP tmp;

appLogInfo = FOREACH appLogFilter GENERATE 
									SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), 'T', 1)) as time, 
                                    header.channelSessionId as uuid, 
                                    body#'label' as logtag, 
                                    body#'optMessage' as value;
tmp = LIMIT appLogInfo 10;
DUMP tmp;


startCallFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.startcall';
tmp = LIMIT startCallFilter 10;
DUMP tmp;
startCallFilter_Count = FOREACH (GROUP startCallFilter ALL) GENERATE COUNT_STAR(startCallFilter); 
DUMP startCallFilter_Count;


eventType = FOREACH data GENERATE specificEventType as event;
eventTypeDist = DISTINCT eventType;
DUMP eventTypeDist;

callANIDetails = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.spoof_params';
tmp = LIMIT callANIDetails 10;
DUMP tmp;
callANIDetails_Count = FOREACH (GROUP callANIDetails ALL) GENERATE COUNT_STAR(callANIDetails); 
DUMP callANIDetails_Count;



callANIDetailsWithANI = FOREACH callANIDetails GENERATE 
													time,
													uuid,
													logtag,
													value,
													SUBSTRING(value, INDEXOF( value,'from=<sip:+', 0) + 11, INDEXOF( value,'from=<sip:+', 0) + 23) as fromANI,
													SUBSTRING(value, INDEXOF( value,'to=sip:', 0) + 7, INDEXOF( value,'to=sip:', 0) + 19) as toANI;
tmp = LIMIT callANIDetailsWithANI 10;
DUMP tmp;
callANIDetailsWithANI_Count = FOREACH (GROUP callANIDetailsWithANI ALL) GENERATE COUNT_STAR(callANIDetailsWithANI); 
DUMP callANIDetailsWithANI_Count;
