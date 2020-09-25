set tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/preddy/CapOne/queue_UDF.py' using jython as func;
Register 'hdfs:///user/vkumar/udf/timeFunctions.py' using jython as time;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

----------------------------------------------------------------------May 2017------------------------------------------------------------
%DECLARE YEAR '2018'

%DECLARE MONTH '01'

data = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/*/*/*/*.avro' using LOAD_IDM;

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

variable8Filter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.callvariable8';
tmp = LIMIT variable8Filter 10;
DUMP tmp;

dispositionFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.disposition.ivrcompletedcall' OR logtag == 'com.tellme.proprietary.capitalone.card.disposition.deleveragedcall' OR logtag == 'com.tellme.proprietary.capitalone.card.disposition.agenttransfer' OR logtag == 'com.tellme.proprietary.capitalone.card.disposition.abandonedcall';
tmp = LIMIT dispositionFilter 10;
DUMP tmp;

canadianFlag = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.CanadianAccount';
tmp = LIMIT canadianFlag 10;
DUMP tmp;

variable8DispJoin = JOIN 
					variable8Filter BY uuid, 
                    dispositionFilter BY uuid;
tmp = LIMIT variable8DispJoin 10;
DUMP tmp;

variable8DispJoin_01 = JOIN 
						variable8DispJoin BY $1 LEFT OUTER, 
                        canadianFlag BY uuid;
tmp = LIMIT variable8DispJoin_01 10;
DUMP tmp;

variable8DispJoinDistinct = DISTINCT variable8DispJoin_01;
tmp = LIMIT variable8DispJoinDistinct 10;
DUMP tmp;



variable8DispCount = FOREACH (GROUP variable8DispJoinDistinct BY ($0,$3,$6,$10)) GENERATE FLATTEN(group), COUNT(variable8DispJoinDistinct);
tmp = LIMIT variable8DispCount 10;
DUMP tmp;

STORE variable8DispCount INTO 'cap1_intent_disposition_Jan2018';