REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/preddy/CapOne/queue_UDF.py' using jython as func;

Register 'hdfs:///user/jjonnada/timeFunctions.py' using jython as time;
DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');
data = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=09/day={01}/*/*/*.avro' using LOAD_IDM;
appLogFilter = FILTER data BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE header.timeEpochMillisUTC as time, header.channelSessionId as uuid, body#'label' as logtag, body#'optMessage' as value;
variable8Filter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.callvariable8';
dispositionFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.disposition.ivrcompletedcall' OR logtag == 'com.tellme.proprietary.capitalone.card.disposition.deleveragedcall' OR logtag == 'com.tellme.proprietary.capitalone.card.disposition.agenttransfer' OR logtag == 'com.tellme.proprietary.capitalone.card.disposition.abandonedcall';

variable8DispJoin = JOIN variable8Filter BY uuid, dispositionFilter BY uuid;
DESCRIBE variable8DispJoin;
STORE variable8DispJoin INTO 'cap1_intent_disposition_25Jul-variable8DispJoin';

variable8DispJoinDistinct = DISTINCT variable8DispJoin;
DESCRIBE variable8DispJoinDistinct;
STORE variable8DispJoinDistinct INTO 'cap1_intent_disposition_25Jul-variable8DispJoinDistinct';

variable8DispCount = FOREACH (GROUP variable8DispJoinDistinct BY ($3,$6)) GENERATE FLATTEN(group), COUNT(variable8DispJoinDistinct);
DESCRIBE variable8DispCount;
STORE variable8DispCount INTO 'cap1_intent_disposition_25Jul';