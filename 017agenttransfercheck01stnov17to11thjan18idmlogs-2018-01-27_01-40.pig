--set tez.staging-dir '/tmp/vkumar/staging';
--SET tez.queue.name 'dsg'; 

--------------------------------------------
---------   Setting Memory Size  -----------
--------------------------------------------
--set mapreduce.map.memory.mb    38400
--set mapreduce.reduce.memory.mb 38400
--set tez.am.resource.memory.mb  8192

--------------------------------------------
---------    Registering jars    -----------
--------------------------------------------
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
%DECLARE YEAR_01 '2017'

%DECLARE MONTH_01 '{11,12}'

%DECLARE YEAR_02 '2018'

%DECLARE MONTH_02 '{01}'

%DECLARE DAY_02 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18}'

--------------------------------------------
---------      Loading Data      -----------
--------------------------------------------
starGateUUID = LOAD '/user/vkumar/Agent_Transfer_Check_01stNov17to11th_Jan18_IDMLogs_03.csv' using PigStorage(',') As (CHANNELSESSIONID: chararray);

data_01 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR_01/month=$MONTH_01/*/*/*/*.avro' using LOAD_IDM;
data_02 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR_02/month=$MONTH_02/day=$DAY_02/*/*/*.avro' using LOAD_IDM;
data = UNION data_01, data_02;

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

--------------------------------------------
---------      Loading AccID     -----------
--------------------------------------------
accIdVaraible_01 = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.bank.application.isCustomerTransfer';
infoJoin = JOIN starGateUUID BY CHANNELSESSIONID LEFT OUTER,
				accIdVaraible_01 BY uuid;
STORE infoJoin INTO '/user/vkumar/IVR_UUIDDetails/IVR_AgentTransfers_01st_Nov17to18th_Jan18_infoJoin.tsv';

