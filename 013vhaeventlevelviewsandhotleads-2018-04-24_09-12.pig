SET tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

/*
--------------------------------------------
---------   Setting Memory Size  -----------
--------------------------------------------
set tez.am.resource.memory.mb	89600
tez.am.session.min.held-containers	89600
set tez.am.resource.memory.mb	8192
set tez.task.resource.memory.mb	8192
tez.container.max.java.heap.fraction	0.8
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
DEFINE UrlDecode InvokeForString('java.net.URLDecoder.decode', 'String String'); 
--------------------------------------------
------Define the Custom Parser Method-------
--------------------------------------------
DEFINE myJsonParser com.CustomParser();
DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();


--------------------------------------------
---------      Loading Data      -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{01,02,03,04,05,06,07,08,09,10,11,12}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/*/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
tmp = LIMIT wdata 10;
DUMP tmp;

pageLoadFilter = FOREACH wdata GENERATE 
					SUBSTRING(eventHour, 0, 6) as date,
                    JsonStringToMap(event) as eventjson;
tmp = LIMIT pageLoadFilter 10;
DUMP tmp;

pageLoadInfo = FILTER pageLoadFilter BY eventjson is not null;

pageLoadInfo_01 = FOREACH pageLoadInfo GENERATE 
												date,
												eventjson#'ts' as ts,
												JsonStringToMap(eventjson#'e') as eventDetail;
tmp = LIMIT pageLoadInfo_01 10;
DUMP tmp;

pageLoadInfo_02 = FILTER pageLoadInfo_01 BY eventDetail is not null;

pageLoadInfo_03 = FOREACH pageLoadInfo_02 GENERATE 
												date,
												SUBSTRING(ts, 0, 7) as ts,
												eventDetail#'ec' as eventType,
												eventDetail#'vi' as visitorID,
												eventDetail#'bsid' as visitID,
												eventDetail#'f' as function;

tmp = LIMIT pageLoadInfo_03 10;
DUMP tmp;

pageLoadInfo_04 = FILTER pageLoadInfo_03 BY (
												ts is not null and 
												eventType is not null and 
												visitorID is not null and 
												visitID is not null and
												function is not null); 
tmp = LIMIT pageLoadInfo_04 10;
DUMP tmp;

pageLoadInfo_05 = FILTER pageLoadInfo_04 BY (eventType == '100001' or eventType == '110000');

pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed_01 BY (ts, eventType)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed_01);
DUMP pageLoadInfoCount;

