SET tez.staging-dir '/tmp/vkumar/staging';
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
DEFINE UrlDecode InvokeForString('java.net.URLDecoder.decode', 'String String'); 
--------------------------------------------
------Define the Custom Parser Method-------
--------------------------------------------
DEFINE myJsonParser com.CustomParser();
DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Jan Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{01}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Jan17_15Jan17_04242018.tsv';


--------------------------------------------
---------      16-31Jan Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{01}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Jan17_31Jan17_04242018.tsv';

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Feb Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{02}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Feb17_15Feb17_04242018.tsv';


--------------------------------------------
---------      16-31Feb Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{02}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Feb17_28Feb17_04242018.tsv';

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Mar Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{03}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Mar17_15Mar17_04242018.tsv';


--------------------------------------------
---------      16-31Mar Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{03}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Mar17_31Mar17_04242018.tsv';

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Apr Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{04}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Apr17_15Apr17_04242018.tsv';


--------------------------------------------
---------      16-31Apr Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{04}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Apr17_30Apr17_04242018.tsv';


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15May Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{05}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01May17_15May17_04242018.tsv';


--------------------------------------------
---------      16-31May Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{05}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16May17_31May17_04242018.tsv';


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Jun Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{06}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Jun17_15Jun17_04242018.tsv';


--------------------------------------------
---------      16-31Jun Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{06}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Jun17_30Jun17_04242018.tsv';



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------     01-15July Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{07}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Jul17_15Jul17_04242018.tsv';


--------------------------------------------
---------     16-31July Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{07}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Jul17_31Jul17_04242018.tsv';



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Aug Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{08}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Aug17_15Aug17_04242018.tsv';


--------------------------------------------
---------      16-31Aug Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{08}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Aug17_31Aug17_04242018.tsv';


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Sep Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{09}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Sep17_15Sep17_04242018.tsv';


--------------------------------------------
---------      16-31Sep Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{09}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Sep17_30Sep17_04242018.tsv';


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Oct Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{10}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Oct17_15Oct17_04242018.tsv';


--------------------------------------------
---------      16-31Oct Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{10}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Oct17_31Oct17_04242018.tsv';


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Nov Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{11}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Nov17_15Nov17_04242018.tsv';


--------------------------------------------
---------      16-31Nov Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{11}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Nov17_31Nov17_04242018.tsv';


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------
---------      01-15Dec Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{12}'
%DECLARE WDAY01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_01Dec17_15Dec17_04242018.tsv';


--------------------------------------------
---------      16-31Dec Data     -----------
--------------------------------------------
%DECLARE YEAR01 '2017'
%DECLARE MONTH01 '{12}'
%DECLARE WDAY01 '{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

wdata = LOAD '/raw/prod/vha/tie/merged/year=$YEAR01/month=$MONTH01/day=$WDAY01/*/*/*'  USING SequenceFileLoader as (eventHour:chararray, event:chararray);
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
tmp = LIMIT pageLoadInfo_05 10;
DUMP tmp;

pageLoadInfo_06 = FOREACH pageLoadInfo_05 GENERATE 
												$0 as date,
												$1 as ts,
												$2 as eventType,
												$3 as visitorID,
												$4 as visitID,
												JsonStringToMap($5) as function;

tmp = LIMIT pageLoadInfo_06 10;
DUMP tmp;

pageLoadInfo_07 = FILTER pageLoadInfo_06 BY function is not null;

pageLoadInfo_08 = FOREACH pageLoadInfo_07 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												JsonStringToMap(function#'p') as profile;

tmp = LIMIT pageLoadInfo_08 10;
DUMP tmp;

pageLoadInfo_09 = FILTER pageLoadInfo_08 BY profile is not null ;

pageLoadInfo_10 = FOREACH pageLoadInfo_09 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												profile#'cp' as url;

tmp = LIMIT pageLoadInfo_10 10;
DUMP tmp;

pageLoadInfo_11 = FILTER pageLoadInfo_10 BY url is not null;

pageLoadInfo_12 = FOREACH pageLoadInfo_11 GENERATE
												date,
												ts,
												eventType,
												visitorID,
												visitID,
												UrlDecode(url, 'UTF-8') as url;

tmp = LIMIT pageLoadInfo_12 10;
DUMP tmp;

pageLoadInfo_13 = FILTER pageLoadInfo_12 BY url is not null;

pageLoadInfoParsed = FOREACH pageLoadInfo_13 Generate 
													date,
													ts,
													eventType,
													visitorID,
													visitID,
													url as pageURL,
													((url is null or url == '' or url == 'null' or url == 'UNKNOWN') ? 'Pageurl - NA' : ((INDEXOF(url, 'services/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'services/', 0)+9)) : ((INDEXOF(url, 'receipts/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'receipts/', 0)+9)) : ((INDEXOF(url, '=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '=', 0)+1)) : ((INDEXOF(url, '?',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '?', 0)+1)) : ((INDEXOF(url, '#',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, '#', 0)+1)) : ((INDEXOF(url, 'android/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'android/', 0)+8)) : ((INDEXOF(url, 'cludoquery=',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'cludoquery=', 0)+11)) : ((INDEXOF(url, 'plans',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'plans', 0)+5)) : ((INDEXOF(url, 'support',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'support', 0)+7)) : ((INDEXOF(url, 'mobile-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'mobile-details/', 0)+15)) : ((INDEXOF(url, 'data_control/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'data_control/', 0)+13)) : ((INDEXOF(url, 'tablet-details/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'tablet-details/', 0)+15)) : ((INDEXOF(url, 'recharge/',0) > 0 ) ? SUBSTRING(url, 0, (int)(INDEXOF(url, 'recharge/', 0)+9)) : url)))))))))))))) as parsedUrl;

tmp = LIMIT pageLoadInfoParsed 10;
DUMP tmp;


pageLoadInfoCount = FOREACH (GROUP pageLoadInfoParsed BY (ts, eventType, parsedUrl)) GENERATE FLATTEN(group), COUNT_STAR(pageLoadInfoParsed);
tmp = LIMIT pageLoadInfoCount 10;
DUMP tmp;

STORE pageLoadInfoCount INTO 'vha_pageloadNhotleadCount_16Dec17_31Dec17_04242018.tsv';


