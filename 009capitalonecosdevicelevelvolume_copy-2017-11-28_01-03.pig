-------------------------------------- Run Code on Tez --------------------------------------
set tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

-------------------------------------- REGISTER the Jar --------------------------------------
REGISTER 'hdfs:///user/vkumar/udf/elephant-bird-pig-3.0.9.jar';
REGISTER 'hdfs:///user/vkumar/udf/json_simple-1.1.jar';

-------------------------------------- Define the Custom Parser Method --------------------------------------
DEFINE myJsonParser com.CustomParser();
DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

-------------------------------------- Jan2016 --------------------------------------
%DECLARE YEAR '2016'
%DECLARE MONTH '{01}'

data = LOAD '/raw/prod/capitalone/tie/merged/year=$YEAR/month=$MONTH/*/*/*/*' USING SequenceFileLoader as (eventHour:chararray, event:chararray);

%DECLARE tmp_01 '-------------------------------------- data --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT data 10;
DUMP tmp;
DESCRIBE tmp;

f1 = FOREACH data GENERATE 
					SUBSTRING(eventHour, 0, LAST_INDEX_OF(eventHour, 'T')) as time,
                    JsonStringToMap(event) as eventjson;

%DECLARE tmp_01 '-------------------------------------- f1 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f1 10;
DUMP tmp;
DESCRIBE tmp;

f2 = FILTER f1 BY eventjson is not null;

%DECLARE tmp_01 '-------------------------------------- f2 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f2 10;
DUMP tmp;
DESCRIBE tmp;

f3 = FOREACH f2 GENERATE 
				time,
                eventjson#'ts' as ts,
				eventjson#'e' as eventDetail;

%DECLARE tmp_01 '-------------------------------------- f3 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f3 10;
DUMP tmp;
DESCRIBE tmp;

f4 = FOREACH f3 GENERATE
				ts as time,
                JsonStringToMap(eventDetail) as eventjson;

%DECLARE tmp_01 '-------------------------------------- f4 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f4 10;
DUMP tmp;
DESCRIBE tmp;

f4 = FILTER f4 BY eventjson is not null;

%DECLARE tmp_01 '-------------------------------------- f4 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f4 10;
DUMP tmp;
DESCRIBE tmp;

f5 = FOREACH f4 GENERATE
				time,
				eventjson#'ec' as eventType,
                eventjson#'vi' as visitorID,
                eventjson#'bsid' as visitID,
                eventjson#'f' as function,
                eventjson;

%DECLARE tmp_01 '-------------------------------------- f5 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f5 10;
DUMP tmp;
DESCRIBE tmp;

f6 = FILTER f5 BY (
					eventType == '100001' and 
                    function is not null and function != '' and 
                    visitorID is not null and visitorID != '' and 
                    visitID is not null and visitID != '' and
                    time != '' and time is not null );

%DECLARE tmp_01 '-------------------------------------- f6 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f6 10;
DUMP tmp;
DESCRIBE tmp;

f7 = FOREACH f6 GENERATE
				SUBSTRING(
                			time,
                            0,
                            INDEXOF(time,' ',0)
                            ) as time,
                eventType,
                visitorID,
                visitID,
                SUBSTRING(
                			JsonStringToMap(JsonStringToMap(function)#'v')#'dd', 
                            INDEXOF(JsonStringToMap(JsonStringToMap(function)#'v')#'dd','~',0)+1,
                            LAST_INDEX_OF(JsonStringToMap(JsonStringToMap(function)#'v')#'dd','~')
							)as deviceType,
                JsonStringToMap(JsonStringToMap(function)#'a')#'qu' as queue;

%DECLARE tmp_01 '-------------------------------------- f7 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f7 10;
DUMP tmp;
DESCRIBE tmp;

f8 = FOREACH f7 GENERATE
				time,
                eventType,
                visitorID,
                visitID,
                deviceType,
                queue,
                CONCAT(time,eventType,deviceType,queue) as lookup_combination;

%DECLARE tmp_01 '-------------------------------------- f8 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f8 10;
DUMP tmp;
DESCRIBE f8;

f8_01 = FILTER f8 BY (lookup_combination is not null and lookup_combination != '');

%DECLARE tmp_01 '-------------------------------------- f8_01 --------------------------------------';
sh echo $tmp_01;
DESCRIBE f8_01;
tmp = LIMIT f8_01 10;
DUMP tmp;


f8_02 = DISTINCT f8_01;

%DECLARE tmp_01 '-------------------------------------- f8_02 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f8_02 10;
DUMP tmp;
DESCRIBE tmp;

f8_03 = FOREACH (GROUP f8_01 by visitID) {
											first = LIMIT f8_01 1;
                                            GENERATE FLATTEN(first);
                                            };

%DECLARE tmp_01 '-------------------------------------- f8_03 --------------------------------------';
sh echo $tmp_01;
DESCRIBE f8_03;
f8_03_grp = GROUP f8_03 ALL;
f8_03_tmp = FOREACH f8_03_grp GENERATE COUNT(f8_03);
DUMP f8_03_tmp;
tmp = LIMIT f8_03 10;
DUMP tmp;


f9 = GROUP f8_03 BY (time, eventType, queue, deviceType, lookup_combination);

%DECLARE tmp_01 '-------------------------------------- f9 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f9 10;
DUMP tmp;
DESCRIBE tmp;

visitCount = FOREACH f9 GENERATE FLATTEN(group), COUNT(f8_03);

%DECLARE tmp_01 '-------------------------------------- visitCount --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT visitCount 10;
DUMP tmp;
DESCRIBE tmp;

f10 = FOREACH f7 GENERATE
				time,
                eventType,
                visitorID,
                deviceType,
                queue,
				CONCAT(time,eventType,deviceType,queue) as lookup_combination;

%DECLARE tmp_01 '-------------------------------------- f10 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f10 10;
DUMP tmp;
DESCRIBE tmp;

f10 = FILTER f10 BY lookup_combination is not null;

%DECLARE tmp_01 '-------------------------------------- f10 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f10 10;
DUMP tmp;
DESCRIBE tmp;

f10_01 = DISTINCT f10;

%DECLARE tmp_01 '-------------------------------------- f10 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f10_01 10;
DUMP tmp;
DESCRIBE tmp;

f11 = GROUP f10_01 BY (time, eventType, queue, deviceType, lookup_combination);

%DECLARE tmp_01 '-------------------------------------- f11 --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT f11 10;
DUMP tmp;
DESCRIBE tmp;

visitorCount = FOREACH f11 GENERATE FLATTEN(group), COUNT(f10_01);

%DECLARE tmp_01 '-------------------------------------- visitorCount --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT visitorCount 10;
DUMP tmp;
DESCRIBE tmp;

visitNvisitorData = JOIN 
					visitorCount BY $4,
					visitCount BY $4;

%DECLARE tmp_01 '-------------------------------------- visitNvisitorData --------------------------------------';
sh echo $tmp_01;
tmp = LIMIT visitNvisitorData 10;
DUMP tmp;
DESCRIBE tmp;

STORE visitNvisitorData INTO 'cap1_COS_DeviceLevel_visitorNvisitCount_Jan2016';

