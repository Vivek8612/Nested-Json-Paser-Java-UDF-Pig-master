REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

REGISTER 'hdfs:///user/preddy/CapOne/get_all_account_details_v3.py' using jython as get_all_account_details_v3;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
--%DECLARE MONTH '{11}'
%DECLARE MONTH '{09,10}'
--%DECLARE MONTH_01 '{09}'
--%DECLARE MONTH_02 '{10}'
--%DECLARE DAYS_01 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30}'
--%DECLARE DAYS_02 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'
--%DECLARE DAYS '{01}'

--data = LOAD '/raw/prod/rtdp/idm/events/metropcs/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
--data_01 = LOAD '/raw/prod/rtdp/idm/events/metropcs/year=$YEAR/month=$MONTH_01/day=$DAYS_01/*/*/*.avro' using LOAD_IDM;
--data_02 = LOAD '/raw/prod/rtdp/idm/events/metropcs/year=$YEAR/month=$MONTH_02/day=$DAYS_02/*/*/*.avro' using LOAD_IDM;
--data = UNION data_01, data_02; 

data = LOAD '/raw/prod/rtdp/idm/events/metropcs/year=$YEAR/month=$MONTH/*/*/*/*.avro' using LOAD_IDM;

f1 = FILTER data BY specificEventType == 'WebPageLoadEvent';

f2 = FOREACH f1 GENERATE 
				header.timeEpochMillisUTC as time, 
				header.associativeTag as vi,
				header.channelSessionId as bsid,
                body#'pageURL' as pageurl;
--custom#'profileReferenceID' as profile_reference_id,
--custom#'accountReferenceID' as account_reference_id,
--custom#'collectionAcctData' as collection_acct_data;

tmp = LIMIT f2 10;
DUMP tmp;

f3 = FILTER f2 BY (pageurl != '' and pageurl is not null and pageurl MATCHES '.*/content/metro/en/mobile/metro/cart/checkout.html.*');

tmp = LIMIT f3 10;
DUMP tmp;
--f3 = FILTER f2 BY (profile_reference_id != '' and profile_reference_id is not null);

f4 = FOREACH f3 GENERATE 
				SUBSTRING(ToString(ToDate((long)time)),1,INDEXOF(ToString(ToDate((long)time)), 'T', 1)) as time,  
				vi,
                bsid;

tmp = LIMIT f4 10;
DUMP tmp;

f5 = DISTINCT f4;

f6 = GROUP f5 BY time;
--tmp = LIMIT f6 10;
--DUMP tmp;

cnt = FOREACH f6 GENERATE group, COUNT(f5);
--DUMP cnt; 
STORE cnt INTO 'metropcs_Visit_Volume_Sept-Oct.tsv';

f7 = FOREACH f5 GENERATE
				time,
				vi;

f8 = DISTINCT f7;

f9 = GROUP f8 BY time;
--tmp = LIMIT f9 10;
--DUMP tmp;

cnt_1 = FOREACH f9 GENERATE group, COUNT(f8);
--DUMP cnt_1; 
STORE cnt_1 INTO 'metropcs_Visitor_Volume_Sept-Oct.tsv';
