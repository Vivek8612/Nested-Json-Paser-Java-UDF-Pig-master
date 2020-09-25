--SET tez.staging-dir '/tmp/vkumar/staging';
--SET tez.queue.name 'dsg'; 

REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/vkumar/udf/get_account_ids_v5.py' using jython as fn_acc_id;
REGISTER 'hdfs:///user/preddy/CapOne/cust_json_parser.py' using jython as cust_json_praser;
REGISTER 'hdfs:///user/vkumar/udf/get_all_account_details_v4.py' using jython as get_all_account_details_v4;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2018'
%DECLARE MONTH '{02}'
%DECLARE DAYS '{09,10,11,12,13,14,15,16,17,18}'

data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

f01 = FILTER data BY specificEventType == 'WebCustomEvent';

f02 = FOREACH f01 GENERATE 
						header.timeEpochMillisUTC as time,
						header.channelSessionId as bsid, 
						custom#'userProfile' as user_profile;

f03 = FILTER f02 BY (user_profile !='' and user_profile is not null);

tmp = LIMIT f03 10;
DUMP tmp;

f04 = FOREACH f03 GENERATE 
					SUBSTRING(ToString(ToDate((long)time)),1,INDEXOF(ToString(ToDate((long)time)), 'T', 1)) as time,
					bsid, 
					fn_acc_id.extract_account_ids($1,$2) as bsid_acc_ids;

tmp = LIMIT f04 10;
DUMP tmp;

f04_01 = FOREACH f04 GENERATE FLATTEN(bsid_acc_ids);

tmp = LIMIT f04_01 10;
DUMP tmp;

f05 = FILTER data BY specificEventType == 'WebPageLoadEvent';
f06 = FOREACH f05 GENERATE 
						SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),1,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), '.', 1)) as time,
						header.channelSessionId as bsid, 
						body#'pageURL' as pageurl;

tmp = LIMIT f06 10;
DUMP tmp;

f07 = GROUP f06 by bsid;
tmp = LIMIT f07 10;
DUMP tmp;

f08 = FOREACH f07 {
					sort = ORDER f06 BY time;
                    first = LIMIT sort 1;
                    GENERATE FLATTEN(first);
                    };
                    
tmp = LIMIT f08 10;
DUMP tmp;


f09 = JOIN 
			f04_01 BY $0,
            f08 BY $1;
            
            
tmp = LIMIT f09 10;
DUMP tmp;


STORE f09 INTO 'cap1_ease_customer_details-09thFeb18_to_18thFeb18_1.tsv';
