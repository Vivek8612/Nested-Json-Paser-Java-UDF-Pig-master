REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

REGISTER 'hdfs:///user/vkumar/udf/get_account_ids_v5.py' using jython as fn_acc_id;
REGISTER 'hdfs:///user/preddy/CapOne/cust_json_parser.py' using jython as cust_json_praser;
REGISTER 'hdfs:///user/vkumar/udf/get_all_account_details_v4.py' using jython as get_all_account_details_v4;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
%DECLARE MONTH '{11}'
%DECLARE DAYS '{01}'

data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;

f1 = FILTER data BY specificEventType == 'WebCustomEvent';

f2 = FOREACH f1 GENERATE 
				header.timeEpochMillisUTC as time, 
				header.channelSessionId as bsid, 
				custom#'tfsProfileReferenceID' as profile_reference_id,
				custom#'tfsPageURL' as pageurl, 
				custom#'tfsAcctIdStatusAndProductIdentifier' as account_ref_details,
				custom#'tfsCollectionAcctData' as collec_cat_code_data,
				custom#'tfsPolicy' as policyID;

tmp = LIMIT f2 10;
DUMP tmp;

f3 = FILTER f2 BY (account_ref_details != '' and account_ref_details is not null);

f4 = FILTER f3 BY pageurl == 'https://myaccounts.capitalone.com/accountSummary';

f5 = GROUP f4 BY bsid;

f6 = FOREACH f5 {
                sort = ORDER f4 BY time;
                first = LIMIT sort 1;
                GENERATE FLATTEN(first);
                };

tmp = LIMIT f6 10;
DUMP tmp;		

f7 = FOREACH f6 GENERATE 
				SUBSTRING(ToString(ToDate((long)time)),1,INDEXOF(ToString(ToDate((long)time)), 'T', 1)) as time, 
				bsid, 
				profile_reference_id, 
				get_all_account_details_v4.extract_account_info(account_ref_details, collec_cat_code_data) as cust_account_details, 
				policyID;	

tmp = LIMIT f7 10;
DUMP tmp;
				
f8 = FOREACH f7 GENERATE 
				time, 
				bsid, 
				profile_reference_id, 
                cust_account_details#'product_missing_flag' as product_missing_flag,
				cust_account_details#'num_accounts' as num_accounts, 
                cust_account_details#'product_id' as product_id,
				cust_account_details#'product_details' as product_details, 
				cust_account_details#'account_status' as account_status,
                cust_account_details#'collection_id' as collection_id,
				cust_account_details#'collection_details' as collection_details,
				policyID; 
	
tmp = LIMIT f8 10;
DUMP tmp;


f9 = FOREACH f1 GENERATE 
				header.timeEpochMillisUTC as time,
				header.channelSessionId as bsid, 
                custom#'userProfile' as user_profile;

f10 = FILTER f9 BY (user_profile !='' and user_profile is not null);

tmp = LIMIT f10 10;
DUMP tmp;

f11 = FOREACH f10 GENERATE 
					SUBSTRING(ToString(ToDate((long)time)),1,INDEXOF(ToString(ToDate((long)time)), 'T', 1)) as time,
                    bsid, 
                    fn_acc_id.extract_account_ids($1,$2) as bsid_acc_ids;

tmp = LIMIT f11 10;
DUMP tmp;

f12 = JOIN 
			f7 by $1,
			f11 by $1;
            

tmp = LIMIT f12 10;
DUMP tmp;
