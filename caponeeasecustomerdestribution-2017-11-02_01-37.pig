REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

REGISTER 'hdfs:///user/preddy/CapOne/num_accounts.py' using jython as num_accounts;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
%DECLARE MONTH '{10}'
%DECLARE DAYS '{01,02,03,04,05,06,07}'
--%DECLARE DAYS '{01}'

--data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

f1 = FILTER data BY specificEventType == 'WebCustomEvent';

f2 = FOREACH f1 GENERATE 
header.timeEpochMillisUTC as time, 
header.channelSessionId as bsid, 
custom#'tfsPageURL' as pageurl, 
custom#'tfsAcctIdStatusAndProductIdentifier' as AccountReffDetails,
custom#'tfsCollectionAcctData' as Collection_catcode_Details,
custom#'tfsPolicy' as policyID;
--f2 = FOREACH f1 GENERATE header.channelSessionId as bsid, custom#'userProfile' as user_profile;

f3 = FILTER f2 BY (AccountReffDetails !='' and AccountReffDetails is not null);
--f3 = FILTER f2 BY (user_profile !='' and user_profile is not null);

f4 = FILTER f3 BY pageurl == 'https://myaccounts.capitalone.com/accountSummary';
--f4 = FOREACH f3 GENERATE bsid, num_accounts.get_acc_id_count(user_profile);

f5 = GROUP f4 BY bsid;

f6 = FOREACH f5 {
	sort = ORDER f4 BY time;
	first = LIMIT sort 1;
	GENERATE FLATTEN(first);
	};

f7 = FOREACH f6 GENERATE STRSPLIT(AccountReffDetails,'::') as accnts_list;
f8 = FOREACH f7 GENERATE accnts_list.$0 as one_accnt; 
-- $0 above takes the first bank account
f9 = FOREACH f8 GENERATE STRSPLIT(one_accnt,'\\|') as accnt_details;
f10 = FOREACH f9 GENERATE accnt_details.$1 as account_type, accnt_details.$2 as account_status;

grp1 = GROUP f10 BY account_status;
res1 = FOREACH grp1 GENERATE group, COUNT(f10);
DUMP res1

grp2 = GROUP f10 BY account_type;
res2 = FOREACH grp2 GENERATE group, COUNT(f10);
DUMP res2	