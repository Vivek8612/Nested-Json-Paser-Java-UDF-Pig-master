set tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2018'
%DECLARE MONTH '{01}'
%DECLARE DAYS '{18,19,20,21}'
--%DECLARE DAYS '{10}'

data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;
--data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

----------------------------------------------------PageLoadEvent--------------------------------------------------------
pageLoad = FILTER data BY specificEventType == 'WebPageLoadEvent';

Visit = FOREACH pageLoad GENERATE
							SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), 'T', 1)) as time, 
                            header.associativeTag as visitorID, 
							header.channelSessionId as visitID,
                            custom#'profileReferenceID' as profileRefID,
                            custom#'DeviceDetails' as deviceDetail,
                            body#'pageURL' as pageURL;

Visit_01 = FILTER Visit BY pageURL == 'https://myaccounts.capitalone.com/accountSummary';

Visit_02 = FOREACH Visit_01 GENERATE
							time,
							GetMonth(ToDate(time)) as Month,
                            visitorID,
							visitID,
                            profileRefID,
                            SUBSTRING(
										deviceDetail, 
										INDEXOF(deviceDetail , '~', 0)+1, 
										INDEXOF(deviceDetail , '~', INDEXOF(deviceDetail , '~', 0)+1)) 
										as Device_Type,
                            pageURL;

Visit_03 = DISTINCT Visit_02;
tmp = LIMIT Visit_03 10;
DUMP tmp;

Visit_Count = FOREACH (GROUP Visit_03 BY (Month, Device_Type)) GENERATE group, COUNT_STAR(Visit_03); 
DUMP Visit_Count;

noPRIDVisit = FILTER Visit_03 BY (profileRefID == 'UNKNOWN' OR profileRefID == '' OR profileRefID is null);
noPRIDVisit_Count = FOREACH (GROUP noPRIDVisit ALL) GENERATE COUNT_STAR(noPRIDVisit); 
DUMP noPRIDVisit_Count;

/*
----------------------------------------------------HotLeadEvent--------------------------------------------------------
hotLead = FILTER data BY specificEventType == 'TargetDeterminationEvent';

hotLeadVisit = FOREACH hotLead GENERATE
							SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), 'T', 1)) as time, 
                            header.associativeTag as hotLeadVisitorID, 
							header.channelSessionId as hotLeadVisitID,
                            --custom#'profileReferenceID' as profileRefID,
                            custom#'DeviceDetails' as deviceDetail,
                            body#'pageURL' as pageURL;

--hotLeadVisit_01 = FILTER hotLeadVisit BY pageURL == 'https://myaccounts.capitalone.com/accountSummary';

hotLeadVisit_02 = FOREACH hotLeadVisit GENERATE
							time,
							GetMonth(ToDate(time)) as Month,
                            hotLeadVisitorID,
							hotLeadVisitID,
                            --profileRefID,
                            SUBSTRING(
										deviceDetail, 
										INDEXOF(deviceDetail , '~', 0)+1, 
										INDEXOF(deviceDetail , '~', INDEXOF(deviceDetail , '~', 0)+1)) 
										as Device_Type;
                            --pageURL,
                            

hotLeadVisit_03 = DISTINCT hotLeadVisit_02;
tmp = LIMIT hotLeadVisit_03 10;
DUMP tmp;

hotLeadVisit_Count = FOREACH (GROUP hotLeadVisit_03 BY (Month, Device_Type)) GENERATE group, COUNT_STAR(hotLeadVisit_03); 
DUMP hotLeadVisit_Count;

--noPRIDhotLeadVisit = FILTER hotLeadVisit_03 BY (profileRefID == 'UNKNOWN' OR profileRefID == '' OR profileRefID is null);
--noPRIDhotLeadVisit_Count = FOREACH (GROUP noPRIDhotLeadVisit ALL) GENERATE COUNT_STAR(noPRIDhotLeadVisit); 
--DUMP noPRIDhotLeadVisit_Count;





----------------------------------------------------InvitationOfferEvent--------------------------------------------------------
invitationOffer = FILTER data BY specificEventType == 'OnlineInvitationOfferEvent';

invitationOfferVisit = FOREACH invitationOffer GENERATE
							SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), 'T', 1)) as time, 
                            header.associativeTag as invitationOfferVisitorID, 
							header.channelSessionId as invitationOfferVisitID,
                            --custom#'profileReferenceID' as profileRefID,
                            custom#'DeviceDetails' as deviceDetail,
                            body#'pageURL' as pageURL;

--invitationOfferVisit_01 = FILTER invitationOfferVisit BY pageURL == 'https://myaccounts.capitalone.com/accountSummary';

invitationOfferVisit_02 = FOREACH invitationOfferVisit GENERATE
							time,
							GetMonth(ToDate(time)) as Month,
                            invitationOfferVisitorID,
							invitationOfferVisitID,
                            --profileRefID,
                            SUBSTRING(
										deviceDetail, 
										INDEXOF(deviceDetail , '~', 0)+1, 
										INDEXOF(deviceDetail , '~', INDEXOF(deviceDetail , '~', 0)+1)) 
										as Device_Type;
                            --pageURL,


invitationOfferVisit_03 = DISTINCT invitationOfferVisit_02;
tmp = LIMIT invitationOfferVisit_03 10;
DUMP tmp;

invitationOfferVisit_Count = FOREACH (GROUP invitationOfferVisit_03 BY (Month, Device_Type)) GENERATE group, COUNT_STAR(invitationOfferVisit_03); 
DUMP invitationOfferVisit_Count;

--noPRIDinvitationOfferVisit = FILTER invitationOfferVisit_03 BY (profileRefID == 'UNKNOWN' OR profileRefID == '' OR profileRefID is null);
--noPRIDinvitationOfferVisit_Count = FOREACH (GROUP noPRIDinvitationOfferVisit ALL) GENERATE COUNT_STAR(noPRIDinvitationOfferVisit); 
--DUMP noPRIDinvitationOfferVisit_Count;





----------------------------------------------------OnlineInvitationResponseEvent--------------------------------------------------------
invitationResponse = FILTER data BY specificEventType == 'OnlineInvitationResponseEvent';

invitationResponseVisit = FOREACH invitationResponse GENERATE
							SUBSTRING(ToString(ToDate((long)header.timeEpochMillisUTC)),0,INDEXOF(ToString(ToDate((long)header.timeEpochMillisUTC)), 'T', 1)) as time, 
                            header.associativeTag as invitationResponseVisitorID, 
							header.channelSessionId as invitationResponseVisitID,
                            --custom#'profileReferenceID' as profileRefID,
                            custom#'DeviceDetails' as deviceDetail,
                            body#'pageURL' as pageURL;

invitationResponseVisit_01 = FILTER invitationResponseVisit BY pageURL == 'https://myaccounts.capitalone.com/accountSummary';

invitationResponseVisit_02 = FOREACH invitationResponseVisit GENERATE
							time,
							GetMonth(ToDate(time)) as Month,
                            invitationResponseVisitorID,
							invitationResponseVisitID,
                            --profileRefID,
                            SUBSTRING(
										deviceDetail, 
										INDEXOF(deviceDetail , '~', 0)+1, 
										INDEXOF(deviceDetail , '~', INDEXOF(deviceDetail , '~', 0)+1)) 
										as Device_Type;
                            --pageURL,
                            

invitationResponseVisit_03 = DISTINCT invitationResponseVisit_02;
tmp = LIMIT invitationResponseVisit_03 10;
DUMP tmp;

invitationResponseVisit_Count = FOREACH (GROUP invitationResponseVisit_03 BY (Month, Device_Type)) GENERATE group, COUNT_STAR(invitationResponseVisit_03); 
DUMP invitationResponseVisit_Count;

--noPRIDinvitationResponseVisit = FILTER invitationResponseVisit_03 BY (profileRefID == 'UNKNOWN' OR profileRefID == '' OR profileRefID is null);
--noPRIDinvitationResponseVisit_Count = FOREACH (GROUP noPRIDinvitationResponseVisit ALL) GENERATE COUNT_STAR(noPRIDinvitationResponseVisit); 
--DUMP noPRIDinvitationResponseVisit_Count;



/*
f7 = FOREACH f6 GENERATE STRSPLIT(AccountReffDetails,'::') as accnts_list;
f8 = FOREACH f7 GENERATE accnts_list.$0 as one_accnt; 
-- $0 above takes the first bank account
f9 = FOREACH f8 GENERATE STRSPLIT(one_accnt,'\\|') as accnt_details;
f10 = FOREACH f9 GENERATE accnt_details.$1 as account_type, accnt_details.$2 as account_status;


f2 = FOREACH f1 GENERATE header.timeEpochMillisUTC as time, 
header.associativeTag as vi, 
header.channelSessionId as bsid, 
custom#'Queue' as Queue,
custom#'Browser' as Browser, 
custom#'DeviceDetails' as DeviceDetails;

tmp = LIMIT f2 10;
DUMP tmp;

f3 = FOREACH f2 GENERATE time, 
vi, 
bsid, 
Queue, 
--SUBSTRING(Queue, INDEXOF(Queue , 'queue-', 0)+1, 3) as Queue_Name,
(INDEXOF(Queue , 'queue-', 0)+5) as Queue_Name_Start, 
LENGHT(Queue) as Queue_Name_End, 
Browser,
SUBSTRING(Browser, 0, 1) as Browser_Type, 
DeviceDetails, 
SUBSTRING(DeviceDetails, INDEXOF(DeviceDetails , '~', 0)+1, INDEXOF(DeviceDetails , '~', INDEXOF(DeviceDetails , '~', 0)+1)) as Device_Type;
--Substring(DeviceDetails, INDEXOF(DeviceDetails , '~', 0), INDEXOF(DeviceDetails , '~', INDEXOF(DeviceDetails , '~', 0)+1)) as Device_Type;

tmp = LIMIT f3 10;
DUMP tmp;

--f3 = FILTER f2 BY (account_ref_details != '' and account_ref_details is not null);

--f4 = FILTER f3 BY pageurl == 'https://myaccounts.capitalone.com/accountSummary';

--tmp = LIMIT f4 100;
--DUMP tmp;

--f5 = GROUP f4 BY bsid;

--f6 = FOREACH f5 {
--                sort = ORDER f4 BY time;
--                first = LIMIT sort 1;
--                GENERATE FLATTEN(first);
--                };

--f7 = FOREACH f6 GENERATE time, bsid, profile_reference_id, 
--get_all_account_details_v3.extract_account_info(account_ref_details, collec_cat_code_data) as cust_account_details, 
--policyID;

--tmp = LIMIT f7 100;
--DUMP tmp;

--STORE f7 INTO 'cap1_ease_account_details_vivek.tsv';


--f8 = FOREACH f7 GENERATE time, 
--bsid, 
--profile_reference_id, 
--cust_account_details#'num_accounts' as num_accounts, 
--cust_account_details#'product_details' as product_details, 
--cust_account_details#'account_status' as account_status,
--cust_account_details#'collection_details' as collection_details,
--policyID; 

--tmp = LIMIT f8 100;
--DUMP tmp;

--STORE f8 INTO 'cap1_ease_account_details_01.tsv';
*/