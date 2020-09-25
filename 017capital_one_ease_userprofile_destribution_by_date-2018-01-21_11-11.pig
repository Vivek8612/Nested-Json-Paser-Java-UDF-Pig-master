SET tez.staging-dir '/tmp/vkumar/staging';
SET tez.queue.name 'dsg'; 

--------------------------------------------
---------   Setting Memory Size  -----------
--------------------------------------------
set mapreduce.map.memory.mb    38400
set mapreduce.reduce.memory.mb 38400
set tez.am.resource.memory.mb  8192

--------------------------------------------
---------    Registering jars    -----------
--------------------------------------------
REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/preddy/CapOne/get_account_ids_v4.py' using jython as fn_acc_id;
REGISTER 'hdfs:///user/preddy/CapOne/webpage_UDF_v2.py' using jython as webpage_udf;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

--------------------------------------------
---------      Loading Data      -----------`
--------------------------------------------
%DECLARE YEAR '2018'
%DECLARE MONTH '{02}'
%DECLARE DAY '{04,05,06,07,08,09,10}'

data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAY/*/*/*.avro' using LOAD_IDM;

--------------------------------------------
---------  Loading PageLoadData  -----------
--------------------------------------------
pageLoadFilter = FILTER data BY specificEventType == 'WebPageLoadEvent';

pageLoadInfo = FOREACH pageLoadFilter GENERATE 
											header.timeEpochMillisUTC as time,
											header.channelSessionId as csid,
											custom#'profileReferenceID' as prID:chararray;

pageLoadInfoNoTime = FOREACH pageLoadInfo GENERATE csid, prID;

pageLoadInfoDistinct = DISTINCT pageLoadInfoNoTime;

uniqueChannelSessionIDCount = FOREACH (GROUP pageLoadInfoDistinct BY csid) GENERATE group, COUNT_STAR(pageLoadInfoDistinct);
uniqueChannelSessionIDCount_01 = FOREACH uniqueChannelSessionIDCount GENERATE $0 as csid, $1 as csidcount;

pageLoadInfoWithCount = JOIN 
							pageLoadInfoDistinct BY $0 LEFT OUTER,
							uniqueChannelSessionIDCount_01 BY $0;

pageLoadInfoWithCount_01 = FOREACH pageLoadInfoWithCount GENERATE
															$0 as csid,
															$1 as prID,
															$3 as csidcount;

pageLoadInfoWithCount_02 = FILTER pageLoadInfoWithCount_01 BY csidcount == 1;

pageLoadInfoWithCount_03 = DISTINCT (FILTER pageLoadInfoWithCount_01 BY (csidcount > 1 and prID is not null and prID != ''));

pageLoadInfoWithCount_04 = UNION pageLoadInfoWithCount_02, pageLoadInfoWithCount_03;

pageLoadInfoWithCount_05 = FOREACH pageLoadInfoWithCount_04 GENERATE 
																CONCAT(csid, prID) as csidNprID,
																$0 as csid,
																$1 as prID,
																$2 as csidcount;

pageLoadInfoTimeLookup = FOREACH pageLoadInfo GENERATE 
												CONCAT($1, $2) as csidNprID, time, csid, prID;

pageLoadInfoTimeLookupDist = DISTINCT pageLoadInfoTimeLookup;

pageLoadInfo_Ordered = ORDER pageLoadInfoTimeLookupDist BY $0, $1;

pageLoadGroup = GROUP pageLoadInfo_Ordered BY $0;

pageLoadGroupSorted = FOREACH pageLoadGroup {
                                            distinctData = DISTINCT pageLoadInfo_Ordered;
                                            sortData = ORDER distinctData BY $0 DESC;
                                            first = LIMIT sortData 1;
                                            GENERATE group as csidNprID, FLATTEN(first);
};

pageLoadGroupSorted_01 = FOREACH pageLoadGroupSorted GENERATE $0 as csidNprID, $2 as time;

pageLoadInfoWithCountNTime = JOIN 
							pageLoadInfoWithCount_05 BY $0 LEFT OUTER,
							pageLoadGroupSorted_01 BY $0;



pageLoadInfoWithCountNTime_01 = FOREACH pageLoadInfoWithCountNTime GENERATE 
																		$0 as csidNprID,
																		$5 as time,
																		$1 as csid,
																		$2 as prID,
																		$3 as csidcount;

pageLoadInfoWithCountNTimeDist = DISTINCT pageLoadInfoWithCountNTime_01;

tmp = LIMIT pageLoadInfoWithCountNTimeDist 10;
DUMP tmp;

--------------------------------------------
---------   Loading Queue Push   -----------
--------------------------------------------
QueuePushFilter = FILTER data BY specificEventType == 'WebCustomEvent';
QueuePushInfo = FOREACH QueuePushFilter GENERATE 
											header.channelSessionId as bsid, 
											custom#'tfsProfileReferenceID' as profileReferenceID,
											custom#'tfsAcctIdStatusAndProductIdentifier' as accountIdentifier:chararray,
											custom#'tfsCollectionAcctData' as collCatCode;

QueuePushInfo_Size = FOREACH QueuePushInfo GENERATE
													CONCAT(bsid, profileReferenceID) as bsidNprID,
													bsid,
													profileReferenceID,
													accountIdentifier,
													collCatCode;

QueuePush = FILTER QueuePushInfo_Size BY (accountIdentifier !='' and accountIdentifier is not null);

QueuePushGroup = GROUP QueuePush BY $0;

QueuePushGroupSorted = FOREACH QueuePushGroup {
												distinctData = DISTINCT QueuePush;
												sortData = ORDER distinctData BY bsidNprID DESC;
												first = LIMIT sortData 1;
												GENERATE group as bsidNprID, FLATTEN(first);
};

QueuePushGroupSorted2 = FOREACH QueuePushGroupSorted GENERATE $0 as bsidNprID, $2 as bsid, $3 as profileReferenceID, $4 as accountIdentifier, $5 as collCatCode;

QueuePushInfoDistinct = DISTINCT QueuePushGroupSorted2;

pageLoadWithQueuePush = JOIN 
								pageLoadInfoWithCountNTimeDist BY $0 LEFT OUTER,
								QueuePushInfoDistinct BY $0;


pageLoadWithQueuePush_01 = FOREACH pageLoadWithQueuePush GENERATE
																$0 as csidNprID,
																SUBSTRING(ToString(ToDate((long)$1)),0,INDEXOF(ToString(ToDate((long)$1)), 'T', 1)) as date,
																$2 as csid,
																$3 as prID,
																$4 as csidcount,
																(($3 is null or $3 == '' or $3 == 'UNKNOWN') ? 'UNKNOWN' : SUBSTRING($3, 0, 1)) as prID1,
																$8 as accountDetails,
																((INDEXOF($8, '|CC|',0) >0 ) ? 'CC Account' : 'Non CC Account') as AccountType,
																$9 as collCatCode,
																((collCatCode is not null) ? 'Not Collections' : 'Collections') as CollectionStatus;
pageLoadWithQueuePushDistinct = DISTINCT pageLoadWithQueuePush_01;
tmp = LIMIT pageLoadWithQueuePushDistinct 10;
DUMP tmp;

--------------------------------------------
---------  Loading User Profile  -----------
--------------------------------------------
userProfileFilter = FILTER data BY specificEventType == 'WebCustomEvent';
userProfileInfo = FOREACH userProfileFilter GENERATE 
						header.channelSessionId as bsid, 
						custom#'userProfile' as user_profile:chararray;

userProfile = FILTER userProfileInfo BY (user_profile !='' and user_profile is not null);

userProfile_01 = FOREACH userProfile GENERATE
										bsid,
										user_profile,
										SUBSTRING(user_profile, (int)(INDEXOF(user_profile, ':', 0)+2), (int)(INDEXOF(user_profile, ',', 0)-1)) as prID;

userProfile_02 = FOREACH userProfile_01 GENERATE
											CONCAT(bsid, prID) as bsidNprID,
											bsid,
											prID,
											user_profile;

userProfile_Ordered = ORDER userProfile_02 BY bsidNprID DESC;

userProfileGroup = GROUP userProfile_Ordered BY $0;

userProfileGroupSorted = FOREACH userProfileGroup {
												distinctData = DISTINCT userProfile_Ordered;
												sortData = ORDER distinctData BY bsidNprID DESC;
												first = LIMIT sortData 1;
												GENERATE group as bsidNprID, FLATTEN(first);
};

userProfileGroupSorted2 = FOREACH userProfileGroupSorted GENERATE 
															$0 as bsidNprID, 
															$2 as bsid,
															$3 as prID,
															$4 as user_profile;



userProfileInfoDistinct = DISTINCT userProfileGroupSorted2;

pageLoadWithUserProfile = JOIN 
								pageLoadWithQueuePushDistinct BY $0 LEFT OUTER,
								userProfileInfoDistinct BY $0;

tmp = LIMIT pageLoadWithUserProfile 10;
DUMP tmp;

pageLoadWithUserProfile_01 = FOREACH pageLoadWithUserProfile GENERATE
																$0 as csidNprID,
																$1 as date,
																$2 as csid,
																$3 as prID,
																$4 as csidcount,
																$5 as prID1,
																$6 as accountDetails,
																$7 as AccountType,
																$8 as collCatCode,
																$9 as CollectionStatus,
																$13 as user_profile,
																(($13 is null or $13 == '' or $13 == 'UNKNOWN') ? 'User Profile - NA' : 'User Profile - Available') as userProfile;

pageLoadWithUserProfileDistinct = DISTINCT pageLoadWithUserProfile_01;

tmp = LIMIT pageLoadWithUserProfileDistinct 10;
DUMP tmp;


pageLoadWithUserProfile_Count = FOREACH (GROUP pageLoadWithUserProfileDistinct BY (date, prID1, AccountType, CollectionStatus, userProfile)) GENERATE group, COUNT_STAR(pageLoadWithUserProfileDistinct); 
DUMP pageLoadWithUserProfile_Count;
