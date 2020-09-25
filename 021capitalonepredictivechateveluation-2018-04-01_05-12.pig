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

--------------------------------------------
---------      Loading Data      -----------
--------------------------------------------
%DECLARE YEAR '2018'
%DECLARE MONTH01 '{05}'
%DECLARE MONTH02 '{06}'

--%DECLARE DAY '{13,14,15,16,17,18,19,20,21,22,23,24,25,26,27}'

--%DECLARE WDAY01 '{26,27,28,29,30}'
--%DECLARE WDAY02 '{01,02,03,04,05,06,07,08,09,10,11,12,13}'
%DECLARE WDAY01 '{30}'

%DECLARE SDAY01 '{30,31}'
%DECLARE SDAY02 '{01,02,03}'
--%DECLARE SDAY02 '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17}'

--wdata01 = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH01/day=$WDAY01/*/*/*.avro' using LOAD_IDM;
--wdata02 = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH02/day=$WDAY02/*/*/*.avro' using LOAD_IDM;
--wdata = UNION wdata01, wdata02;

wdata = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH01/day=$WDAY01/*/*/*.avro' using LOAD_IDM;



sdata01 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH01/day=$SDAY01/*/*/*.avro' using LOAD_IDM;
sdata02 = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH02/day=$SDAY02/*/*/*.avro' using LOAD_IDM;
sdata = UNION sdata01, sdata02;

--sdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH02/day=$SDAY02/*/*/*.avro' using LOAD_IDM;

--------------------------------------------
---------  Loading PageLoadData  -----------
--------------------------------------------
pageLoadFilter = FILTER wdata BY specificEventType == 'WebPageLoadEvent';

pageLoadInfo = FOREACH pageLoadFilter GENERATE 
											header.timeEpochMillisUTC as time,
											header.channelSessionId as csid,
											custom#'profileReferenceID' as prID:chararray,
											body#'pageURL' as pageURL,
											custom#'language' as language:chararray;

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

pageLoadInfoWithCount_01Count = FOREACH (GROUP pageLoadInfoWithCount_01 ALL) GENERATE group, COUNT_STAR(pageLoadInfoWithCount_01);
--pageLoadInfoWithCount_01Sum = FOREACH (GROUP pageLoadInfoWithCount_01 ALL) GENERATE SUM(pageLoadInfoWithCount_01.csidcount);
pageLoadInfoWithCount_02Count = FOREACH (GROUP pageLoadInfoWithCount_02 ALL) GENERATE group, COUNT_STAR(pageLoadInfoWithCount_02);
pageLoadInfoWithCount_03Count = FOREACH (GROUP pageLoadInfoWithCount_03 ALL) GENERATE group, COUNT_STAR(pageLoadInfoWithCount_03);


--DUMP pageLoadInfoWithCount_01Count;
--5,257,955
--DUMP pageLoadInfoWithCount_02Count;
--4,458,104
--DUMP pageLoadInfoWithCount_03Count;
--0,799,851

pageLoadInfoWithCount_04 = UNION pageLoadInfoWithCount_02, pageLoadInfoWithCount_03;

pageLoadInfoWithCount_05 = FOREACH pageLoadInfoWithCount_04 GENERATE 
																CONCAT(csid, prID) as csidNprID,
																$0 as csid,
																$1 as prID,
																$2 as csidcount;

pageLoadInfoTimeLookup = FOREACH pageLoadInfo GENERATE 
												CONCAT($1, $2) as csidNprID, time, csid, prID, language;

pageLoadInfoTimeLookupDist = DISTINCT pageLoadInfoTimeLookup;

pageLoadInfo_Ordered = ORDER pageLoadInfoTimeLookupDist BY $0, $1;

pageLoadGroup = GROUP pageLoadInfo_Ordered BY $0;

pageLoadGroupSorted = FOREACH pageLoadGroup {
                                            distinctData = DISTINCT pageLoadInfo_Ordered;
                                            sortData = ORDER distinctData BY $0 DESC;
                                            first = LIMIT sortData 1;
                                            GENERATE group as csidNprID, FLATTEN(first);
};

pageLoadGroupSorted_01 = FOREACH pageLoadGroupSorted GENERATE $0 as csidNprID, $2 as time, $5 as language;

pageLoadInfoWithCountNTime = JOIN 
								pageLoadInfoWithCount_05 BY $0 LEFT OUTER,
								pageLoadGroupSorted_01 BY $0;



pageLoadInfoWithCountNTime_01 = FOREACH pageLoadInfoWithCountNTime GENERATE 
																		$0 as csidNprID,
																		$5 as time,
																		$1 as csid,
																		$2 as prID,
																		$3 as csidcount,
																		$6 as language;

pageLoadInfoWithCountNTimeDist = DISTINCT pageLoadInfoWithCountNTime_01;

--https://myaccounts.capitalone.com/accountSummary
--https://myaccounts.capitalone.com/alerts
--https://myaccounts.capitalone.com/Settings
--https://myaccounts.capitalone.com/security


tmp = LIMIT pageLoadInfoWithCountNTimeDist 10;
DUMP tmp;

--------------------------------------------
---------  Loading HotLeadData   -----------
--------------------------------------------
HotLeadFilter = FILTER wdata BY specificEventType == 'TargetDeterminationEvent';

HotLeadInfo = FOREACH HotLeadFilter GENERATE 
											header.timeEpochMillisUTC as time,
											header.channelSessionId as csid,
											custom#'profileReferenceID' as prID:chararray,
											custom#'pageURL' as pageURL:chararray,
											custom#'RuleId' as ruleID;

HotLeadInfoNoTime = FOREACH HotLeadInfo GENERATE csid, prID;

HotLeadInfoDistinct = DISTINCT HotLeadInfoNoTime;

uniqueChannelSessionIDCount = FOREACH (GROUP HotLeadInfoDistinct BY csid) GENERATE group, COUNT_STAR(HotLeadInfoDistinct);
uniqueChannelSessionIDCount_01 = FOREACH uniqueChannelSessionIDCount GENERATE $0 as csid, $1 as csidcount;

HotLeadInfoWithCount = JOIN 
							HotLeadInfoDistinct BY $0 LEFT OUTER,
							uniqueChannelSessionIDCount_01 BY $0;

HotLeadInfoWithCount_01 = FOREACH HotLeadInfoWithCount GENERATE
															$0 as csid,
															$1 as prID,
															$3 as csidcount;

HotLeadInfoWithCount_02 = FILTER HotLeadInfoWithCount_01 BY csidcount == 1;

HotLeadInfoWithCount_03 = DISTINCT (FILTER HotLeadInfoWithCount_01 BY (csidcount > 1 and prID is not null and prID != ''));

HotLeadInfoWithCount_01Count = FOREACH (GROUP HotLeadInfoWithCount_01 ALL) GENERATE group, COUNT_STAR(HotLeadInfoWithCount_01);
--HotLeadInfoWithCount_01Sum = FOREACH (GROUP HotLeadInfoWithCount_01 ALL) GENERATE SUM(HotLeadInfoWithCount_01.csidcount);
HotLeadInfoWithCount_02Count = FOREACH (GROUP HotLeadInfoWithCount_02 ALL) GENERATE group, COUNT_STAR(HotLeadInfoWithCount_02);
HotLeadInfoWithCount_03Count = FOREACH (GROUP HotLeadInfoWithCount_03 ALL) GENERATE group, COUNT_STAR(HotLeadInfoWithCount_03);


--DUMP HotLeadInfoWithCount_01Count;
--1,431,635
--DUMP HotLeadInfoWithCount_02Count;
--1,279,084
--DUMP HotLeadInfoWithCount_03Count;
--0,151,684

HotLeadInfoWithCount_04 = UNION HotLeadInfoWithCount_02, HotLeadInfoWithCount_03;

HotLeadInfoWithCount_05 = FOREACH HotLeadInfoWithCount_04 GENERATE 
																CONCAT(csid, prID) as csidNprID,
																$0 as csid,
																$1 as prID,
																$2 as csidcount;

HotLeadInfoTimeLookup = FOREACH HotLeadInfo GENERATE 
												CONCAT($1, $2) as csidNprID, time, csid, prID, pageURL, ruleID;

HotLeadInfoTimeLookupDist = DISTINCT HotLeadInfoTimeLookup;

HotLeadInfo_Ordered = ORDER HotLeadInfoTimeLookupDist BY $0, $1;

HotLeadGroup = GROUP HotLeadInfo_Ordered BY $0;

HotLeadGroupSorted = FOREACH HotLeadGroup {
                                            distinctData = DISTINCT HotLeadInfo_Ordered;
                                            sortData = ORDER distinctData BY $0 DESC;
                                            first = LIMIT sortData 1;
                                            GENERATE group as csidNprID, FLATTEN(first);
};

HotLeadGroupSorted_01 = FOREACH HotLeadGroupSorted GENERATE $0 as csidNprID, $2 as time, $5 as pageURL, $6 as ruleID;

HotLeadInfoWithCountNTime = JOIN 
								HotLeadInfoWithCount_05 BY $0 LEFT OUTER,
								HotLeadGroupSorted_01 BY $0;



HotLeadInfoWithCountNTime_01 = FOREACH HotLeadInfoWithCountNTime GENERATE 
																		$0 as csidNprID,
																		$5 as time,
																		$1 as csid,
																		$2 as prID,
																		$3 as csidcount,
																		$6 as pageURL,
																		$7 as ruleID;

HotLeadInfoWithCountNTimeDist = DISTINCT HotLeadInfoWithCountNTime_01;

--tmp = LIMIT HotLeadInfoWithCountNTimeDist 10;
--DUMP tmp;

pageLoadHotLeadJoin = JOIN 
						pageLoadInfoWithCountNTimeDist BY $0 FULL OUTER, 
						HotLeadInfoWithCountNTimeDist BY $0;

pageLoadHotLeadDistinct_01 = DISTINCT pageLoadHotLeadJoin;

--tmp = LIMIT pageLoadHotLeadDistinct_01 10;
--DUMP tmp;

pageLoadHotLeadDistinct_02 = FOREACH pageLoadHotLeadDistinct_01 GENERATE
																$0 as csidNprID:chararray,
																SUBSTRING(ToString(ToDate((long)$1)),0,INDEXOF(ToString(ToDate((long)$1)), '.', 1)) as pltime,
																$2 as csid:chararray,
																$3 as prID:chararray,
																$4 as plcsidcount,
																$5 as language:chararray,
																$6 as hlcsidNprID:chararray,
																SUBSTRING(ToString(ToDate((long)$7)),0,INDEXOF(ToString(ToDate((long)$7)), '.', 1)) as hltime,
																$8 as hlcsid:chararray,
																$9 as hlprID:chararray,
																$10 as hlcsidcount,
																$11 as pageURL,
																$12 as ruleID;

--tmp = LIMIT pageLoadHotLeadDistinct_02 10;
--DUMP tmp;

pageLoadHotLeadDistinct_03 = FOREACH pageLoadHotLeadDistinct_02 GENERATE
																(($0 is null or $0 == '') ? $5 : $0) as csidNprID,
																(($1 is null or $1 == '') ? $7 : $1) as pltime,
																(($2 is null or $2 == '') ? $8 : $2) as csid,
																(($3 is null or $3 == '') ? $9 : $3) as prID,
																$4 as plcsidcount,
																$5 as language,
																$7 as hltime,
																$10 as hlcsidcount,
																$11 as pageURL,
																$12 as ruleID;

pageLoadHotLeadDistinct_04 = FILTER pageLoadHotLeadDistinct_03 BY (csidNprID !='' and csidNprID is not null); 

pageLoadHotLeadDistinct= DISTINCT pageLoadHotLeadDistinct_04;

tmp = LIMIT pageLoadHotLeadDistinct 10;
DUMP tmp;


pageLoadHotLeadDistinct_03Count = FOREACH (GROUP pageLoadHotLeadDistinct_03 ALL) GENERATE group, COUNT_STAR(pageLoadHotLeadDistinct_03);
pageLoadHotLeadDistinct_04Count = FOREACH (GROUP pageLoadHotLeadDistinct_04 ALL) GENERATE group, COUNT_STAR(pageLoadHotLeadDistinct_04);
pageLoadHotLeadDistinctCount = FOREACH (GROUP pageLoadHotLeadDistinct ALL) GENERATE group, COUNT_STAR(pageLoadHotLeadDistinct);

--DUMP pageLoadHotLeadDistinct_03Count;
--6,569,985
--DUMP pageLoadHotLeadDistinct_04Count;
--6,475,517
--DUMP pageLoadHotLeadDistinctCount;
--6,475,517

--------------------------------------------
---------  Loading PageLoadData  -----------
--------------------------------------------
--pageLoadFilter = FILTER data BY specificEventType == 'WebPageLoadEvent';
--pageLoadInfo = FOREACH pageLoadFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as csid, body#'pageTitle' as pageTitle, body#'pageURL' as pageUrl, body#'referrerURL' as referrerURL, custom#'Queue' as queue;
pageLoadInfo_01 = FOREACH pageLoadInfo GENERATE 
												CONCAT($1, $2) as csidNprID, 
												$0 as time, 
												$1 as csid,
												$2 as prID:chararray,
												$3 as pageURL;


pageLoadInfo_02 = FILTER pageLoadInfo_01 BY (csid !='' and csid is not null);

pageLoadGroup = GROUP pageLoadInfo_02 BY $0;

pageLoadGroupSorted = FOREACH pageLoadGroup {
                                            distinctData = DISTINCT pageLoadInfo_02;
											sortData = ORDER distinctData BY $1 DESC;
											first = LIMIT sortData 1;
											GENERATE group as csidNprID, FLATTEN(first);
};
--
--
--pageLoadAccountJoin = JOIN 
--							bsid_account_ids BY $0, 
--							pageLoadGroupSorted BY $0;
--pageLoadAccountJoinDistinct = DISTINCT pageLoadAccountJoin;
--

pageLoadInfo_Last = FOREACH pageLoadGroupSorted GENERATE 
									$0 as csidNprID,
									$2 as time,
									$3 as csid,
									$4 as prID,
									$5 as pageURL;

pageLoadInfo_Last = DISTINCT pageLoadInfo_Last;

tmp = LIMIT pageLoadInfo_Last 10;
DUMP tmp;

--------------------------------------------
---------   Loading Queue Push   -----------
--------------------------------------------
QueuePushFilter = FILTER wdata BY specificEventType == 'WebCustomEvent';
QueuePushInfo = FOREACH QueuePushFilter GENERATE 
											header.channelSessionId as bsid, 
											custom#'tfsProfileReferenceID' as profileReferenceID:chararray,
											custom#'tfsAcctIdStatusAndProductIdentifier' as accountIdentifier:chararray,
											custom#'tfsCollectionAcctData' as collCatCode:chararray,
											custom#'tfsLanguage' as language:chararray;

QueuePushInfo_Size = FOREACH QueuePushInfo GENERATE
													CONCAT(bsid, profileReferenceID) as bsidNprID,
													bsid,
													profileReferenceID,
													accountIdentifier,
													collCatCode,
													language,
													SIZE(accountIdentifier) as accLen;

QueuePush = FILTER QueuePushInfo_Size BY (accountIdentifier !='' and accountIdentifier is not null);

QueuePushGroup = GROUP QueuePush BY $0;

QueuePushGroupSorted = FOREACH QueuePushGroup {
												distinctData = DISTINCT QueuePush;
												sortData = ORDER distinctData BY bsidNprID, accLen DESC;
												first = LIMIT sortData 1;
												GENERATE group as bsidNprID, FLATTEN(first);
};

QueuePushGroupSorted2 = FOREACH QueuePushGroupSorted GENERATE 
														$0 as bsidNprID, 
														$2 as bsid, 
														$3 as profileReferenceID, 
														$4 as accountIdentifier, 
														$5 as collCatCode, 
														$6 as language;

QueuePushInfoDistinct = DISTINCT QueuePushGroupSorted2;

pageLoadWithQueuePush = JOIN 
								pageLoadHotLeadDistinct BY $0 LEFT OUTER,
								QueuePushInfoDistinct BY $0;


pageLoadWithQueuePush_01 = FOREACH pageLoadWithQueuePush GENERATE
																$0 as csidNprID,
																$1 as pltime,
																$2 as csid,
																$3 as prID,
																$4 as plcsidcount,
																$5 as pllanguage,
																$6 as hltime,
																$7 as hlcsidcount,
																$8 as pageURL, 
																$9 as ruleID,
																(($3 is null or $3 == '' or $3 == 'UNKNOWN' or $3 == 'NA' or $3 == 'anonymized') ? 'UNKNOWN' : SUBSTRING($3, 0, 1)) as prID1,
																$13 as accountDetails,
																((INDEXOF($13, '|CC|',0) > 0 ) ? 'CC Account' : 'Non CC Account') as AccountType,
																$14 as collCatCode,
																(($14 is not null and $14 !='' and $14 != 'null' and $14 != 'UNKNOWN' and $14 != 'NA' and SIZE($14) > 10) ? 'Collections' : 'Not Collections') as CollectionStatus,
																$15 as wclanguage;
pageLoadWithQueuePushDistinct = DISTINCT pageLoadWithQueuePush_01;
tmp = LIMIT pageLoadWithQueuePushDistinct 10;
DUMP tmp;

--------------------------------------------
---------  Loading User Profile  -----------
--------------------------------------------
userProfileFilter = FILTER wdata BY specificEventType == 'WebCustomEvent';
userProfileInfo = FOREACH userProfileFilter GENERATE 
						header.channelSessionId as bsid, 
						custom#'userProfile' as user_profile:chararray;

userProfile = FILTER userProfileInfo BY (user_profile !='' and user_profile is not null);

--------------------------------------------
--------  Processing for WEB MAP  ----------
--------------------------------------------
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

--tmp = LIMIT pageLoadWithUserProfile 10;
--DUMP tmp;

pageLoadWithUserProfile_01 = FOREACH pageLoadWithUserProfile GENERATE
																$0 as csidNprID,
																SUBSTRING($1,0,INDEXOF($1, 'T', 1)) as date,
																$1 as pltime,
																$2 as csid,
																$3 as prID,
																$4 as csidcount,
																$5 as pllanguage,
																$6 as hltime,
																$7 as hlcsidcount,
																$8 as hlpageURL,
																$9 as ruleID,
																$10 as prID1,
																$11 as accountDetails,
																$12 as AccountType,
																$13 as collCatCode,
																$14 as CollectionStatus,
																$15 as wclanguage,
																$19 as user_profile,
																(($9 is null or $9 == '' or $9 == 'null' or $9 == 'UNKNOWN') ? 'Not Hotlead' : 'HotLead') as hotLead,
																(($19 is null or $19 == '' or $19 == 'null' or $19 == 'UNKNOWN') ? 'User Profile - NA' : 'User Profile - Available') as userProfile,
																(($3 is null or $3 == '' or $3 == 'UNKNOWN' or $3 == 'null' or $3 == 'NA') ? 'Unclassified' : (($10 == '0' or $10 == '1' or $10 == '2' or $10 == '3' or $10 == '4' or $10 == '5' or $10 == '6' or $10 == '7' or $10 == '8' or $10 == '9' or $10 == 'A' or $10 == 'B' or $10 == 'C' or $10 == 'D' or $10 == 'E' or $10 == 'F' or $10 == 'G' or $10 == 'H' or $10 == 'I') ? 'Predictive TG' : (($10 == 'J' or $10 == 'K' or $10 == 'L' or $10 == 'M' or $10 == 'N') ? 'Predictive CG' : (($10 == 'O' or $10 == 'P' or $10 == 'Q' or $10 == 'R' or $10 == 'S' or $10 == 'T' or $10 == 'U' or $10 == 'V' or $10 == 'W' or $10 == 'X' or $10 == 'Y' or $10 == 'Z' or $10 == 'a' or $10 == 'b' or $10 == 'c' or $10 == 'd' or $10 == 'e' or $10 == 'f' or $10 == 'g' or $10 == 'h' or $10 == 'i' or $10 == 'j' or $10 == 'k' or $10 == 'l') ? 'BAU Chat' : (($10 == 'm' or $10 == 'n' or $10 == 'o' or $10 == 'p' or $10 == 'q' or $10 == 'r' or $10 == 's') ? 'Future Testing' : (($10 == 't' or $10 == 'u' or $10 == 'v' or $10 == 'w' or $10 == 'x' or $10 == 'y' or $10 == 'z' or $10 == '+' or $10 == '/') ? 'No Chat Offer' : '')))))) as ChatIndicator;


pageLoadWithUserProfileDistinct = DISTINCT pageLoadWithUserProfile_01;

--tmp = LIMIT pageLoadWithUserProfileDistinct 10;
--DUMP tmp;

pageLoadWithUserProfile_Count = FOREACH (GROUP pageLoadWithUserProfileDistinct BY (date, prID1, AccountType, CollectionStatus, pllanguage, hotLead, ruleID, userProfile, ChatIndicator)) GENERATE group, COUNT_STAR(pageLoadWithUserProfileDistinct); 
DUMP pageLoadWithUserProfile_Count;

--------------------------------------------
-------- Processing for SPEECH MAP----------
--------------------------------------------
userProfile_Preprocess = FOREACH pageLoadWithUserProfileDistinct GENERATE csidNprID, user_profile;

userProfile_Filtered = FILTER userProfile_Preprocess BY (user_profile !='' and user_profile is not null);

userProfile_Extracted = FOREACH userProfile_Filtered GENERATE csidNprID, fn_acc_id.extract_account_ids($0,$1) as bsid_acc_ids;

bsid_account_ids = FOREACH userProfile_Extracted GENERATE $0 as csidNprID;
bsid_account_ids_01 = DISTINCT bsid_account_ids;

userProfile_Extracted_01 = FOREACH userProfile_Extracted GENERATE FLATTEN($1);
userProfile_Extracted_01 = DISTINCT userProfile_Extracted_01;
userProfile_Extracted_02 = FOREACH userProfile_Extracted_01 GENERATE $0 as csidNprID, $1 as accound_id;

--tmp = LIMIT userProfile_Extracted_02 10;
--DUMP tmp;

UserProfileWithAccID = JOIN 
								pageLoadWithUserProfileDistinct BY $0 LEFT OUTER,
								userProfile_Extracted_02 BY $0;

UserProfileWithAccIDDistinct = DISTINCT UserProfileWithAccID;

--tmp = LIMIT UserProfileWithAccIDDistinct 10;
--DUMP tmp;

UserProfileWithAccIDDistinct_01 = FOREACH UserProfileWithAccIDDistinct GENERATE 
																			$0 as csidNprID,
																			$1 as date,
																			$2 as pltime,
																			$3 as csid,
																			$4 as prID,
																			$5 as csidcount,
																			$6 as pllanguage,
																			$7 as hltime,
																			$8 as hlcsidcount,
																			$9 as hlpageURL,
																			$10 as ruleID,
																			$11 as prID1,
																			$12 as accountDetails,
																			$13 as AccountType,
																			$14 as collCatCode,
																			$15 as CollectionStatus,
																			$16 as wclanguage,
																			$17 as user_profile,
																			$18 as hotLead,
																			$19 as userProfile,
																			$20 as ChatIndicator,
																			$22 as accound_id;


tmp = LIMIT UserProfileWithAccIDDistinct_01 10;
DUMP tmp;

--userProfile_Filtered = FILTER UserProfileWithAccIDDistinct_01 BY (accound_id !='' and accound_id is not null);
--tmp = LIMIT userProfile_Filtered 10;
--DUMP tmp;


--------------------------------------------
---------   Loading SPEECH Logs  -----------
--------------------------------------------
startFilter = FILTER sdata BY specificEventType == 'SpeechPlatformCallStartEvent';
startInfo = FOREACH startFilter GENERATE header.timeEpochMillisUTC as startTime,header.channelSessionId as uuid, body#'ani',body#'dnis';

appLogFilter = FILTER sdata BY specificEventType == 'SpeechPlatformAppLogEvent';
appLogInfo = FOREACH appLogFilter GENERATE header.timeEpochMillisUTC as time,header.channelSessionId as uuid, body#'label' as logtag, body#'optMessage' as message;

custIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.customerid';
acctIdFilter = FILTER appLogInfo BY logtag == 'com.tellme.proprietary.capitalone.card.customerinfo.accountid';


infoJoin = JOIN 
				startInfo BY uuid, 
				custIdFilter BY uuid, 
				acctIdFilter BY uuid;


speechInfo = FOREACH infoJoin GENERATE $0 as startTime,$1 as uuid,$2 as ani, $3 as dnis,$7 as custId,$11 as acctId;
speechInfoDistinct = DISTINCT speechInfo;

speechInfoAccountIdJoin = JOIN 
								UserProfileWithAccIDDistinct_01 BY $21, 
								speechInfo BY acctId;

tmp = LIMIT speechInfoAccountIdJoin 10;
DUMP tmp;


--------------------------------------------
------- Applying DISPOSITION Filter---------
--------------------------------------------

speechInfoAccountIdJoin_uuid = FOREACH speechInfoAccountIdJoin GENERATE $23 as uuid;
speechInfoAccountIdJoin_uuid01 = FILTER speechInfoAccountIdJoin_uuid BY (uuid !='' and uuid is not null);
speechInfoAccountIdJoin_uuid02 = DISTINCT speechInfoAccountIdJoin_uuid01;

dispFilter = FILTER appLogFilter BY header.eventType == 'SpeechPlatformEvent';
dispFilter2 = FOREACH dispFilter GENERATE 
										header.channelSessionId AS uuid, 
										header.timeEpochMillisUTC as event_time, 
										header.optSequence as sequence, 
										body#'label' as logtag, 
										body#'optMessage' as message;

speechAccountDispJoin = JOIN 
							dispFilter2 BY $0,
							speechInfoAccountIdJoin_uuid02 BY $0;

speechAccountDispJoin_01 = FILTER speechAccountDispJoin BY ($5 != '' and $5 is not null); 

speechAccountDispJoinDistinct = DISTINCT speechAccountDispJoin_01;

speechAccountDispGroup = GROUP speechAccountDispJoinDistinct BY $0;

speechAccountDispGroupSorted = FOREACH speechAccountDispGroup {
																by_seq = ORDER speechAccountDispJoinDistinct BY sequence ASC;
																outval = extract.get_intent_from_applog(by_seq);
																generate group as uuid_intent, outval.intent, outval.disposition,outval.logtype;
																--generate group as uuid_intent, outval.disposition;
};


--tmp = LIMIT speechAccountDispGroupSorted 10;
--DUMP tmp;

speechAccountAgentTransfer = FILTER speechAccountDispGroupSorted BY $2 == 'agenttransfer';
--speechAccountAgentTransfer = FOREACH speechAccountAgentTransfer GENERATE $0;
speechAccountAgentTransfer = DISTINCT speechAccountAgentTransfer;

tmp = LIMIT speechAccountAgentTransfer 10;
DUMP tmp;

--------------------------------------------
--------     Merging Web & IVR     ---------
--------------------------------------------
--speechInfoPageLoadJoin = JOIN 
--								pageLoadAccountJoinDistinct BY $0, 
--								speechInfoAccountIdJoin BY $0;

speechInfoAccountIdJoin2 = JOIN 
								speechInfoAccountIdJoin BY $23, 
								speechAccountAgentTransfer BY $0;

speechInfoAccountIdJoin3 = JOIN
								speechInfoAccountIdJoin2 BY $0 LEFT OUTER,
								pageLoadInfo_Last BY $0;

tmp = LIMIT speechInfoAccountIdJoin3 10;
DUMP tmp;

--speechInfoPageLoadJoin = JOIN 
--							pageLoadAccountJoinDistinct BY $0, 
--							speechInfoAccountIdJoin2 BY $0;
--
--webSpeechInfo = FOREACH speechInfoPageLoadJoin GENERATE $0 as web_csid, $2 as webPages, $4 as w_acctid, $9 as w_cid, $5 as speechStartTime, $6 as speech_uuid;
--webSpeechInfo2 = FOREACH webSpeechInfo GENERATE web_csid, web2ivr_udf.get_web2ivr_details(webPages, speechStartTime) as web2ivr_map, w_acctid, w_cid, speechStartTime, speech_uuid;
--webSpeechInfo3 = FOREACH  webSpeechInfo2 GENERATE 
--												web_csid, 
--												web2ivr_map#'max_page_num' as max_page_num, 
--												web2ivr_map#'last_pg_url' as last_pg_url, 
--												web2ivr_map#'last_pg_time' as last_pg_time, 
--												web2ivr_map#'last_pg_queue' as last_pg_queue,  
--												w_acctid, 
--												w_cid, 
--												speechStartTime, 
--												speech_uuid;
--
--
--webSpeechInfo3 = FILTER webSpeechInfo3 BY ($2 is not null and TRIM($2) != '' );

--------------------------------------------
--------     Removing FP BSIDs     ---------
--------------------------------------------
webSpeechInfo = FOREACH speechInfoAccountIdJoin3 GENERATE
														$0 as csidNprID,
														$1 as date,
														$2 as pltime,
														$3 as csid,
														$4 as prID,
														$5 as csidcount,
														$6 as pllanguage,
														$7 as hltime,
														$8 as hlpageURL,
														$9 as hlcsidcount,
														$10 as ruleID,
														$11 as prID1,
														$12 as accountDetails,
														$13 as AccountType,
														$14 as collCatCode,
														$15 as CollectionStatus,
														$16 as wclanguage,
														$17 as user_profile,
														$18 as hotLead,
														$19 as userProfile,
														$20 as ChatIndicator,
														$21 as accound_id,
														$22 as SpeachStartTime,
														$23 as SpeechUUID,
														$24 as ani,
														$25 as dnis,
														$26 as custId,
														$27 as acctId,
														(($28 is null or $28 == '' or $28 == 'null' or $28 == 'UNKNOWN' or $28 == 'NA') ? 'No Agent Transfer' : 'Agent Transfer') as agentTransferIndicator,
														$28 as uuid_intent,
														$29 as intent,
														$30 as disposition,
														$31 as logtype,
														$33 as lastPageLoadTime,
														$36 as lastPageURL;

tmp = LIMIT webSpeechInfo 10;
DUMP tmp;



webSpeechInfo3_fmt = FOREACH webSpeechInfo GENERATE *, (SpeachStartTime-lastPageLoadTime)/1000 as time_diff_sec;


webSpeechInfoCount = FOREACH (GROUP webSpeechInfo3_fmt BY csidNprID) GENERATE group, COUNT_STAR(webSpeechInfo3_fmt);
webSpeechInfoCount_01 = FOREACH webSpeechInfoCount GENERATE $1 as count;
webSpeechInfoCount_02 = DISTINCT webSpeechInfoCount_01;
DUMP webSpeechInfoCount_02;

/*
webSpeechInfo3_fmt2 = FOREACH (GROUP webSpeechInfo3_fmt BY csidNprID) {
                                                                       sort_data = ORDER webSpeechInfo3_fmt BY csidNprID, time_diff_sec;
                                                                       first = LIMIT sort_data 1;
                                                                       GENERATE FLATTEN(first);
                                                                     };
*/

webSpeechInfo3_fmt1 = FILTER webSpeechInfo3_fmt BY time_diff_sec > 0;



webSpeechInfo3_fmt3 = FOREACH (GROUP webSpeechInfo3_fmt1 BY SpeechUUID) {
                                                                           sort_data = ORDER webSpeechInfo3_fmt1 BY SpeechUUID, time_diff_sec;
                                                                           first = LIMIT sort_data 1;
                                                                           GENERATE FLATTEN(first);
                                                                         };


/*
--------------------------------------------
--------Loading Interaction ID Data---------
--------------------------------------------
screenLoadEventFilter = FILTER wdata BY specificEventType == 'OnlineScreenLoadEvent';

screenLoadIntID = FILTER screenLoadEventFilter BY (header.optSharedSessionId is not null and header.optSharedSessionId !='' and header.optSharedSessionId !='null' and header.optSharedSessionId !='UNKNOWN');

screenLoadIntID_01 = FOREACH screenLoadIntID GENERATE 
													specificEventType as eventName,
													header.timeEpochMillisUTC as time,
													header.channelSessionId as bsidNInviteID,
													SUBSTRING(header.channelSessionId, 0, INDEXOF(header.channelSessionId, '@',0)) as bsid,
													header.optSharedSessionId as IntId,
													body#'screenId' as screenType,
													body#'screenTitle' as screenName,
													custom#'RuleId' as ruleID,
													custom#'Queue' as Queue; 

screenLoadIntID_Ordered = ORDER screenLoadIntID_01 BY $3, $1 ASC;

screenLoadIntIDGroup = GROUP screenLoadIntID_Ordered BY $4;

screenLoadIntIDSorted = FOREACH screenLoadIntIDGroup {
													distinctData = DISTINCT screenLoadIntID_Ordered;
													sortData = ORDER distinctData BY $1 ASC;
													first = LIMIT sortData 1;
													GENERATE group as InteractionID, FLATTEN(first);
};

screenLoadIntIDSorted_01 = FOREACH screenLoadIntIDSorted GENERATE 
																$0 as IntId,
																$1 as eventName,
																SUBSTRING(ToString(ToDate((long)$2)),0,INDEXOF(ToString(ToDate((long)$2)), '.', 1)) as time,
																$4 as csid,
																$6 as screenType,
																$7 as screenName,
																$8 as ruleID,
																$9 as Queue;

tmp = LIMIT screenLoadIntIDSorted_01 10;
DUMP tmp;


--------------------------------------------
--------    Storing Data to BDP    ---------
--------------------------------------------

STORE screenLoadIntIDSorted_01 INTO 'cap1_ease_InteractionID_CSID_Mapping_28mar_09Apr_04122018.tsv';
*/
STORE webSpeechInfo3_fmt3 INTO 'cap1_ease_we2ivr_agent_transfer_30May_05312018.tsv';




