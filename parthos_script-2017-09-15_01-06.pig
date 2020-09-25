REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;
REGISTER 'hdfs:///user/preddy/CapOne/queue_UDF.py' using jython as func;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

rawdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=01/*/*/*/*.avro' using LOAD_IDM;
--startFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformCallStartEvent';
appLogFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformAppLogEvent';
-- ## Get the CustId wise count of occurences ##
labelData = FILTER appLogFilter BY body#'label' matches 'com.tellme.proprietary.capitalone.card.customerinfo.customerid' ;
distinctCustid = foreach labelData  Generate body#'optMessage' as custID, specificEventType;
groupCustid = Group distinctCustid by custID;
countCustid = foreach groupCustid Generate group, COUNT(distinctCustid.specificEventType);
STORE countCustid INTO 'IVR_Custid_connects_Jan2017';

rawdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=02/*/*/*/*.avro' using LOAD_IDM;
--startFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformCallStartEvent';
appLogFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformAppLogEvent';
-- ## Get the CustId wise count of occurences ##
labelData = FILTER appLogFilter BY body#'label' matches 'com.tellme.proprietary.capitalone.card.customerinfo.customerid' ;
distinctCustid = foreach labelData  Generate body#'optMessage' as custID, specificEventType;
groupCustid = Group distinctCustid by custID;
countCustid = foreach groupCustid Generate group, COUNT(distinctCustid.specificEventType);
STORE countCustid INTO 'IVR_Custid_connects_Feb2017';

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');
rawdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=03/*/*/*/*.avro' using LOAD_IDM;
--startFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformCallStartEvent';
appLogFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformAppLogEvent';
-- ## Get the CustId wise count of occurences ##
labelData = FILTER appLogFilter BY body#'label' matches 'com.tellme.proprietary.capitalone.card.customerinfo.customerid' ;
distinctCustid = foreach labelData  Generate body#'optMessage' as custID, specificEventType;
groupCustid = Group distinctCustid by custID;
countCustid = foreach groupCustid Generate group, COUNT(distinctCustid.specificEventType);
STORE countCustid INTO 'IVR_Custid_connects_Mar2017';

rawdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=04/*/*/*/*.avro' using LOAD_IDM;
--startFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformCallStartEvent';
appLogFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformAppLogEvent';
-- ## Get the CustId wise count of occurences ##
labelData = FILTER appLogFilter BY body#'label' matches 'com.tellme.proprietary.capitalone.card.customerinfo.customerid' ;
distinctCustid = foreach labelData  Generate body#'optMessage' as custID, specificEventType;
groupCustid = Group distinctCustid by custID;
countCustid = foreach groupCustid Generate group, COUNT(distinctCustid.specificEventType);
STORE countCustid INTO 'IVR_Custid_connects_Apr2017';

rawdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=05/*/*/*/*.avro' using LOAD_IDM;
--startFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformCallStartEvent';
appLogFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformAppLogEvent';
-- ## Get the CustId wise count of occurences ##
labelData = FILTER appLogFilter BY body#'label' matches 'com.tellme.proprietary.capitalone.card.customerinfo.customerid' ;
distinctCustid = foreach labelData  Generate body#'optMessage' as custID, specificEventType;
groupCustid = Group distinctCustid by custID;
countCustid = foreach groupCustid Generate group, COUNT(distinctCustid.specificEventType);
STORE countCustid INTO 'IVR_Custid_connects_May2017';

rawdata = LOAD '/raw/prod/rtdp/idm/events/cap1/year=2017/month=06/*/*/*/*.avro' using LOAD_IDM;
--startFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformCallStartEvent';
appLogFilter = FILTER rawdata BY specificEventType == 'SpeechPlatformAppLogEvent';
-- ## Get the CustId wise count of occurences ##
labelData = FILTER appLogFilter BY body#'label' matches 'com.tellme.proprietary.capitalone.card.customerinfo.customerid' ;
distinctCustid = foreach labelData  Generate body#'optMessage' as custID, specificEventType;
groupCustid = Group distinctCustid by custID;
countCustid = foreach groupCustid Generate group, COUNT(distinctCustid.specificEventType);

STORE countCustid INTO 'IVR_Custid_connects_Jun2017';

-- Dump countCustid;
--Explain data
--Illustrate data2;

