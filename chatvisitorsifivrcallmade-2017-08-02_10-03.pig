REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar';
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

%DECLARE YEAR '2017'
%DECLARE MONTH '{07}'
%DECLARE DAYS '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15}'

input_file = load 'hdfs:///user/vkumar/Chat_BSID.csv' USING PigStorage(',') as (bsid:chararray);

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');
data = LOAD '/raw/prod/rtdp/idm/events/capitalone/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

customEventFilter = FILTER data BY specificEventType == 'WebCustomEvent' and body#'customEventType' == '100010';
accountInfo = FOREACH customEventFilter GENERATE header.channelSessionId as bsid, custom#'accid' as w_acctid, custom#'ci' as w_cid;

-- JOIN applog_table BY $0, input_file BY $0;

res = JOIN input_file BY $0 LEFT OUTER, accountInfo BY $0;
res = DISTINCT res;

STORE res INTO 'BSID_AccountIDMapping.tsv';