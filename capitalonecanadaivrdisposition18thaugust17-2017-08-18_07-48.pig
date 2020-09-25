REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar';
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

REGISTER 'hdfs:///user/jjonnada/extract_intent.py' using org.apache.pig.scripting.jython.JythonScriptEngine as extract;

%DECLARE YEAR '2017'
%DECLARE MONTH '{07}'
-- %DECLARE DAYS '{15}'
%DECLARE DAYS '{01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}'

input_file = load 'hdfs:///user/vkumar/capone_omni_channel_01July2017-31July2017_1.csv' USING PigStorage(',') as (csid:chararray, uuid:chararray);

/* Intent (from applog, only available for 40% of the cases)*/
DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

ivr_log_data = LOAD '/raw/prod/rtdp/idm/events/cap1/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

applog_data = FILTER ivr_log_data BY header.eventType == 'SpeechPlatformEvent' AND specificEventType == 'SpeechPlatformAppLogEvent';

applog_table = FOREACH applog_data GENERATE
header.channelSessionId AS uuid,
header.timeEpochMillisUTC as event_time,
header.optSequence as sequence,
body#'label' as logtag, 
body#'optMessage' as message;
applog_group = GROUP applog_table BY uuid;
applog_group_sorted = FOREACH applog_group {
                by_seq = ORDER applog_table BY sequence ASC;
    outval = extract.get_intent_from_applog(by_seq);
    generate group as uuid_intent, outval.intent, outval.disposition,outval.logtype;
}

final_recs = JOIN applog_group_sorted BY $2, input_file BY $1;
final_recs = DISTINCT final_recs;

STORE final_recs INTO 'capone_ivr_disposition_July17.tsv';
