REGISTER 'hadoop-lzo-0.4.3.jar';
REGISTER elephant-bird-pig-3.0.9.jar;
REGISTER 'hadoop-lzo-cdh4-0.4.15-gplextras.jar';
-- REGISTER the Jar
REGISTER nestedJsonParser.jar;
REGISTER piggybank.jar;
REGISTER datafu-1.2.0.jar;

-- Define the Custom Parser Method
DEFINE myJsonParser com.CustomParser();

DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
raw_data = LOAD '/raw/prod/searshs/pxoe/merged/year=2015/month=09/day={04,05,06}/*/*' using SequenceFileLoader as (eventHour:chararray, event:chararray);
--

-- simpleEventString is comma separated string containing values from json string (V1,V2,V3....,Vn). Code Can be changed to return different format after parsing.
parsedtoFileds = FOREACH raw_data GENERATE eventHour, myJsonParser(event) AS simpleEventString;
splittedFields = FOREACH parsedtoFileds GENERATE FLATTEN(STRSPLIT(simpleEventString,','));

STORE splittedFields INTO 'parsedSplittedFields';
