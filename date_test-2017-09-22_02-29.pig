variable8DispJoin = LOAD '/user/vkumar/variable8DispJoin' 
as ( time: long, uuid: chararray, logtag: bytearray, value: bytearray, 
time_1: long, uuid_1: chararray, logtag_1: bytearray, value_1: bytearray);

limit_data = LIMIT variable8DispJoin 4;
DUMP limit_data;

--variable8DispJoin_1 = FOREACH variable8DispJoin GENERATE GetDay(ToDate((long)time)) as time_1;
variable8DispJoin_1 = FOREACH variable8DispJoin GENERATE ToDate((long)time) as time_1;
limit_data_1 = LIMIT variable8DispJoin_1 4;
DUMP limit_data_1;

variable8DispJoin_2 = FOREACH variable8DispJoin GENERATE ToString(ToDate((long)time)) as time_1: chararray;
limit_data_2 = LIMIT variable8DispJoin_2 4;
DUMP limit_data_2;


variable8DispJoin_3 = 
FOREACH variable8DispJoin 
GENERATE SUBSTRING(ToString(ToDate((long)time)),1,INDEXOF(ToString(ToDate((long)time)), 'T', 1)) as time_1;
limit_data_3 = LIMIT variable8DispJoin_3 4;
DUMP limit_data_3;


--as ( id:int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray );