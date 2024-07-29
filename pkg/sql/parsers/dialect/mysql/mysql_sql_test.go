// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	debugSQL = struct {
		input  string
		output string
	}{
		//input:  "upgrade account all with retry 10",
		//output: "upgrade account all with retry 10",
		//input:  "upgrade account all",
		//output: "upgrade account all",
		//input:  "upgrade account 'acc1' with retry 5",
		//output: "upgrade account acc1 with retry 5",
		//input:  "upgrade account 'acc1'",
		//output: "upgrade account acc1",
		input:  "show upgrade",
		output: "show upgrade",
	}
)

func TestDebug(t *testing.T) {
	if debugSQL.output == "" {
		debugSQL.output = debugSQL.input
	}
	ast, err := ParseOne(context.TODO(), debugSQL.input, 1)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", debugSQL.input, err)
		return
	}
	out := tree.String(ast, dialect.MYSQL)
	if debugSQL.output != out {
		t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", debugSQL.output, out)
	}
}

var (
	orginSQL = struct {
		input  string
		output string
	}{
		input:  "select A from t1",
		output: "select A from t1",
	}
)

// character set latin1 NOT NULL default
func TestOriginSQL(t *testing.T) {
	if orginSQL.output == "" {
		orginSQL.output = orginSQL.input
	}
	ast, err := ParseOne(context.TODO(), orginSQL.input, 0)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", orginSQL.input, err)
		return
	}
	out := tree.String(ast, dialect.MYSQL)
	if orginSQL.output != out {
		t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", orginSQL.output, out)
	}
}

var (
	validSQL = []struct {
		input  string
		output string
	}{{
		input:  "create account 0b6d35cc_11ab_4da5_a5c5_c4c09917c11 admin_name='admin' identified by '123456';",
		output: "create account 0b6d35cc_11ab_4da5_a5c5_c4c09917c11 admin_name 'admin' identified by '******'",
	}, {
		input:  "select enable from t1;",
		output: "select enable from t1",
	}, {
		input:  "select _wstart(ts), _wend(ts), max(temperature), min(temperature) from sensor_data where ts > \"2023-08-01 00:00:00.000\" and ts < \"2023-08-01 00:50:00.000\" interval(ts, 10, minute) sliding(5, minute) fill(prev);",
		output: "select _wstart(ts), _wend(ts), max(temperature), min(temperature) from sensor_data where ts > 2023-08-01 00:00:00.000 and ts < 2023-08-01 00:50:00.000 interval(ts, 10, minute) sliding(5, minute) fill(prev)",
	}, {
		input:  "select cluster_centers(a) from t1;",
		output: "select cluster_centers(a, 1,vector_l2_ops,random,false) from t1",
	}, {
		input:  "select cluster_centers(a kmeans '5') from t1;",
		output: "select cluster_centers(a, 5) from t1",
	}, {
		input:  "select cluster_centers(a kmeans '5,vector_l2_ops') from t1;",
		output: "select cluster_centers(a, 5,vector_l2_ops) from t1",
	}, {
		input:  "select cluster_centers(a kmeans '5,vector_cosine_ops') from t1;",
		output: "select cluster_centers(a, 5,vector_cosine_ops) from t1",
	}, {
		input:  "select cluster_centers(a kmeans '5,vector_cosine_ops,kmeansplusplus') from t1;",
		output: "select cluster_centers(a, 5,vector_cosine_ops,kmeansplusplus) from t1",
	}, {
		input:  "select cluster_centers(a kmeans '5,vector_cosine_ops,random') from t1;",
		output: "select cluster_centers(a, 5,vector_cosine_ops,random) from t1",
	}, {
		input:  "select cluster_centers(a kmeans '5,vector_cosine_ops,random,true') from t1;",
		output: "select cluster_centers(a, 5,vector_cosine_ops,random,true) from t1",
	}, {
		input:  "alter table t1 alter reindex idx1 IVFFLAT lists = 5",
		output: "alter table t1 alter reindex idx1 ivfflat lists = 5",
	}, {
		input:  "create connector for s with (\"type\"='kafka', \"topic\"= 'user', \"partition\" = '1', \"value\"= 'json', \"bootstrap.servers\" = '127.0.0.1:62610');",
		output: "create connector for s with (type = kafka, topic = user, partition = 1, value = json, bootstrap.servers = 127.0.0.1:62610)",
	}, {
		input:  "select _wstart(ts), _wend(ts), max(temperature), min(temperature) from sensor_data where ts > \"2023-08-01 00:00:00.000\" and ts < \"2023-08-01 00:50:00.000\" interval(ts, 10, minute) sliding(5, minute) fill(prev);",
		output: "select _wstart(ts), _wend(ts), max(temperature), min(temperature) from sensor_data where ts > 2023-08-01 00:00:00.000 and ts < 2023-08-01 00:50:00.000 interval(ts, 10, minute) sliding(5, minute) fill(prev)",
	}, {
		input:  "create connector for s with (\"type\"='kafkamo', \"topic\"= 'user', \"partion\" = '1', \"value\"= 'json', \"bootstrap.servers\" = '127.0.0.1:62610');",
		output: "create connector for s with (type = kafkamo, topic = user, partion = 1, value = json, bootstrap.servers = 127.0.0.1:62610)",
	}, {
		input:  "create source s(a varchar, b varchar) with (\"type\"='kafka', \"topic\"= 'user', \"partion\" = '1', \"value\"= 'json', \"bootstrap.servers\" = '127.0.0.1:62610');",
		output: "create source s (a varchar, b varchar) with (type = kafka, topic = user, partion = 1, value = json, bootstrap.servers = 127.0.0.1:62610)",
	}, {
		input:  "drop source if exists s",
		output: "drop table if exists s",
	}, {
		input:  "CREATE source pageviews (\n    page_id BIGINT KEY\n  ) WITH (\n    KAFKA_TOPIC = 'keyed-pageviews-topic',\n    VALUE_FORMAT = 'JSON_SR',\n    VALUE_SCHEMA_ID = 2\n  );",
		output: "create source pageviews (page_id bigint key) with (kafka_topic = keyed-pageviews-topic, value_format = JSON_SR, value_schema_id = 2)",
	}, {
		input:  "CREATE source pageviews (\n    viewtime BIGINT,\n    user_id VARCHAR\n  ) WITH (\n    KAFKA_TOPIC = 'keyless-pageviews-topic',\n    KEY_FORMAT = 'AVRO',\n    KEY_SCHEMA_ID = 1,\n    VALUE_FORMAT = 'JSON_SR'\n  );",
		output: "create source pageviews (viewtime bigint, user_id varchar) with (kafka_topic = keyless-pageviews-topic, key_format = AVRO, key_schema_id = 1, value_format = JSON_SR)",
	}, {
		input:  "CREATE source pageviews (page_id BIGINT, viewtime BIGINT, user_id VARCHAR) WITH (\n    KAFKA_TOPIC = 'keyless-pageviews-topic',\n    VALUE_FORMAT = 'JSON'\n  )",
		output: "create source pageviews (page_id bigint, viewtime bigint, user_id varchar) with (kafka_topic = keyless-pageviews-topic, value_format = JSON)",
	}, {
		input:  "select row_number() over (partition by col1, col2 order by col3 desc range unbounded preceding) from t1",
		output: "select row_number() over (partition by col1, col2 order by col3 desc range unbounded preceding) from t1",
	}, {
		input:  "select dense_rank() over (partition by col1, col2 order by col3 desc range unbounded preceding) from t1",
		output: "select dense_rank() over (partition by col1, col2 order by col3 desc range unbounded preceding) from t1",
	}, {
		input:  "select day_key,day_date,day,month,quarter,year,week,day_of_week from bi_date where 1=2;",
		output: "select day_key, day_date, day, month, quarter, year, week, day_of_week from bi_date where 1 = 2",
	}, {
		input:  "select sum(a) over(partition by a range between interval 1 day preceding and interval 2 day following) from t1",
		output: "select sum(a) over (partition by a range between interval(1, day) preceding and interval(2, day) following) from t1",
	}, {
		input:  "select rank() over(partition by a range between 1 preceding and current row) from t1",
		output: "select rank() over (partition by a range between 1 preceding and current row) from t1",
	}, {
		input:  "select rank() over(partition by a) from t1",
		output: "select rank() over (partition by a) from t1",
	}, {
		input:  "select rank() over(partition by a order by b desc) from t1",
		output: "select rank() over (partition by a order by b desc) from t1",
	}, {
		input:  "load data url s3option {\"bucket\"='dan-test1', \"filepath\"='ex_table_dan_gzip.gz',\"role_arn\"='arn:aws:iam::468413122987:role/dev-cross-s3', \"external_id\"='5404f91c_4e59_4898_85b3', \"compression\"='auto'} into table hx3.t2 fields terminated by ',' enclosed by '\\\"' lines terminated by '\\n';\n",
		output: "load data url s3option {'bucket'='dan-test1', 'filepath'='ex_table_dan_gzip.gz', 'role_arn'='arn:aws:iam::468413122987:role/dev-cross-s3', 'external_id'='5404f91c_4e59_4898_85b3', 'compression'='auto'} into table hx3.t2 fields terminated by , enclosed by \" lines terminated by \n",
	}, {
		input:  "load data url stageoption my_stage into table hx3.t2 fields terminated by ',' enclosed by '' lines terminated by '\\n';\n",
		output: "load data url from stage my_stage into table hx3.t2 fields terminated by , lines terminated by \n",
	}, {
		input:  "SHOW CREATE TABLE information_schema.PROCESSLIST;",
		output: "show create table information_schema.processlist",
	}, {
		input:  "create table t1 (a int comment '\"123123\\'')",
		output: "create table t1 (a int comment \"123123'')",
	}, {
		input:  "select * from t1 where a not ilike '%a'",
		output: "select * from t1 where a not ilike %a",
	}, {
		input:  "select * from t1 where a ilike '%a'",
		output: "select * from t1 where a ilike %a",
	}, {
		input:  "select * from result_scan(query_id)",
		output: "select * from result_scan(query_id)",
	}, {
		input:  "select * from meta_scan('query_id');",
		output: "select * from meta_scan(query_id)",
	}, {
		input:  "show variables like 'sql_mode'",
		output: "show variables like sql_mode",
	}, {
		input:  "show index from t1 from db",
		output: "show index from t1 from db",
	}, {
		input:  "create table t2(a int, b datalink);",
		output: "create table t2 (a int, b datalink)",
	}, {
		input:  "select * from (SELECT * FROM (SELECT 1, 2, 3)) AS t1",
		output: "select * from (select * from (select 1, 2, 3)) as t1",
	}, {
		input:  "SELECT count(*) AS low_stock FROM (\nSELECT s_w_id, s_i_id, s_quantity\nFROM bmsql_stock\nWHERE s_w_id = 1 AND s_quantity < 1000 AND s_i_id IN (\nSELECT ol_i_id\nFROM bmsql_district\nJOIN bmsql_order_line ON ol_w_id = d_w_id\nAND ol_d_id = d_id\nAND ol_o_id >= d_next_o_id - 20\nAND ol_o_id < d_next_o_id\nWHERE d_w_id = 1 AND d_id = 1\n)\n);",
		output: "select count(*) as low_stock from (select s_w_id, s_i_id, s_quantity from bmsql_stock where s_w_id = 1 and s_quantity < 1000 and s_i_id in (select ol_i_id from bmsql_district inner join bmsql_order_line on ol_w_id = d_w_id and ol_d_id = d_id and ol_o_id >= d_next_o_id - 20 and ol_o_id < d_next_o_id where d_w_id = 1 and d_id = 1))",
	}, {
		input:  "create account `abc@124` admin_name `abc@124` identified by '111'",
		output: "create account abc@124 admin_name 'abc@124' identified by '******'",
	}, {
		input:  "create account account ADMIN_NAME 'root' IDENTIFIED BY '123456';",
		output: "create account account admin_name 'root' identified by '******'",
	}, {
		input: "drop table if exists history",
	}, {
		input:  "create user daisy@192.168.1.10 identified by '123456'",
		output: "create user daisy@192.168.1.10 identified by '******'",
	}, {
		input: "create table t0 (a float(255, 3))",
	}, {
		input:  "SELECT  id,name,view_type,attribute,attribute_filed,size,created_at,updated_at  FROM view_warehouse limit 0,10",
		output: "select id, name, view_type, attribute, attribute_filed, size, created_at, updated_at from view_warehouse limit 10 offset 0",
	}, {
		input:  "select algo_alarm_record.* from algo_alarm_record inner join (SELECT id FROM algo_alarm_record use index(algo_alarm_record_algo_id_first_id_created_at_index) WHERE first_id = 0 AND created_at >= '2022-09-18 00:00:00' AND created_at <= '2022-10-18 00:00:00' and algo_id not in (9808,9809) order by id desc limit 0,10 ) e on e.id = algo_alarm_record.id order by algo_alarm_record.id desc;",
		output: "select algo_alarm_record.* from algo_alarm_record inner join (select id from algo_alarm_record use index(algo_alarm_record_algo_id_first_id_created_at_index) where first_id = 0 and created_at >= 2022-09-18 00:00:00 and created_at <= 2022-10-18 00:00:00 and algo_id not in (9808, 9809) order by id desc limit 10 offset 0) as e on e.id = algo_alarm_record.id order by algo_alarm_record.id desc",
	}, {
		input:  "SELECT * FROM kv WHERE k = 1 FOR UPDATE",
		output: "select * from kv where k = 1 for update",
	}, {
		input: "select a from t1 use index(b)",
	}, {
		input:  "SELECT   id,cid,status,ip,streams   FROM camera     WHERE (cid_type = ?)",
		output: "select id, cid, status, ip, streams from camera where (cid_type = ?)",
	}, {
		input:  "CREATE  \nVIEW `xab0100` AS (\n  select `a`.`SYSUSERID` AS `sysuserid`,`a`.`USERID` AS `userid`,`a`.`USERNAME` AS `usernm`,`a`.`PWDHASH` AS `userpwd`,`a`.`USERTYPE` AS `usertype`,`a`.`EMPID` AS `empid`,`a`.`EMAIL` AS `email`,`a`.`TELO` AS `telo`,`a`.`TELH` AS `telh`,`a`.`MOBIL` AS `mobil`,(case `a`.`ACTIVED` when '1' then 'N' when '2' then 'Y' else 'Y' end) AS `useyn`,`a`.`ENABLEPWD` AS `enablepwd`,`a`.`ENABLEMMSG` AS `enablemmsg`,`a`.`FEECENTER` AS `feecenter`,left(concat(ifnull(`c`.`ORGID`,''),'|'),(char_length(concat(ifnull(`c`.`ORGID`,''),'|')) - 1)) AS `orgid`,left(concat(ifnull(`c`.`ORGNAME`,''),'|'),(char_length(concat(ifnull(`c`.`ORGNAME`,''),'|')) - 1)) AS `orgname`,ifnull(`a`.`ISPLANNER`,'') AS `isplanner`,ifnull(`a`.`ISWHEMPLOYEE`,'') AS `iswhemployee`,ifnull(`a`.`ISBUYER`,'') AS `isbuyer`,ifnull(`a`.`ISQCEMPLOYEE`,'') AS `isqceemployee`,ifnull(`a`.`ISSALEEMPLOYEE`,'') AS `issaleemployee`,`a`.`SEX` AS `sex`,ifnull(`c`.`ENTID`,'3') AS `ORGANIZATION_ID`,ifnull(`a`.`NOTICEUSER`,'') AS `NOTICEUSER` \n  from ((`kaf_cpcuser` `a` left join `kaf_cpcorguser` `b` on((`a`.`SYSUSERID` = `b`.`SYSUSERID`))) left join `kaf_cpcorg` `c` on((`b`.`ORGID` = `c`.`ORGID`))) \n  order by `a`.`SYSUSERID`,`a`.`USERID`,`a`.`USERNAME`,`a`.`USERPASS`,`a`.`USERTYPE`,`a`.`EMPID`,`a`.`EMAIL`,`a`.`TELO`,`a`.`TELH`,`a`.`MOBIL`,`a`.`ACTIVED`,`a`.`ENABLEPWD`,`a`.`ENABLEMMSG`,`a`.`FEECENTER`,`a`.`ISPLANNER`,`a`.`ISWHEMPLOYEE`,`a`.`ISBUYER`,`a`.`ISQCEMPLOYEE`,`a`.`ISSALEEMPLOYEE`,`a`.`SEX`,`c`.`ENTID`) ;\n",
		output: "create view xab0100 as (select a.SYSUSERID as sysuserid, a.USERID as userid, a.USERNAME as usernm, a.PWDHASH as userpwd, a.USERTYPE as usertype, a.EMPID as empid, a.EMAIL as email, a.TELO as telo, a.TELH as telh, a.MOBIL as mobil, (case a.ACTIVED when 1 then N when 2 then Y else Y end) as useyn, a.ENABLEPWD as enablepwd, a.ENABLEMMSG as enablemmsg, a.FEECENTER as feecenter, left(concat(ifnull(c.ORGID, ), |), (char_length(concat(ifnull(c.ORGID, ), |)) - 1)) as orgid, left(concat(ifnull(c.ORGNAME, ), |), (char_length(concat(ifnull(c.ORGNAME, ), |)) - 1)) as orgname, ifnull(a.ISPLANNER, ) as isplanner, ifnull(a.ISWHEMPLOYEE, ) as iswhemployee, ifnull(a.ISBUYER, ) as isbuyer, ifnull(a.ISQCEMPLOYEE, ) as isqceemployee, ifnull(a.ISSALEEMPLOYEE, ) as issaleemployee, a.SEX as sex, ifnull(c.ENTID, 3) as ORGANIZATION_ID, ifnull(a.NOTICEUSER, ) as NOTICEUSER from kaf_cpcuser as a left join kaf_cpcorguser as b on ((a.SYSUSERID = b.SYSUSERID)) left join kaf_cpcorg as c on ((b.ORGID = c.ORGID)) order by a.SYSUSERID, a.USERID, a.USERNAME, a.USERPASS, a.USERTYPE, a.EMPID, a.EMAIL, a.TELO, a.TELH, a.MOBIL, a.ACTIVED, a.ENABLEPWD, a.ENABLEMMSG, a.FEECENTER, a.ISPLANNER, a.ISWHEMPLOYEE, a.ISBUYER, a.ISQCEMPLOYEE, a.ISSALEEMPLOYEE, a.SEX, c.ENTID)",
	}, {
		input:  "ALTER  \nVIEW `xab0100` AS (\n  select `a`.`SYSUSERID` AS `sysuserid`,`a`.`USERID` AS `userid`,`a`.`USERNAME` AS `usernm`,`a`.`PWDHASH` AS `userpwd`,`a`.`USERTYPE` AS `usertype`,`a`.`EMPID` AS `empid`,`a`.`EMAIL` AS `email`,`a`.`TELO` AS `telo`,`a`.`TELH` AS `telh`,`a`.`MOBIL` AS `mobil`,(case `a`.`ACTIVED` when '1' then 'N' when '2' then 'Y' else 'Y' end) AS `useyn`,`a`.`ENABLEPWD` AS `enablepwd`,`a`.`ENABLEMMSG` AS `enablemmsg`,`a`.`FEECENTER` AS `feecenter`,left(concat(ifnull(`c`.`ORGID`,''),'|'),(char_length(concat(ifnull(`c`.`ORGID`,''),'|')) - 1)) AS `orgid`,left(concat(ifnull(`c`.`ORGNAME`,''),'|'),(char_length(concat(ifnull(`c`.`ORGNAME`,''),'|')) - 1)) AS `orgname`,ifnull(`a`.`ISPLANNER`,'') AS `isplanner`,ifnull(`a`.`ISWHEMPLOYEE`,'') AS `iswhemployee`,ifnull(`a`.`ISBUYER`,'') AS `isbuyer`,ifnull(`a`.`ISQCEMPLOYEE`,'') AS `isqceemployee`,ifnull(`a`.`ISSALEEMPLOYEE`,'') AS `issaleemployee`,`a`.`SEX` AS `sex`,ifnull(`c`.`ENTID`,'3') AS `ORGANIZATION_ID`,ifnull(`a`.`NOTICEUSER`,'') AS `NOTICEUSER` \n  from ((`kaf_cpcuser` `a` left join `kaf_cpcorguser` `b` on((`a`.`SYSUSERID` = `b`.`SYSUSERID`))) left join `kaf_cpcorg` `c` on((`b`.`ORGID` = `c`.`ORGID`))) \n  order by `a`.`SYSUSERID`,`a`.`USERID`,`a`.`USERNAME`,`a`.`USERPASS`,`a`.`USERTYPE`,`a`.`EMPID`,`a`.`EMAIL`,`a`.`TELO`,`a`.`TELH`,`a`.`MOBIL`,`a`.`ACTIVED`,`a`.`ENABLEPWD`,`a`.`ENABLEMMSG`,`a`.`FEECENTER`,`a`.`ISPLANNER`,`a`.`ISWHEMPLOYEE`,`a`.`ISBUYER`,`a`.`ISQCEMPLOYEE`,`a`.`ISSALEEMPLOYEE`,`a`.`SEX`,`c`.`ENTID`) ;\n",
		output: "alter view xab0100 as (select a.SYSUSERID as sysuserid, a.USERID as userid, a.USERNAME as usernm, a.PWDHASH as userpwd, a.USERTYPE as usertype, a.EMPID as empid, a.EMAIL as email, a.TELO as telo, a.TELH as telh, a.MOBIL as mobil, (case a.ACTIVED when 1 then N when 2 then Y else Y end) as useyn, a.ENABLEPWD as enablepwd, a.ENABLEMMSG as enablemmsg, a.FEECENTER as feecenter, left(concat(ifnull(c.ORGID, ), |), (char_length(concat(ifnull(c.ORGID, ), |)) - 1)) as orgid, left(concat(ifnull(c.ORGNAME, ), |), (char_length(concat(ifnull(c.ORGNAME, ), |)) - 1)) as orgname, ifnull(a.ISPLANNER, ) as isplanner, ifnull(a.ISWHEMPLOYEE, ) as iswhemployee, ifnull(a.ISBUYER, ) as isbuyer, ifnull(a.ISQCEMPLOYEE, ) as isqceemployee, ifnull(a.ISSALEEMPLOYEE, ) as issaleemployee, a.SEX as sex, ifnull(c.ENTID, 3) as ORGANIZATION_ID, ifnull(a.NOTICEUSER, ) as NOTICEUSER from kaf_cpcuser as a left join kaf_cpcorguser as b on ((a.SYSUSERID = b.SYSUSERID)) left join kaf_cpcorg as c on ((b.ORGID = c.ORGID)) order by a.SYSUSERID, a.USERID, a.USERNAME, a.USERPASS, a.USERTYPE, a.EMPID, a.EMAIL, a.TELO, a.TELH, a.MOBIL, a.ACTIVED, a.ENABLEPWD, a.ENABLEMMSG, a.FEECENTER, a.ISPLANNER, a.ISWHEMPLOYEE, a.ISBUYER, a.ISQCEMPLOYEE, a.ISSALEEMPLOYEE, a.SEX, c.ENTID)",
	}, {
		input: "select time from t1 as value",
	}, {
		input:  "alter database test set mysql_compatibility_mode = '{transaction_isolation: REPEATABLE-READ, lower_case_table_names: 0}'",
		output: "alter database configuration for test as {transaction_isolation: REPEATABLE-READ, lower_case_table_names: 0} ",
	}, {
		input: "show profiles",
	}, {
		input:  "CREATE TABLE new_t1 LIKE t1",
		output: "create table new_t1 like t1",
	}, {
		input:  "CREATE TABLE new_t1 LIKE test.t1",
		output: "create table new_t1 like test.t1",
	}, {
		input: "show privileges",
	}, {
		input: "show events from db1",
	}, {
		input: "show collation",
	}, {
		input: "show plugins",
	}, {
		input: "show procedure status",
	}, {
		input: "show triggers from db1 where 1",
	}, {
		input: "show engines",
	}, {
		input: "show config",
	}, {
		input: "show grants",
	}, {
		input:  "show grants for 'test'@'localhost'",
		output: "show grants for test@localhost",
	}, {
		input: "show table status from t1",
	}, {
		input: "show table status from t1",
	}, {
		input: "grant connect on account * to role_r1",
	}, {
		input: "select password from t1",
	}, {
		input:  "create table t1 (a datetime on update CURRENT_TIMESTAMP(1))",
		output: "create table t1 (a datetime on update CURRENT_TIMESTAMP(1))",
	}, {
		input:  `create table table10 (a int primary key, b varchar(10)) checksum=0 COMMENT="asdf"`,
		output: "create table table10 (a int primary key, b varchar(10)) checksum = 0 comment = 'asdf'",
	}, {
		input:  "commit work",
		output: "commit",
	}, {
		input: "select * from tables",
	}, {
		input: "update t1 set a = default",
	}, {
		input:  "truncate t1",
		output: "truncate table t1",
	}, {
		input:  "truncate table t1",
		output: "truncate table t1",
	}, {
		input:  "truncate db1.t1",
		output: "truncate table db1.t1",
	}, {
		input:  "truncate table db1.t1",
		output: "truncate table db1.t1",
	},
		{
			input:  "upgrade account all with retry 10",
			output: "upgrade account all with retry 10",
		}, {
			input:  "upgrade account all",
			output: "upgrade account all",
		}, {
			input:  "upgrade account 'acc1' with retry 5",
			output: "upgrade account acc1 with retry 5",
		}, {
			input:  "upgrade account 'acc1'",
			output: "upgrade account acc1",
		}, {
			input:  "explain select * from emp",
			output: "explain select * from emp",
		}, {
			input:  "explain verbose select * from emp",
			output: "explain (verbose) select * from emp",
		}, {
			input:  "explain analyze select * from emp",
			output: "explain (analyze) select * from emp",
		}, {
			input:  "explain analyze verbose select * from emp",
			output: "explain (analyze,verbose) select * from emp",
		}, {
			input:  "explain (analyze true,verbose false) select * from emp",
			output: "explain (analyze true,verbose false) select * from emp",
		}, {
			input:  "explain (analyze true,verbose false,format json) select * from emp",
			output: "explain (analyze true,verbose false,format json) select * from emp",
		}, {
			input:  "with t11 as (select * from t1) update t11 join t2 on t11.a = t2.b set t11.b = 1 where t2.a > 1",
			output: "with t11 as (select * from t1) update t11 inner join t2 on t11.a = t2.b set t11.b = 1 where t2.a > 1",
		}, {
			input:  "UPDATE items,(SELECT id FROM items WHERE id IN (SELECT id FROM items WHERE retail / wholesale >= 1.3 AND quantity < 100)) AS discounted SET items.retail = items.retail * 0.9 WHERE items.id = discounted.id",
			output: "update items cross join (select id from items where id in (select id from items where retail / wholesale >= 1.3 and quantity < 100)) as discounted set items.retail = items.retail * 0.9 where items.id = discounted.id",
		}, {
			input:  "with t2 as (select * from t1) DELETE FROM a1, a2 USING t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id;",
			output: "with t2 as (select * from t1) delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id",
		}, {
			input:  "DELETE FROM a1, a2 USING t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id;",
			output: "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id",
		}, {
			input:  "DELETE a1, a2 FROM t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id",
			output: "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id",
		}, {
			input:  "DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id",
			output: "delete from t1, t2 using t1 inner join t2 inner join t3 where t1.id = t2.id and t2.id = t3.id",
		}, {
			input:  "DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id",
			output: "delete from t1, t2 using t1 inner join t2 inner join t3 where t1.id = t2.id and t2.id = t3.id",
		}, {
			input: "select cast(false as varchar)",
		}, {
			input: "select cast(a as timestamp)",
		}, {
			input:  "select cast(\"2022-01-30\" as varchar);",
			output: "select cast(2022-01-30 as varchar)",
		}, {
			input: "select cast(b as timestamp) from t2",
		}, {
			input:  "select cast(\"2022-01-01 01:23:34\" as varchar)",
			output: "select cast(2022-01-01 01:23:34 as varchar)",
		}, {
			input:  "select serial_extract(col, 1 as varchar(3)) from t1",
			output: "select serial_extract(col, 1 as varchar(3)) from t1",
		}, {
			input:  "select binary('Geeksforgeeks')",
			output: "select binary(Geeksforgeeks)",
		}, {
			input:  "show schemas where 1",
			output: "show databases where 1",
		}, {
			input: "select role from t1",
		}, {
			input:  "select a || 'hello' || 'world' from t1;",
			output: "select concat(concat(a, hello), world) from t1",
		}, {
			input:  "select col || 'bar'",
			output: "select concat(col, bar)",
		}, {
			input:  "select 'foo' || 'bar'",
			output: "select concat(foo, bar)",
		}, {
			input:  "select 'a\\'b'",
			output: "select a'b",
		}, {
			input:  "select char_length('\\n\\t\\r\\b\\0\\_\\%\\\\');",
			output: "select char_length(\\n\\t\\r\\b\\0\\_\\%\\\\)",
		}, {
			input:  "select CAST('10 ' as unsigned);",
			output: "select cast(10  as unsigned)",
		}, {
			input:  "select CAST('10 ' as unsigned integer);",
			output: "select cast(10  as integer unsigned)",
		}, {
			input:  "SELECT ((+0) IN ((0b111111111111111111111111111111111111111111111111111),(rpad(1.0,2048,1)), (32767.1)));",
			output: "select ((+0) in ((0b111111111111111111111111111111111111111111111111111), (rpad(1.0, 2048, 1)), (32767.1)))",
		}, {
			input: "select 0b111111111111111111111111111111111111111111111111111",
		}, {
			input:  "select date,format,to_date(date, format) as to_date from t1;",
			output: "select date, format, to_date(date, format) as to_date from t1",
		}, {
			input:  "select date,format,concat_ws(',',to_date(date, format)) as con from t1;",
			output: "select date, format, concat_ws(,, to_date(date, format)) as con from t1",
		}, {
			input:  "select date,format,to_date(date, format) as to_date from t1;",
			output: "select date, format, to_date(date, format) as to_date from t1",
		}, {
			input:  "select date,format,concat_ws(\" \",to_date(date, format),'') as con from t1;",
			output: "select date, format, concat_ws( , to_date(date, format), ) as con from t1",
		}, {
			input: "select schema()",
		}, {
			input: "select last_insert_id()",
		}, {
			input:  "show char set where charset = 'utf8mb4'",
			output: "show charset where charset = utf8mb4",
		}, {
			input:  "show charset where charset = 'utf8mb4'",
			output: "show charset where charset = utf8mb4",
		}, {
			input:  "show character set where charset = 'utf8mb4'",
			output: "show charset where charset = utf8mb4",
		}, {
			input: "show config where a > 1",
		}, {
			input:  "set @@a = b",
			output: "set a = b",
		}, {
			input:  "set @a = b",
			output: "set a = b",
		}, {
			input:  "CREATE TABLE t1 (datetime datetime, timestamp timestamp, date date)",
			output: "create table t1 (datetime datetime, timestamp timestamp, date date)",
		}, {
			input:  "SET timestamp=DEFAULT;",
			output: "set timestamp = default",
		}, {
			input:  "SET timestamp=UNIX_TIMESTAMP('2011-07-31 10:00:00')",
			output: "set timestamp = UNIX_TIMESTAMP(2011-07-31 10:00:00)",
		}, {
			input:  "select ltrim(\"a\"),rtrim(\"a\"),trim(BOTH \"\" from \"a\"),trim(BOTH \" \" from \"a\");",
			output: "select ltrim(a), rtrim(a), trim(both  from a), trim(both   from a)",
		}, {
			input:  "select rpad('hello', -18446744073709551616, '1');",
			output: "select rpad(hello, -18446744073709551616, 1)",
		}, {
			input:  "select rpad('hello', -18446744073709551616, '1');",
			output: "select rpad(hello, -18446744073709551616, 1)",
		}, {
			input:  "SELECT CONCAT_WS(1471290948102948112341241204312904-23412412-4141, \"a\", \"b\")",
			output: "select CONCAT_WS(1471290948102948112341241204312904 - 23412412 - 4141, a, b)",
		}, {
			input:  "SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );",
			output: "select * from t1 where a = any (select 1 union (select 1 union select 1))",
		}, {
			input:  "SELECT * FROM t1 WHERE a = ANY ( SELECT 1 except ( SELECT 1 except SELECT 1 ) );",
			output: "select * from t1 where a = any (select 1 except (select 1 except select 1))",
		}, {
			input:  "SELECT * FROM t1 WHERE a = ANY ( SELECT 1 intersect ( SELECT 1 intersect SELECT 1 ) );",
			output: "select * from t1 where a = any (select 1 intersect (select 1 intersect select 1))",
		}, {
			input:  "SELECT * FROM t1 WHERE a = ANY ( SELECT 1 minus ( SELECT 1 minus SELECT 1 ) );",
			output: "select * from t1 where a = any (select 1 minus (select 1 minus select 1))",
		}, {
			input:  "SELECT * FROM t1 WHERE (a,b) = ANY (SELECT a, max(b) FROM t1 GROUP BY a);",
			output: "select * from t1 where (a, b) = any (select a, max(b) from t1 group by a)",
		}, {
			input:  "select  (1,2) != ALL (select * from t1);",
			output: "select (1, 2) != all (select * from t1)",
		}, {
			input:  "select s1, s1 = ANY (SELECT s1 FROM t2) from t1;",
			output: "select s1, s1 = any (select s1 from t2) from t1",
		}, {
			input:  "select * from t3 where a >= some (select b from t2);",
			output: "select * from t3 where a >= some (select b from t2)",
		}, {
			input:  "select 9999999999999999999;",
			output: "select 9999999999999999999",
		}, {
			input:  "select substring('hello', -18446744073709551616, -18446744073709551616);",
			output: "select substring(hello, -18446744073709551616, -18446744073709551616)",
		}, {
			input:  "select substring('hello', -18446744073709551616, 1);",
			output: "select substring(hello, -18446744073709551616, 1)",
		}, {
			input:  "select space(18446744073709551616);",
			output: "select space(18446744073709551616)",
		}, {
			input:  "select space(-18446744073709551616);",
			output: "select space(-18446744073709551616)",
		}, {
			input:  "select ltrim(\"a\"),rtrim(\"a\"),trim(BOTH \"\" from \"a\"),trim(BOTH \" \" from \"a\");",
			output: "select ltrim(a), rtrim(a), trim(both  from a), trim(both   from a)",
		}, {
			input:  "SELECT (rpad(1.0, 2048,1)) IS NOT FALSE;",
			output: "select (rpad(1.0, 2048, 1)) is not false",
		}, {
			input:  "SELECT 1 is unknown;",
			output: "select 1 is unknown",
		}, {
			input:  "SELECT false is not unknown;",
			output: "select false is not unknown",
		}, {
			input:  "SELECT 1 is true;",
			output: "select 1 is true",
		}, {
			input:  "SELECT false is not true;",
			output: "select false is not true",
		}, {
			input:  "SELECT 1 is false;",
			output: "select 1 is false",
		}, {
			input:  "SELECT false is not false;",
			output: "select false is not false",
		}, {
			input:  "SELECT FROM_UNIXTIME(99999999999999999999999999999999999999999999999999999999999999999);",
			output: "select FROM_UNIXTIME(99999999999999999999999999999999999999999999999999999999999999999)",
		}, {
			input:  "SELECT FROM_UNIXTIME(2147483647) AS c1, FROM_UNIXTIME(2147483648) AS c2, FROM_UNIXTIME(2147483647.9999999) AS c3, FROM_UNIXTIME(32536771199) AS c4,FROM_UNIXTIME(32536771199.9999999) AS c5;",
			output: "select FROM_UNIXTIME(2147483647) as c1, FROM_UNIXTIME(2147483648) as c2, FROM_UNIXTIME(2147483647.9999999) as c3, FROM_UNIXTIME(32536771199) as c4, FROM_UNIXTIME(32536771199.9999999) as c5",
		}, {
			input:  "select date_add(\"1997-12-31 23:59:59\",INTERVAL -100000 YEAR);",
			output: "select date_add(1997-12-31 23:59:59, INTERVAL(-100000, year))",
		}, {
			input:  "SELECT ADDDATE(DATE'2021-01-01', INTERVAL 1 DAY);",
			output: "select ADDDATE(DATE(2021-01-01), INTERVAL(1, day))",
		}, {
			input:  "select '2007-01-01' + interval a day from t1;",
			output: "select 2007-01-01 + interval(a, day) from t1",
		}, {
			input:  "SELECT CAST(COALESCE(t0.c0, -1) AS UNSIGNED) IS TRUE FROM t0;",
			output: "select cast(COALESCE(t0.c0, -1) as unsigned) is true from t0",
		}, {
			input:  "select Fld1, variance(Fld2) as q from t1 group by Fld1 having q is not null;",
			output: "select Fld1, variance(Fld2) as q from t1 group by Fld1 having q is not null",
		}, {
			input:  "select variance(-99999999999999999.99999);",
			output: "select variance(-99999999999999999.99999)",
		}, {
			input:  "select Fld1, std(Fld2) from t1 group by Fld1 having variance(Fld2) is not null",
			output: "select Fld1, std(Fld2) from t1 group by Fld1 having variance(Fld2) is not null",
		}, {
			input:  "select a.f1 as a, a.f1 > b.f1 as gt, a.f1 < b.f1 as lt, a.f1<=>b.f1 as eq from t1 a, t1 b;",
			output: "select a.f1 as a, a.f1 > b.f1 as gt, a.f1 < b.f1 as lt, a.f1 <=> b.f1 as eq from t1 as a cross join t1 as b",
		}, {
			input:  "select var_samp(s) as '0.5', var_pop(s) as '0.25' from bug22555;",
			output: "select var_samp(s) as 0.5, var_pop(s) as 0.25 from bug22555",
		}, {
			input:  "select var_samp(s) as 'null', var_pop(s) as 'null' from bug22555;",
			output: "select var_samp(s) as null, var_pop(s) as null from bug22555",
		}, {
			input: "select cast(variance(ff) as decimal(10, 3)) from t2",
		}, {
			input:  "SELECT GROUP_CONCAT(DISTINCT 2) from t1",
			output: "select GROUP_CONCAT(distinct 2, ,) from t1",
		}, {
			input:  "SELECT GROUP_CONCAT(DISTINCT a order by a) from t1",
			output: "select GROUP_CONCAT(distinct a, ,order by a) from t1",
		}, {
			input: "select variance(2) from t1",
		}, {
			input:  "select SQL_BIG_RESULT bit_and(col), bit_or(col) from t1 group by col;",
			output: "select sql_big_result bit_and(col), bit_or(col) from t1 group by col",
		}, {
			input: "select sql_small_result t2.id, avg(rating + 0.0e0) from t2 group by t2.id",
		}, {
			input: "select sql_small_result t2.id, avg(rating) from t2 group by t2.id",
		}, {
			input:  "select any_value(name), avg(value1), std(value1), variance(value1) from t1, t2 where t1.id = t2.id group by t1.id",
			output: "select any_value(name), avg(value1), std(value1), variance(value1) from t1 cross join t2 where t1.id = t2.id group by t1.id",
		}, {
			input: "select id, avg(value1), std(value1), variance(value1) from t1 group by id",
		}, {
			input: "select i, count(*), std(s1 / s2) from bug22555 group by i order by i",
		}, {
			input: "select i, count(*), variance(s1 / s2) from bug22555 group by i order by i",
		}, {
			input: "select i, count(*), variance(s1 / s2) from bug22555 group by i order by i",
		}, {
			input:  "select name, avg(value1), std(value1), variance(value1) from t1, t2 where t1.id = t2.id group by t1.id",
			output: "select name, avg(value1), std(value1), variance(value1) from t1 cross join t2 where t1.id = t2.id group by t1.id",
		}, {
			input:  "select sum(all a),count(all a),avg(all a),std(all a),variance(all a),bit_or(all a),bit_and(all a),min(all a),max(all a),min(all c),max(all c) from t",
			output: "select sum(all a), count(all a), avg(all a), std(all a), variance(all a), bit_or(all a), bit_and(all a), min(all a), max(all a), min(all c), max(all c) from t",
		}, {
			input:  "insert into t1 values (date_add(NULL, INTERVAL 1 DAY));",
			output: "insert into t1 values (date_add(null, INTERVAL(1, day)))",
		}, {
			input:  "replace into t1 values (date_add(NULL, INTERVAL 1 DAY));",
			output: "replace into t1 values (date_add(null, INTERVAL(1, day)))",
		}, {
			input:  "SELECT DATE_ADD('2022-02-28 23:59:59.9999', INTERVAL 1 SECOND) '1 second later';",
			output: "select DATE_ADD(2022-02-28 23:59:59.9999, INTERVAL(1, second)) as 1 second later",
		}, {
			input:  "SELECT sum(a) as 'hello' from t1;",
			output: "select sum(a) as hello from t1",
		}, {
			input:  "select stream from t1;",
			output: "select stream from t1",
		}, {
			input:  "SELECT DATE_ADD(\"2017-06-15\", INTERVAL -10 MONTH);",
			output: "select DATE_ADD(2017-06-15, INTERVAL(-10, month))",
		}, {
			input:  "create table t1 (a varchar)",
			output: "create table t1 (a varchar)",
		}, {
			input:  "SELECT (CAST(0x7FFFFFFFFFFFFFFF AS char));",
			output: "select (cast(0x7fffffffffffffff as varchar))",
		}, {
			input:  "select cast(-19999999999999999999 as signed);",
			output: "select cast(-19999999999999999999 as signed)",
		}, {
			input:  "select cast(19999999999999999999 as signed);",
			output: "select cast(19999999999999999999 as signed)",
		}, {
			input:  "select date_sub(now(), interval 1 day) from t1;",
			output: "select date_sub(now(), interval(1, day)) from t1",
		}, {
			input:  "select date_sub(now(), interval '1' day) from t1;",
			output: "select date_sub(now(), interval(1, day)) from t1",
		}, {
			input:  "select date_add(now(), interval '1' day) from t1;",
			output: "select date_add(now(), interval(1, day)) from t1",
		}, {
			input:  "SELECT md.datname as `Database` FROM TT md",
			output: "select md.datname as Database from tt as md",
		}, {
			input:  "select * from t where a = `Hello`",
			output: "select * from t where a = Hello",
		}, {
			input:  "CREATE VIEW v AS SELECT * FROM t WHERE t.id = f(t.name);",
			output: "create view v as select * from t where t.id = f(t.name)",
		}, {
			input:  "ALTER VIEW v AS SELECT * FROM t WHERE t.id = f(t.name);",
			output: "alter view v as select * from t where t.id = f(t.name)",
		}, {
			input:  "CREATE VIEW v AS SELECT qty, price, qty*price AS value FROM t;",
			output: "create view v as select qty, price, qty * price as value from t",
		}, {
			input:  "ALTER VIEW v AS SELECT qty, price, qty*price AS value FROM t;",
			output: "alter view v as select qty, price, qty * price as value from t",
		}, {
			input: "create view v_today (today) as select current_day from t",
		}, {
			input: "alter view v_today (today) as select current_day from t",
		}, {
			input: "explain (analyze true,verbose false) select * from emp",
		}, {
			input: "select quarter from ontime limit 1",
		}, {
			input: "select month from ontime limit 1",
		}, {
			input: "with tw as (select * from t2), tf as (select * from t3) select * from tw where a > 1",
		}, {
			input: "with tw as (select * from t2) select * from tw where a > 1",
		}, {
			input:  "create table t (a double(13))  // comment",
			output: "create table t (a double(13))",
		}, {
			input:  "create table t (a double(13))  -- comment",
			output: "create table t (a double(13))",
		}, {
			input: "select a as promo_revenue from (select * from r) as c_orders(c_custkey, c_count)",
		}, {
			input:  "select extract(year from l_shipdate) as l_year from t",
			output: "select extract(year, l_shipdate) as l_year from t",
		}, {
			input:  "select * from R join S on R.uid = S.uid where l_shipdate <= date '1998-12-01' - interval '112' day",
			output: "select * from r inner join s on R.uid = S.uid where l_shipdate <= date(1998-12-01) - interval(112, day)",
		}, {
			input: "create table deci_table (a decimal(10, 5))",
		}, {
			input: "create table deci_table (a decimal(20, 5))",
		}, {
			input:  "create table deci_table (a decimal)",
			output: "create table deci_table (a decimal(38))",
		}, {
			input: "create table deci_table (a decimal(20))",
		}, {
			input: "select substr(name, 5) from t1",
		}, {
			input: "select substring(name, 5) from t1",
		}, {
			input: "select substr(name, 5, 3) from t1",
		}, {
			input: "select substring(name, 5, 3) from t1",
		}, {
			input:  "select * from R join S on R.uid = S.uid",
			output: "select * from r inner join s on R.uid = S.uid",
		}, {
			input:  "create table t (a int, b char, key idx1 type zonemap (a, b))",
			output: "create table t (a int, b char, index idx1 using zonemap (a, b))",
		}, {
			input: "create table t (a int, index idx1 using zonemap (a))",
		}, {
			input: "create table t (a int, index idx1 using bsi (a))",
		}, {
			input:  "set @@sql_mode ='TRADITIONAL'",
			output: "set sql_mode = TRADITIONAL",
		}, {
			input:  "set @@session.sql_mode ='TRADITIONAL'",
			output: "set sql_mode = TRADITIONAL",
		}, {
			input:  "set session sql_mode ='TRADITIONAL'",
			output: "set sql_mode = TRADITIONAL",
		}, {
			input:  "select @session.tx_isolation",
			output: "select @session.tx_isolation",
		}, {
			input:  "select @@session.tx_isolation",
			output: "select @@tx_isolation",
		}, {
			input:  "/* mysql-connector-java-8.0.27 (Revision: e920b979015ae7117d60d72bcc8f077a839cd791) */SHOW VARIABLES;",
			output: "show variables",
		}, {
			input: "create index idx1 using bsi on a (a)",
		}, {
			input:  "INSERT INTO pet VALUES row('Sunsweet05','Dsant05','otter','f',30.11,2), row('Sunsweet06','Dsant06','otter','m',30.11,3);",
			output: "insert into pet values (Sunsweet05, Dsant05, otter, f, 30.11, 2), (Sunsweet06, Dsant06, otter, m, 30.11, 3)",
		}, {
			input:  "INSERT INTO t1 SET f1 = -1.0e+30, f2 = 'exore', f3 = 123",
			output: "insert into t1 (f1, f2, f3) values (-1.0e+30, exore, 123)",
		}, {
			input:  "INSERT INTO t1 SET f1 = -1;",
			output: "insert into t1 (f1) values (-1)",
		}, {
			input:  "INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b), b=VALUES(a)+VALUES(c);",
			output: "insert into t1 (a, b, c) values (1, 2, 3), (4, 5, 6) on duplicate key update c = VALUES(a) + VALUES(b), b = VALUES(a) + VALUES(c)",
		}, {
			input:  "INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=2, b=3;",
			output: "insert into t1 (a, b, c) values (1, 2, 3), (4, 5, 6) on duplicate key update c = 2, b = 3",
		}, {
			input:  "INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=2/2, b=3;",
			output: "insert into t1 (a, b, c) values (1, 2, 3), (4, 5, 6) on duplicate key update c = 2 / 2, b = 3",
		}, {
			input:  "insert into t1 values (18446744073709551615), (0xFFFFFFFFFFFFFFFE), (18446744073709551613), (18446744073709551612)",
			output: "insert into t1 values (18446744073709551615), (0xfffffffffffffffe), (18446744073709551613), (18446744073709551612)",
		}, {
			input:  "REPLACE INTO pet VALUES row('Sunsweet05','Dsant05','otter','f',30.11,2), row('Sunsweet06','Dsant06','otter','m',30.11,3);",
			output: "replace into pet values (Sunsweet05, Dsant05, otter, f, 30.11, 2), (Sunsweet06, Dsant06, otter, m, 30.11, 3)",
		}, {
			input:  "REPLACE INTO t1 SET f1 = -1.0e+30, f2 = 'exore', f3 = 123",
			output: "replace into t1 (f1, f2, f3) values (-1.0e+30, exore, 123)",
		}, {
			input:  "REPLACE INTO t1 SET f1 = -1;",
			output: "replace into t1 (f1) values (-1)",
		}, {
			input:  "replace into t1 values (18446744073709551615), (0xFFFFFFFFFFFFFFFE), (18446744073709551613), (18446744073709551612)",
			output: "replace into t1 values (18446744073709551615), (0xfffffffffffffffe), (18446744073709551613), (18446744073709551612)",
		}, {
			input:  "create table t (a int) properties(\"host\" = \"127.0.0.1\", \"port\" = \"8239\", \"user\" = \"mysql_user\", \"password\" = \"mysql_passwd\")",
			output: "create table t (a int) properties(host = 127.0.0.1, port = 8239, user = mysql_user, password = mysql_passwd)",
		}, {
			input:  "create table t (a int) properties('a' = 'b')",
			output: "create table t (a int) properties(a = b)",
		}, {
			input: "create table t (a int, b char, check (1 + 1) enforced)",
		}, {
			input: "create table t (a int, b char, foreign key sdf (a, b) references b(a asc, b desc))",
		}, {
			input:  "create table t (a int, b char, constraint sdf foreign key (a, b) references b(a asc, b desc))",
			output: "create table t (a int, b char, constraint sdf foreign key (a, b) references b(a asc, b desc))",
		}, {
			input:  "create table t (a int, b char, constraint sdf foreign key dddd (a, b) references b(a asc, b desc))",
			output: "create table t (a int, b char, constraint sdf foreign key dddd (a, b) references b(a asc, b desc))",
		}, {
			input: "create table t (a int, b char, unique key idx (a, b))",
		}, {
			input: "create table t (a int, b char, index if not exists idx (a, b))",
		}, {
			input: "create table t (a int, b char, fulltext idx (a, b))",
		}, {
			input:  "create table t (a int, b char, constraint p1 primary key idx using hash (a, b))",
			output: "create table t (a int, b char, constraint p1 primary key idx using none (a, b))",
		}, {
			input: "create table t (a int, b char, primary key idx (a, b))",
		}, {
			input:  "create dynamic table t as select a from t1",
			output: "create dynamic table t as select a from t1",
		}, {
			input:  "create dynamic table t as select a from t1 with (\"type\"='kafka')",
			output: "create dynamic table t as select a from t1 with (type = kafka)",
		}, {
			input:  "create external table t (a int) infile 'data.txt'",
			output: "create external table t (a int) infile 'data.txt'",
		}, {
			input: "create external table t (a int) infile {'filepath'='data.txt', 'compression'='none'}",
		}, {
			input: "create external table t (a int) infile {'filepath'='data.txt', 'compression'='auto'}",
		}, {
			input: "create external table t (a int) infile {'filepath'='data.txt', 'compression'='lz4'}",
		}, {
			input:  "create external table t (a int) infile 'data.txt' FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY ''",
			output: "create external table t (a int) infile 'data.txt' fields terminated by '' optionally enclosed by '' lines terminated by ''",
		}, {
			input:  "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
			output: "set names = utf8mb4 utf8mb4_general_ci",
		}, {
			input: "insert into cms values (null, default)",
		}, {
			input: "replace into cms values (null, default)",
		}, {
			input:  "create database `show`",
			output: "create database show",
		}, {
			input: "create table table16 (1a20 int, 1e int)",
		}, {
			input: "insert into t2 values (-3, 2)",
		}, {
			input: "replace into t2 values (-3, 2)",
		}, {
			input:  "select spID,userID,score from t1 where spID>(userID-1);",
			output: "select spID, userID, score from t1 where spID > (userID - 1)",
		}, {
			input:  "CREATE TABLE t2(product VARCHAR(32),country_id INTEGER NOT NULL,year INTEGER,profit INTEGER)",
			output: "create table t2 (product varchar(32), country_id integer not null, year integer, profit integer)",
		}, {
			input: "insert into numtable values (255, 65535, 4294967295, 18446744073709551615)",
		}, {
			input: "replace into numtable values (255, 65535, 4294967295, 18446744073709551615)",
		}, {
			input: "create table numtable (a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned)",
		}, {
			input:  "SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by user",
			output: "select userID as user, MAX(score) as max from t1 group by userID order by user",
		}, {
			input:  "load data infile 'test/loadfile5' ignore INTO TABLE T.A FIELDS TERMINATED BY  ',' (@,@,c,d,e,f)",
			output: "load data infile test/loadfile5 ignore into table t.a fields terminated by , (, , c, d, e, f)",
		}, {
			input:  "load data infile '/root/lineorder_flat_10.tbl' into table lineorder_flat FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '';",
			output: "load data infile /root/lineorder_flat_10.tbl into table lineorder_flat fields terminated by '' optionally enclosed by '' lines terminated by ''",
		}, {
			input:  "load data local infile 'data' replace into table db.a (a, b, @vc, @vd) set a = @vc != 0, d = @vd != 1",
			output: "load data local infile data replace into table db.a (a, b, @vc, @vd) set a = @vc != 0, d = @vd != 1",
		}, {
			input:  "load data local infile 'data' replace into table db.a lines starting by '#' terminated by '\t' ignore 2 lines",
			output: "load data local infile data replace into table db.a lines starting by # terminated by 	 ignore 2 lines",
		}, {
			input:  "load data local infile 'data' replace into table db.a lines starting by '#' terminated by '\t' ignore 2 rows",
			output: "load data local infile data replace into table db.a lines starting by # terminated by 	 ignore 2 lines",
		}, {
			input:  "load data local infile 'data' replace into table db.a lines terminated by '\t' starting by '#' ignore 2 lines",
			output: "load data local infile data replace into table db.a lines starting by # terminated by 	 ignore 2 lines",
		}, {
			input:  "load data local infile 'data' replace into table db.a lines terminated by '\t' starting by '#' ignore 2 rows",
			output: "load data local infile data replace into table db.a lines starting by # terminated by 	 ignore 2 lines",
		}, {
			input:  "load data infile 'data.txt' into table db.a fields terminated by '\t' escaped by '\t'",
			output: "load data infile data.txt into table db.a fields terminated by \t escaped by \t",
		}, {
			input:  "load data infile 'data.txt' into table db.a fields terminated by '\t' enclosed by '\t' escaped by '\t'",
			output: "load data infile data.txt into table db.a fields terminated by \t enclosed by \t escaped by \t",
		}, {
			input:  "load data infile 'data.txt' into table db.a",
			output: "load data infile data.txt into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='auto'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='none'} into table db.a",
		}, {
			input:  "create external table t (a int) infile 'data.txt'",
			output: "create external table t (a int) infile 'data.txt'",
		}, {
			input: "create external table t (a int) infile {'filepath'='data.txt', 'compression'='none'}",
		}, {
			input: "create external table t (a int) infile {'filepath'='data.txt', 'compression'='auto'}",
		}, {
			input: "create external table t (a int) infile {'filepath'='data.txt', 'compression'='lz4'}",
		}, {
			input:  "create external table t (a int) infile 'data.txt' FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY ''",
			output: "create external table t (a int) infile 'data.txt' fields terminated by '' optionally enclosed by '' lines terminated by ''",
		}, {
			input:  "create external table t (a int) URL s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='XXX', 'secret_access_key'='XXX', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}",
			output: "create external table t (a int) url s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}",
		}, {
			input:  "load data infile 'test/loadfile5' ignore INTO TABLE T.A FIELDS TERMINATED BY  ',' (@,@,c,d,e,f)",
			output: "load data infile test/loadfile5 ignore into table t.a fields terminated by , (, , c, d, e, f)",
		}, {
			input:  "load data infile '/root/lineorder_flat_10.tbl' into table lineorder_flat FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '';",
			output: "load data infile /root/lineorder_flat_10.tbl into table lineorder_flat fields terminated by '' optionally enclosed by '' lines terminated by ''",
		}, {
			input:  "load data infile '/root/lineorder_flat_10.tbl' into table lineorder_flat FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '' parallel 'true';",
			output: "load data infile /root/lineorder_flat_10.tbl into table lineorder_flat fields terminated by '' optionally enclosed by '' lines terminated by '' parallel true ",
		}, {
			input:  "load data infile '/root/lineorder_flat_10.tbl' into table lineorder_flat FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '' parallel 'true' strict 'true';",
			output: "load data infile /root/lineorder_flat_10.tbl into table lineorder_flat fields terminated by '' optionally enclosed by '' lines terminated by '' parallel true strict true ",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='auto'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='none'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='GZIP'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='BZIP2'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='FLATE'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='LZW'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='ZLIB'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='LZ4'} into table db.a",
		}, {
			input:  "LOAD DATA URL s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='XXX', 'secret_access_key'='XXX', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'} into table db.a",
			output: "load data url s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'} into table db.a",
		},
		{
			input: `load data url s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='jsonline/jsonline_object.jl', 'region'='us-west-2', 'compression'='none', 'format'='jsonline', 'jsondata'='object'} into table t1`,
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='GZIP'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='BZIP2'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='FLATE'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='LZW'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='ZLIB'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'compression'='LZ4'} into table db.a",
		}, {
			input: "load data infile {'filepath'='data.txt', 'format'='jsonline', 'jsondata'='array'} into table db.a",
		},
		{
			input: "load data infile {'filepath'='data.txt', 'format'='jsonline', 'jsondata'='object'} into table db.a",
		},
		{
			input: "load data infile {'filepath'='data.txt', 'compression'='BZIP2', 'format'='jsonline', 'jsondata'='object'} into table db.a",
		},
		{
			input: "load data inline format='jsonline', data='[1,2,3,4]', jsontype='array' into table t1",
		},
		{
			input:  "show tables from test01 where tables_in_test01 like '%t2%'",
			output: "show tables from test01 where tables_in_test01 like %t2%",
		}, {
			input:  "select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score",
			output: "select userID, MAX(score) as max_score from t1 where concat(userID < 2, userID > 3) group by userID order by max_score",
		}, {
			input: "select c1, -c2 from t2 order by -c1 desc",
		}, {
			input:  "select * from t1 where spID>2 AND userID <2 || userID >=2 OR userID < 2 limit 3",
			output: "select * from t1 where concat(spID > 2 and userID < 2, userID >= 2) or userID < 2 limit 3",
		}, {
			input:  "select * from t10 where (b='ba' or b='cb') and (c='dc' or c='ed');",
			output: "select * from t10 where (b = ba or b = cb) and (c = dc or c = ed)",
		}, {
			input:  "select CAST(userID AS DOUBLE) cast_double, CAST(userID AS FLOAT(3)) cast_float , CAST(userID AS REAL) cast_real, CAST(userID AS SIGNED) cast_signed, CAST(userID AS UNSIGNED) cast_unsigned from t1 limit 2",
			output: "select cast(userID as double) as cast_double, cast(userID as float(3)) as cast_float, cast(userID as real) as cast_real, cast(userID as signed) as cast_signed, cast(userID as unsigned) as cast_unsigned from t1 limit 2",
		}, {
			input: "select distinct name as name1 from t1",
		}, {
			input:  "select userID, userID DIV 2 as user_dir, userID%2 as user_percent, userID MOD 2 as user_mod from t1",
			output: "select userID, userID div 2 as user_dir, userID % 2 as user_percent, userID % 2 as user_mod from t1",
		}, {
			input:  "select sum(score) as sum from t1 where spID=6 group by score order by sum desc",
			output: "select sum(score) as sum from t1 where spID = 6 group by score order by sum desc",
		}, {
			input:  "select userID,count(score) from t1 where userID>2 group by userID having count(score)>1",
			output: "select userID, count(score) from t1 where userID > 2 group by userID having count(score) > 1",
		}, {
			input:  "SELECT product, SUM(profit),AVG(profit) FROM t2 where product<>'TV' GROUP BY product order by product asc",
			output: "select product, SUM(profit), AVG(profit) from t2 where product != TV group by product order by product asc",
		}, {
			input:  "SELECT product, SUM(profit),AVG(profit) FROM t2 where product='Phone' GROUP BY product order by product asc",
			output: "select product, SUM(profit), AVG(profit) from t2 where product = Phone group by product order by product asc",
		}, {
			input:  "select sum(col_1d),count(col_1d),avg(col_1d),min(col_1d),max(col_1d) from tbl1 group by col_1e",
			output: "select sum(col_1d), count(col_1d), avg(col_1d), min(col_1d), max(col_1d) from tbl1 group by col_1e",
		}, {
			input:  "select u.a, (select t.a from sa.t, u) from u, (select t.a, u.a from sa.t, u where t.a = u.a) as t where (u.a, u.b, u.c) in (select t.a, u.a, t.b * u.b tubb from t)",
			output: "select u.a, (select t.a from sa.t cross join u) from u cross join (select t.a, u.a from sa.t cross join u where t.a = u.a) as t where (u.a, u.b, u.c) in (select t.a, u.a, t.b * u.b as tubb from t)",
		}, {
			input:  "select u.a, (select t.a from sa.t, u) from u",
			output: "select u.a, (select t.a from sa.t cross join u) from u",
		}, {
			input:  "select t.a, u.a, t.b * u.b from sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000 order by t.a desc, u.a asc, v.d asc, tubb limit 200 offset 100",
			output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000 order by t.a desc, u.a asc, v.d asc, tubb limit 200 offset 100",
		}, {
			input:  "select t.a, u.a, t.b * u.b from sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000",
			output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000",
		}, {
			input:  "select t.a, u.a, t.b * u.b from sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b)",
			output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b)",
		}, {
			input:  "SELECT t.a,u.a,t.b * u.b FROM sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b",
			output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b",
		}, {
			input: "select avg(u.a), count(u.b), cast(u.c as varchar) from u",
		}, {
			input: "select avg(u.a), count(*) from u",
		}, {
			input: "select approx_count(*) from u",
		}, {
			input: "select avg(u.a), count(u.b) from u",
		}, {
			input: "select sum(col_1d) from tbl1 where col_1d < 13 group by col_1e",
		}, {
			input:  "select sum(col_1a),count(col_1b),avg(col_1c),min(col_1d),max(col_1d) from tbl1",
			output: "select sum(col_1a), count(col_1b), avg(col_1c), min(col_1d), max(col_1d) from tbl1",
		}, {
			input:  "insert into tbl1 values (0,1,5,11, \"a\")",
			output: "insert into tbl1 values (0, 1, 5, 11, a)",
		}, {
			input:  "replace into tbl1 values (0,1,5,11, \"a\")",
			output: "replace into tbl1 values (0, 1, 5, 11, a)",
		}, {
			input: "create table tbl1 (col_1a tinyint, col_1b smallint, col_1c int, col_1d bigint, col_1e char(10) not null)",
		}, {
			input: "insert into numtable values (4, 1.234567891, 1.234567891)",
		}, {
			input: "insert into numtable values (3, 1.234567, 1.234567)",
		}, {
			input: "replace into numtable values (4, 1.234567891, 1.234567891)",
		}, {
			input: "replace into numtable values (3, 1.234567, 1.234567)",
		}, {
			input: "create table numtable (id int, fl float, dl double)",
		}, {
			input: "drop table if exists numtable",
		}, {
			input:  "create table table17 (`index` int)",
			output: "create table table17 (index int)",
		}, {
			input: "create table table19$ (a int)",
		}, {
			input:  "create table `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int);",
			output: "create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)",
		}, {
			input:  "create table table12 (`a ` int)",
			output: "create table table12 (a  int)",
		}, {
			input:  "create table `table11 ` (a int)",
			output: "create table table11  (a int)",
		}, {
			input:  "create table table10 (a int primary key, b varchar(10)) checksum=0 COMMENT=\"asdf\"",
			output: "create table table10 (a int primary key, b varchar(10)) checksum = 0 comment = 'asdf'",
		}, {
			input:  "create temporary table table05 ( a int, b char(10));",
			output: "create temporary table table05 (a int, b char(10))",
		}, {
			input:  "create table table15 (a varchar(5) default 'abcde')",
			output: "create table table15 (a varchar(5) default abcde)",
		}, {
			input:  "create table table01 (a TINYINT primary key, b SMALLINT SIGNED, c INT UNSIGNED, d BIGINT not null , e FLOAT unique,f DOUBLE, g CHAR(10), h VARCHAR(20))",
			output: "create table table01 (a tinyint primary key, b smallint, c int unsigned, d bigint not null, e float unique, f double, g char(10), h varchar(20))",
		}, {
			input:  "create database test04 CHARACTER SET=utf8 collate=utf8_general_ci ENCRYPTION='N'",
			output: "create database test04 character set utf8 collate utf8_general_ci encryption N",
		}, {
			input:  "create database test03 DEFAULT CHARACTER SET utf8 collate utf8_general_ci ENCRYPTION 'Y'",
			output: "create database test03 default character set utf8 collate utf8_general_ci encryption Y",
		}, {
			input: "drop database if exists t01234567890123456789012345678901234567890123456789012345678901234567890123456789",
		}, {
			input: "select distinct a from t",
		}, {
			input:  "select * from t where a like 'a%'",
			output: "select * from t where a like a%",
		}, {
			input: "select sysdate(), curtime(22) from t",
		}, {
			input: "select sysdate(), curtime from t",
		}, {
			input:  "select current_time(), current_timestamp, lacalTIMe(89), utc_time() from t",
			output: "select current_time(), current_timestamp(), lacalTIMe(89), utc_time() from t",
		}, {
			input:  "select current_user(), current_role(), current_date, utc_date from t",
			output: "select current_user(), current_role(), current_date(), utc_date() from t",
		}, {
			input: "select ascii(a), collation(b), hour(c), microsecond(d) from t",
		}, {
			input:  "select dayofmonth('2001-11-00'), month('2005-00-00') from t",
			output: "select dayofmonth(2001-11-00), month(2005-00-00) from t",
		}, {
			input: "select sum(distinct s) from tbl where 1",
		}, {
			input:  "select u.a, interval 1 second from t",
			output: "select u.a, interval(1, second) from t",
		}, {
			input:  "select u.a, (select t.a from sa.t, u) from t where (u.a, u.b, u.c) in (select * from t)",
			output: "select u.a, (select t.a from sa.t cross join u) from t where (u.a, u.b, u.c) in (select * from t)",
		}, {
			input:  "select u.a, (select t.a from sa.t, u) from t where (u.a, u.b, u.c)",
			output: "select u.a, (select t.a from sa.t cross join u) from t where (u.a, u.b, u.c)",
		}, {
			input:  "select u.a, (select t.a from sa.t, u) from u",
			output: "select u.a, (select t.a from sa.t cross join u) from u",
		}, {
			input:  "select t.a from sa.t, u",
			output: "select t.a from sa.t cross join u",
		}, {
			input: "select t.a from sa.t",
		}, {
			input:  "create table k1 (id int not null primary key,name varchar(20)) partition by key() partitions 2",
			output: "create table k1 (id int not null primary key, name varchar(20)) partition by key algorithm = 2 partitions 2",
		}, {
			input:  "create table k1 (id int not null,name varchar(20),unique key (id))partition by key() partitions 2",
			output: "create table k1 (id int not null, name varchar(20), unique key (id)) partition by key algorithm = 2 partitions 2",
		}, {
			input:  "create table a (a int) partition by key (a, b, db.t.c) (partition xx (subpartition s1, subpartition s3 max_rows = 1000 min_rows = 100))",
			output: "create table a (a int) partition by key algorithm = 2 (a, b, db.t.c) (partition xx (subpartition s1, subpartition s3 max_rows = 1000 min_rows = 100))",
		}, {
			input:  "create table a (a int) partition by key (a, b, db.t.c) (partition xx row_format = dynamic max_rows = 1000 min_rows = 100)",
			output: "create table a (a int) partition by key algorithm = 2 (a, b, db.t.c) (partition xx row_format = dynamic max_rows = 1000 min_rows = 100)",
		}, {
			input:  "create table a (a int) engine = 'innodb' row_format = dynamic comment = 'table A' compression = 'lz4' data directory = '/data' index directory = '/index' max_rows = 1000 min_rows = 100",
			output: "create table a (a int) engine = innodb row_format = dynamic comment = 'table A' compression = lz4 data directory = /data index directory = /index max_rows = 1000 min_rows = 100",
		}, {
			input:  "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) (partition xx values less than (1, 2, 323), partition yy)",
			output: "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) (partition xx values less than (1, 2, 323), partition yy)",
		}, {
			input:  "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) partitions 10 subpartition by key (a, b, db.t.c) subpartitions 10",
			output: "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) partitions 10 subpartition by key algorithm = 2 (a, b, db.t.c) subpartitions 10",
		}, {
			input: "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) partitions 10",
		}, {
			input: "create table a (a int) partition by linear hash (1 + 1234 / 32)",
		}, {
			input: "create table a (a int) partition by linear key algorithm = 31 (a, b, db.t.c)",
		}, {
			input:  "create table a (a int) partition by linear key (a, b, db.t.c)",
			output: "create table a (a int) partition by linear key algorithm = 2 (a, b, db.t.c)",
		}, {
			input: "create table a (a int) partition by list columns (a, b, db.t.c)",
		}, {
			input: "create table a (a int) partition by list columns (a, b, db.t.c)",
		}, {
			input: "create table a (a int) partition by range columns (a, b, db.t.c)",
		}, {
			input: "create table a (a int) partition by range(1 + 21)",
		}, {
			input: "create table a (a int storage disk constraint cx check (b + c) enforced)",
		}, {
			input: "create table a (a int storage disk, b int references b(a asc, b desc) match full on delete cascade on update restrict)",
		}, {
			input: "create table a (a int storage disk, b int)",
		}, {
			input: "create table a (a int not null default 1 auto_increment unique primary key collate utf8_bin storage disk)",
		},
		{
			input:  `CREATE TABLE tp1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3) PARTITIONS 4`,
			output: `create table tp1 (col1 int, col2 char(5), col3 date) partition by key algorithm = 2 (col3) partitions 4`,
		},
		{
			input:  `CREATE TABLE tp2 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3)`,
			output: `create table tp2 (col1 int, col2 char(5), col3 date) partition by key algorithm = 2 (col3)`,
		},
		{
			input:  `CREATE TABLE tp3 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY(col3) PARTITIONS 5`,
			output: `create table tp3 (col1 int, col2 char(5), col3 date) partition by linear key algorithm = 2 (col3) partitions 5`,
		},
		{
			input:  `CREATE TABLE tp4 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM = 1 (col3)`,
			output: `create table tp4 (col1 int, col2 char(5), col3 date) partition by key algorithm = 1 (col3)`,
		},
		{
			input:  `CREATE TABLE tp5 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY ALGORITHM = 1 (col3) PARTITIONS 5;`,
			output: `create table tp5 (col1 int, col2 char(5), col3 date) partition by linear key algorithm = 1 (col3) partitions 5`,
		},
		{
			input:  `CREATE TABLE tp6 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col1, col2) PARTITIONS 4`,
			output: `create table tp6 (col1 int, col2 char(5), col3 date) partition by key algorithm = 2 (col1, col2) partitions 4`,
		},
		{
			input:  `CREATE TABLE tp7 (col1 INT NOT NULL, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL, PRIMARY KEY (col1, col2)) PARTITION BY KEY(col1) PARTITIONS 4`,
			output: `create table tp7 (col1 int not null, col2 date not null, col3 int not null, col4 int not null, primary key (col1, col2)) partition by key algorithm = 2 (col1) partitions 4`,
		},
		{
			input:  `CREATE TABLE tp9 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1) PARTITIONS 4`,
			output: `create table tp9 (col1 int, col2 char(5)) partition by hash (col1) partitions 4`,
		},
		{
			input:  `CREATE TABLE tp10 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3));`,
			output: `create table tp10 (col1 int, col2 char(5), col3 datetime) partition by hash (YEAR(col3))`,
		},
		{
			input:  `CREATE TABLE tp11 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6`,
			output: `create table tp11 (col1 int, col2 char(5), col3 date) partition by linear hash (YEAR(col3)) partitions 6`,
		},
		{
			input:  `CREATE TABLE tp12 (col1 INT NOT NULL, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL, PRIMARY KEY (col1, col2)) PARTITION BY HASH(col1) PARTITIONS 4`,
			output: `create table tp12 (col1 int not null, col2 date not null, col3 int not null, col4 int not null, primary key (col1, col2)) partition by hash (col1) partitions 4`,
		},
		{
			input: `CREATE TABLE tp13 (
					id INT NOT NULL,
					fname VARCHAR(30),
					lname VARCHAR(30),
					hired DATE NOT NULL DEFAULT '1970-01-01',
					separated DATE NOT NULL DEFAULT '9999-12-31',
					job_code INT,
					store_id INT
				)
				PARTITION BY RANGE ( YEAR(separated) ) (
					PARTITION p0 VALUES LESS THAN (1991),
					PARTITION p1 VALUES LESS THAN (1996),
					PARTITION p2 VALUES LESS THAN (2001),
					PARTITION p3 VALUES LESS THAN MAXVALUE
				);`,
			output: `create table tp13 (id int not null, fname varchar(30), lname varchar(30), hired date not null default 1970-01-01, separated date not null default 9999-12-31, job_code int, store_id int) partition by range(YEAR(separated)) (partition p0 values less than (1991), partition p1 values less than (1996), partition p2 values less than (2001), partition p3 values less than (MAXVALUE))`,
		},
		{
			input: `CREATE TABLE tp14 (
					a INT NOT NULL,
					b INT NOT NULL
				)
				PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 4 (
					PARTITION p0 VALUES LESS THAN (10,5),
					PARTITION p1 VALUES LESS THAN (20,10),
					PARTITION p2 VALUES LESS THAN (50,20),
					PARTITION p3 VALUES LESS THAN (65,30)
				)`,
			output: `create table tp14 (a int not null, b int not null) partition by range columns (a, b) partitions 4 (partition p0 values less than (10, 5), partition p1 values less than (20, 10), partition p2 values less than (50, 20), partition p3 values less than (65, 30))`,
		},
		{
			input: `CREATE TABLE tp15 (
					id   INT PRIMARY KEY,
					name VARCHAR(35),
					age INT unsigned
				)
				PARTITION BY LIST (id) (
					PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
					PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
					PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
					PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
				);`,
			output: `create table tp15 (id int primary key, name varchar(35), age int unsigned) partition by list(id) (partition r0 values in (1, 5, 9, 13, 17, 21), partition r1 values in (2, 6, 10, 14, 18, 22), partition r2 values in (3, 7, 11, 15, 19, 23), partition r3 values in (4, 8, 12, 16, 20, 24))`,
		},
		{
			input: `CREATE TABLE tp16 (
					a INT NULL,
					b INT NULL
				)
				PARTITION BY LIST COLUMNS(a,b) (
					PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
					PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
					PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
					PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
				)`,
			output: `create table tp16 (a int null, b int null) partition by list columns (a, b) (partition p0 values in ((0, 0), (null, null)), partition p1 values in ((0, 1), (0, 2), (0, 3), (1, 1), (1, 2)), partition p2 values in ((1, 0), (2, 0), (2, 1), (3, 0), (3, 1)), partition p3 values in ((1, 3), (2, 2), (2, 3), (3, 2), (3, 3)))`,
		},
		{
			input: `CREATE TABLE tp17 (
					id INT NOT NULL PRIMARY KEY,
					fname VARCHAR(30),
					lname VARCHAR(30)
				)
				PARTITION BY RANGE (id) (
					PARTITION p0 VALUES LESS THAN (6),
					PARTITION p1 VALUES LESS THAN (11),
					PARTITION p2 VALUES LESS THAN (16),
					PARTITION p3 VALUES LESS THAN (21)
				);`,
			output: `create table tp17 (id int not null primary key, fname varchar(30), lname varchar(30)) partition by range(id) (partition p0 values less than (6), partition p1 values less than (11), partition p2 values less than (16), partition p3 values less than (21))`,
		},
		{
			input: "grant all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.a to u1, u2 with grant option",
		}, {
			input: "grant proxy on u1 to u2, u3, u4 with grant option",
		}, {
			input: "grant proxy on u1 to u2, u3, u4",
		},
		{
			input: "grant r1, r2, r3 to u1, u1, u3",
		}, {
			input:  "grant super(a, b, c) on procedure db.func to 'h3'",
			output: "grant super(a, b, c) on procedure db.func to h3",
		},
		{
			input:  "revoke all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.A from u1, u2",
			output: "revoke all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.a from u1, u2",
		}, {
			input: "revoke r1, r2, r3 from u1, u2, u3",
		}, {
			input: "revoke super(a, b, c) on procedure db.func from h3",
		}, {
			input:  "revoke all on table db.A from u1, u2",
			output: "revoke all on table db.a from u1, u2",
		}, {
			input: "revoke all on table db.a from u1",
		}, {
			input: "set default role r1, r2, r3 to u1, u2, u3",
		}, {
			input: "set default role all to u1, u2, u3",
		}, {
			input: "set default role none to u1, u2, u3",
		}, {
			input:  "set password = password('ppp')",
			output: "set password = ppp",
		}, {
			input:  "set password for u1@h1 = password('ppp')",
			output: "set password for u1@h1 = ppp",
		}, {
			input:  "set password for u1@h1 = 'ppp'",
			output: "set password for u1@h1 = ppp",
		}, {
			input:  "set @a = 0, @b = 1",
			output: "set a = 0, b = 1",
		}, {
			input:  "set a = 0, session b = 1, @@session.c = 1, global d = 1, @@global.e = 1",
			output: "set a = 0, b = 1, c = 1, global d = 1, global e = 1",
		}, {
			input:  "set @@session.a = 1",
			output: "set a = 1",
		}, {
			input:  "set @@global.a = 1",
			output: "set global a = 1",
		}, {
			input: "set global a = 1",
		}, {
			input:  "set persist a = 1",
			output: "set global a = 1",
		}, {
			input:  "alter account config set MYSQL_COMPATIBILITY_MODE a = 1",
			output: "set global a = 1",
		}, {
			input: "set a = 1",
		}, {
			input: "rollback",
		}, {
			input:  "rollback and chain no release",
			output: "rollback",
		}, {
			input:  "commit and chain no release",
			output: "commit",
		}, {
			input: "commit",
		}, {
			input: "start transaction read only",
		}, {
			input: "start transaction read write",
		}, {
			input: "start transaction",
		}, {
			input: "use db1",
		}, {
			input: "use",
		}, {
			input: "update a as aa set a = 3, b = 4 where a != 0 order by b limit 1",
		}, {
			input: "update a as aa set a = 3, b = 4",
		}, {
			input: "explain insert into u (a, b, c, d) values (1, 2, 3, 4), (5, 6, 7, 8)",
		}, {
			input: "explain replace into u (a, b, c, d) values (1, 2, 3, 4), (5, 6, 7, 8)",
		}, {
			input: "explain delete from a where a != 0 order by b limit 1",
		}, {
			input: "explain select a from a union select b from b",
		}, {
			input: "explain select a from a intersect select b from b",
		}, {
			input: "explain select a from a except select b from b",
		}, {
			input: "explain select a from a minus select b from b",
		}, {
			input: "explain select a from a",
		}, {
			input:  "explain (format text) select a from A",
			output: "explain (format text) select a from a",
		}, {
			input:  "explain analyze select * from t",
			output: "explain (analyze) select * from t",
		}, {
			input:  "explain format = 'tree' for connection 10",
			output: "explain format = tree for connection 10",
		}, {
			input:  "explain db.a",
			output: "show columns from db.a",
		}, {
			input:  "explain a",
			output: "show columns from a",
		}, {
			input: "show index from t where true",
		}, {
			input:  "show databases like 'a%'",
			output: "show databases like a%",
		}, {
			input: "show global status where 1 + 21 > 21",
		}, {
			input: "show global variables",
		}, {
			input: "show warnings",
		}, {
			input: "show errors",
		}, {
			input: "show full processlist",
		}, {
			input: "show processlist",
		}, {
			input:  "show full tables from db1 like 'a%' where a != 0",
			output: "show full tables from db1 like a% where a != 0",
		}, {
			input:  "show open tables from db1 like 'a%' where a != 0",
			output: "show open tables from db1 like a% where a != 0",
		}, {
			input:  "show tables from db1 like 'a%' where a != 0",
			output: "show tables from db1 like a% where a != 0",
		}, {
			input:  "show databases like 'a%' where a != 0",
			output: "show databases like a% where a != 0",
		}, {
			input: "show databases",
		}, {
			input:  "show extended full columns from t from db like 'a%'",
			output: "show extended full columns from t from db like a%",
		}, {
			input: "show extended full columns from t from db where a != 0",
		}, {
			input: "show columns from t from db where a != 0",
		}, {
			input: "show columns from t from db",
		}, {
			input: "show create database if not exists db",
		}, {
			input: "show create database db",
		}, {
			input: "show create table db.t1",
		}, {
			input: "show create table t1",
		}, {
			input: "drop user if exists u1, u2, u3",
		}, {
			input: "drop user u1",
		}, {
			input: "drop role r1",
		}, {
			input: "drop role if exists r1, r2, r3",
		}, {
			input: "drop index if exists idx1 on db.t",
		}, {
			input: "drop index idx1 on db.t",
		}, {
			input: "drop table if exists t1, t2, db.t",
		}, {
			input: "drop table db.t",
		}, {
			input: "drop table if exists t",
		}, {
			input: "drop database if exists t",
		}, {
			input: "drop database t",
		}, {
			input:  "create role if not exists 'a', 'b'",
			output: "create role if not exists a, b",
		}, {
			input:  "create role if not exists 'webapp'",
			output: "create role if not exists webapp",
		}, {
			input:  "create role 'admin', 'developer'",
			output: "create role admin, developer",
		}, {
			input:  "create index idx1 on a (a) KEY_BLOCK_SIZE 10 with parser x comment 'x' invisible",
			output: "create index idx1 on a (a) KEY_BLOCK_SIZE 10 with parser x comment x invisible",
		}, {
			input:  "create index idx1 using btree on A (a) KEY_BLOCK_SIZE 10 with parser x comment 'x' invisible",
			output: "create index idx1 using btree on a (a) KEY_BLOCK_SIZE 10 with parser x comment x invisible",
		}, {
			input:  "create index idx using ivfflat on A (a) LISTS 10",
			output: "create index idx using ivfflat on a (a) LISTS 10 ",
		}, {
			input:  "create index idx using ivfflat on A (a) LISTS 10 op_type 'vector_l2_ops'",
			output: "create index idx using ivfflat on a (a) LISTS 10 OP_TYPE vector_l2_ops ",
		}, {
			input: "create index idx1 on a (a)",
		}, {
			input:  "create index idx using master on A (a,b,c)",
			output: "create index idx using master on a (a, b, c)",
		}, {
			input: "create unique index idx1 using btree on a (a, b(10), (a + b), (a - b)) visible",
		}, {
			input:  "create database test_db default collate 'utf8mb4_general_ci' collate utf8mb4_general_ci",
			output: "create database test_db default collate utf8mb4_general_ci collate utf8mb4_general_ci",
		}, {
			input: "create database if not exists test_db character set geostd8",
		}, {
			input: "create database test_db default collate utf8mb4_general_ci",
		}, {
			input: "create database if not exists db",
		}, {
			input: "create database db",
		}, {
			input: "delete from a as aa",
		}, {
			input: "delete from t where a > 1 order by b limit 1 offset 2",
		}, {
			input: "delete from t where a = 1",
		}, {
			input: "insert into u partition(p1, p2) (a, b, c, d) values (1, 2, 3, 4), (5, 6, 1, 0)",
		}, {
			input:  "insert into t values ('aa', 'bb', 'cc')",
			output: "insert into t values (aa, bb, cc)",
		}, {
			input:  "insert into t() values (1, 2, 3)",
			output: "insert into t values (1, 2, 3)",
		}, {
			input: "insert into t (c1, c2, c3) values (1, 2, 3)",
		}, {
			input: "insert into t (c1, c2, c3) select c1, c2, c3 from t1",
		}, {
			input: "insert into t select c1, c2, c3 from t1",
		}, {
			input: "insert into t values (1, 3, 4)",
		}, {
			input: "replace into u partition(p1, p2) (a, b, c, d) values (1, 2, 3, 4), (5, 6, 1, 0)",
		}, {
			input:  "replace into t values ('aa', 'bb', 'cc')",
			output: "replace into t values (aa, bb, cc)",
		}, {
			input:  "replace into t() values (1, 2, 3)",
			output: "replace into t values (1, 2, 3)",
		}, {
			input: "replace into t (c1, c2, c3) values (1, 2, 3)",
		}, {
			input: "replace into t (c1, c2, c3) select c1, c2, c3 from t1",
		}, {
			input: "replace into t select c1, c2, c3 from t1",
		}, {
			input: "replace into t values (1, 3, 4)",
		}, {
			input:  "create table t1 (`show` bool(0));",
			output: "create table t1 (show bool(0))",
		}, {
			input:  "create table t1 (t bool(0));",
			output: "create table t1 (t bool(0))",
		}, {
			input: "create table t1 (t char(0))",
		}, {
			input: "create table t1 (t bool(20), b int, c char(20), d varchar(20))",
		}, {
			input: "create table t (a int(20) not null)",
		}, {
			input: "create table db.t (db.t.a int(20) null)",
		}, {
			input: "create table t (a float(20, 20) not null, b int(20) null, c int(30) null)",
		}, {
			input:  "create table t1 (t time(3) null, dt datetime(6) null, ts timestamp(1) null)",
			output: "create table t1 (t time(3, 3) null, dt datetime(6, 6) null, ts timestamp(1, 1) null)",
		}, {
			input:  "create table t1 (a int default 1 + 1 - 2 * 3 / 4 div 7 ^ 8 << 9 >> 10 % 11)",
			output: "create table t1 (a int default 1 + 1 - 2 * 3 / 4 div 7 ^ 8 << 9 >> 10 % 11)",
		}, {
			input: "create table t1 (t bool default -1 + +1)",
		}, {
			input: "create table t (id int unique key)",
		}, {
			input: "select * from t",
		}, {
			input:  "select c1, c2, c3 from t1, t as t2 where t1.c1 = 1 group by c2 having c2 > 10",
			output: "select c1, c2, c3 from t1 cross join t as t2 where t1.c1 = 1 group by c2 having c2 > 10",
		}, {
			input: "select a from t order by a desc limit 1 offset 2",
		}, {
			input:  "select a from t order by a desc limit 1, 2",
			output: "select a from t order by a desc limit 2 offset 1",
		}, {
			input: "select * from t union select c from t1",
		}, {
			input: "select * from t union all select c from t1",
		}, {
			input: "select * from t union distinct select c from t1",
		}, {
			input: "select * from t except select c from t1",
		}, {
			input: "select * from t except all select c from t1",
		}, {
			input: "select * from t except distinct select c from t1",
		}, {
			input: "select * from t intersect select c from t1",
		}, {
			input: "select * from t intersect all select c from t1",
		}, {
			input: "select * from t intersect distinct select c from t1",
		}, {
			input: "select * from t minus all select c from t1",
		}, {
			input: "select * from t minus distinct select c from t1",
		}, {
			input: "select * from t minus select c from t1",
		}, {
			input: "select * from (select a from t) as t1",
		}, {
			input:  "select * from (select a from t) as t1 join t2 on 1",
			output: "select * from (select a from t) as t1 inner join t2 on 1",
		}, {
			input: "select * from (select a from t) as t1 inner join t2 using (a)",
		}, {
			input: "select * from (select a from t) as t1 cross join t2",
		}, {
			input:  "select * from t1 join t2 using (a, b, c)",
			output: "select * from t1 inner join t2 using (a, b, c)",
		}, {
			input: "select * from t1 straight_join t2 on 1 + 213",
		}, {
			input: "select * from t1 straight_join t2 on col",
		}, {
			input:  "select * from t1 right outer join t2 on 123",
			output: "select * from t1 right join t2 on 123",
		}, {
			input: "select * from t1 natural left join t2",
		}, {
			input: "select 1",
		}, {
			input: "select $ from t",
		}, {
			input:  "analyze table part (a,b )",
			output: "analyze table part(a, b)",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test'",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ','",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'TRUE'",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE'",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE' MAX_FILE_SIZE 100",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false max_file_size 102400",
		}, {
			input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE' MAX_FILE_SIZE 100 FORCE_QUOTE (a, b)",
			output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false max_file_size 102400 force_quote a, b",
		}, {
			input: "drop prepare stmt_name1",
		}, {
			input: "deallocate prepare stmt_name1",
		}, {
			input: "execute stmt_name1",
		}, {
			input: "execute stmt_name1 using @var_name,@@sys_name",
		}, {
			input: "prepare stmt_name1 from select * from t1",
		}, {
			input:  "prepare stmt_name1 from 'select * from t1'",
			output: "prepare stmt_name1 from select * from t1",
		}, {
			input: "prepare stmt_name1 from select * from t1 where a > ? or abs(b) < ?",
		}, {
			input:  "create account if not exists nihao admin_name 'admin' identified by '123' open comment 'new account'",
			output: "create account if not exists nihao admin_name 'admin' identified by '******' open comment 'new account'",
		}, {
			input: "create account if not exists nihao admin_name 'admin' identified by random password",
		}, {
			input:  "create account if not exists nihao admin_name 'admin' identified with '123'",
			output: "create account if not exists nihao admin_name 'admin' identified with '******'",
		}, {
			input:  "create account nihao admin_name 'admin' identified by '123' open comment 'new account'",
			output: "create account nihao admin_name 'admin' identified by '******' open comment 'new account'",
		}, {
			input: "create account nihao admin_name 'admin' identified by random password",
		}, {
			input:  "create account nihao admin_name 'admin' identified with '123'",
			output: "create account nihao admin_name 'admin' identified with '******'",
		}, {
			input:  "create account ? admin_name 'admin' identified with '123'",
			output: "create account ? admin_name 'admin' identified with '******'",
		}, {
			input: "create account nihao admin_name ? identified by ?",
		}, {
			input: "drop account if exists abc",
		}, {
			input: "drop account ?",
		}, {
			input:  "alter account if exists nihao admin_name 'admin' identified by '123' open comment 'new account'",
			output: "alter account if exists nihao admin_name 'admin' identified by '******' open comment 'new account'",
		}, {
			input: "alter account if exists nihao admin_name 'admin' identified by random password",
		}, {
			input:  "alter account if exists nihao admin_name 'admin' identified with '123'",
			output: "alter account if exists nihao admin_name 'admin' identified with '******'",
		}, {
			input:  "alter account nihao admin_name 'admin' identified by '123' open comment 'new account'",
			output: "alter account nihao admin_name 'admin' identified by '******' open comment 'new account'",
		}, {
			input: "alter account nihao admin_name 'admin' identified by random password",
		}, {
			input:  "alter account nihao admin_name 'admin' identified with '123'",
			output: "alter account nihao admin_name 'admin' identified with '******'",
		}, {
			input: "alter account ? admin_name ? identified with ?",
		}, {
			input: "create user if not exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '111' default role def_role " +
				"password expire " +
				"comment 'new comment'",
			output: "create user if not exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******' default role def_role " +
				"password expire " +
				"comment 'new comment'",
		}, {
			input: "create user if not exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '111' default role de_role " +
				"lock " +
				"attribute 'new attribute'",
			output: "create user if not exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******' default role de_role " +
				"lock " +
				"attribute 'new attribute'",
		}, {
			input: "create user if not exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '111', " +
				"abc4 identified by random password, " +
				"abc5 identified with '345' " +
				"default role de_role " +
				"attribute 'new attribute'",
			output: "create user if not exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******', " +
				"abc4 identified by random password, " +
				"abc5 identified with '******' " +
				"default role de_role " +
				"attribute 'new attribute'",
		}, {
			input: "create user if not exists abc1 identified by '111' " +
				"default role de_role " +
				"comment 'new comment'",
			output: "create user if not exists abc1 identified by '******' " +
				"default role de_role " +
				"comment 'new comment'",
		}, {
			input: "create user if not exists abc1 identified by '111' " +
				"default role de_role",
			output: "create user if not exists abc1 identified by '******' " +
				"default role de_role",
		}, {
			input: "create user if not exists abc1 identified by '123' " +
				"default role de_role",
			output: "create user if not exists abc1 identified by '******' " +
				"default role de_role",
		}, {
			input: "create user if not exists abc1 identified by '123' " +
				"default role de_role",
			output: "create user if not exists abc1 identified by '******' " +
				"default role de_role",
		}, {
			input: "create user abc1 identified by '123' " +
				"default role de_role",
			output: "create user abc1 identified by '******' " +
				"default role de_role",
		}, {
			input: "create user abc1 identified by '111' " +
				"default role de_role",
			output: "create user abc1 identified by '******' " +
				"default role de_role",
		}, {
			input:  "create user abc1 identified by 'a111'",
			output: "create user abc1 identified by '******'",
		}, {
			input: "drop user if exists abc1, abc2, abc3",
		}, {
			input: "drop user abc1, abc2, abc3",
		}, {
			input: "drop user abc1",
		}, {
			input: "alter user if exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '123' " +
				"default role de_role " +
				"lock " +
				"comment 'new comment'",
			output: "alter user if exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******' " +
				"default role de_role " +
				"lock " +
				"comment 'new comment'",
		}, {
			input: "alter user if exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '123' " +
				"default role de_role " +
				"unlock " +
				"comment 'new comment'",
			output: "alter user if exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******' " +
				"default role de_role " +
				"unlock " +
				"comment 'new comment'",
		}, {
			input: "alter user if exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '123' " +
				"default role de_role " +
				"password expire " +
				"attribute 'new attribute'",
			output: "alter user if exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******' " +
				"default role de_role " +
				"password expire " +
				"attribute 'new attribute'",
		}, {
			input: "alter user if exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '123' " +
				"attribute 'new attribute'",
			output: "alter user if exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******' " +
				"attribute 'new attribute'",
		}, {
			input:  "alter user if exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '123'",
			output: "alter user if exists abc1 identified by '******', abc2 identified by '******', abc3 identified by '******'",
		}, {
			input:  "alter user if exists abc1 identified by '123', abc2 identified with '234', abc3 identified with 'SSL'",
			output: "alter user if exists abc1 identified by '******', abc2 identified with '******', abc3 identified with '******'",
		}, {
			input:  "alter user if exists abc1 identified by '123'",
			output: "alter user if exists abc1 identified by '******'",
		}, {
			input:  "alter user if exists abc1 identified by '123'",
			output: "alter user if exists abc1 identified by '******'",
		}, {
			input:  "alter user abc1 identified by '123'",
			output: "alter user abc1 identified by '******'",
		}, {
			input: "create role if not exists role1, role2, role2",
		}, {
			input: "create role role1",
		}, {
			input: "drop role if exists role1, role2, role2",
		}, {
			input: "drop role if exists role1",
		}, {
			input: "drop role role1",
		}, {
			input: "grant all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.a to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table *.* to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table db.a to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table db.* to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on database * to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table *.* to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table db1.* to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table db1.tb1 to u1, u2 with grant option",
		}, {
			input: "grant all, all(a, b) on table tb1 to u1, u2 with grant option",
		}, {
			input: "grant r1, r2 to u1, u2, r3 with grant option",
		}, {
			input: "grant r1, r2 to u1, u2, r3",
		}, {
			input: "grant r1, r2 to u1@h1, u2@h2, r3",
		}, {
			input:  "revoke if exists all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.A from u1, u2",
			output: "revoke if exists all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.a from u1, u2",
		}, {
			input: "revoke if exists r1, r2, r3 from u1, u2, u3",
		}, {
			input: "revoke if exists super(a, b, c) on procedure db.func from h3",
		}, {
			input:  "revoke if exists all on table db.A from u1, u2",
			output: "revoke if exists all on table db.a from u1, u2",
		}, {
			input: "revoke if exists all on table db.a from u1",
		}, {
			input: "use db1",
		}, {
			input: "set role r1",
		}, {
			input: "set secondary role all",
		}, {
			input: "set secondary role none",
		}, {
			input:  `select json_extract('{"a":1,"b":2}', '$.b')`,
			output: `select json_extract({"a":1,"b":2}, $.b)`,
		}, {
			input:  `select json_extract(a, '$.b') from t`,
			output: `select json_extract(a, $.b) from t`,
		}, {
			input: `create table t1 (a int, b uuid)`,
		}, {
			input: `create table t2 (a uuid primary key, b varchar(10))`,
		}, {
			input: `create table t3 (a int, b uuid, primary key idx (a, b))`,
		}, {
			input:  `DO SLEEP(5)`,
			output: `do SLEEP(5)`,
		}, {
			input:  `DECLARE a, b INT`,
			output: `declare a b int default null`,
		}, {
			input:  `DECLARE a, b INT DEFAULT 1`,
			output: `declare a b int default 1`,
		}, {
			input: "grant truncate on table *.* to r1",
		}, {
			input: "grant reference on table *.* to r1",
		},
		{
			input:  `VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8)`,
			output: `values row(1, -2, 3), row(5, 7, 9), row(4, 6, 8)`,
		}, {
			input:  `VALUES ROW(5,7,9), ROW(1,2,3), ROW(9,10,11) ORDER BY column_1`,
			output: `values row(5, 7, 9), row(1, 2, 3), row(9, 10, 11) order by column_1`,
		},
		{
			input:  `VALUES ROW(5,7,9), ROW(1,2,3), ROW(9,10,11) ORDER BY column_1 LIMIT 2`,
			output: `values row(5, 7, 9), row(1, 2, 3), row(9, 10, 11) order by column_1 limit 2`,
		},
		{
			input:  `select * from unnest("a") as f`,
			output: `select * from unnest(a) as f`,
		},
		{
			input:  `select * from unnest("a", "b") as f`,
			output: `select * from unnest(a, b) as f`,
		},
		{
			input:  `select * from unnest("a", "b", true) as f`,
			output: `select * from unnest(a, b, true) as f`,
		},
		{
			input:  `select * from unnest("a")`,
			output: `select * from unnest(a)`,
		},
		{
			input:  `select * from unnest("a", "b")`,
			output: `select * from unnest(a, b)`,
		},
		{
			input:  `select * from unnest("a", "b", true)`,
			output: `select * from unnest(a, b, true)`,
		},
		{
			input:  `select * from unnest(t.a)`,
			output: `select * from unnest(t.a)`,
		},
		{
			input:  `select * from unnest(t.a, "$.b")`,
			output: `select * from unnest(t.a, $.b)`,
		},
		{
			input:  `select * from unnest(t.a, "$.b", true)`,
			output: `select * from unnest(t.a, $.b, true)`,
		},
		{
			input:  `select * from unnest(t.a) as f`,
			output: `select * from unnest(t.a) as f`,
		},
		{
			input:  `select * from unnest(t.a, "$.b") as f`,
			output: `select * from unnest(t.a, $.b) as f`,
		},
		{
			input:  `select * from unnest(t.a, "$.b", true) as f`,
			output: `select * from unnest(t.a, $.b, true) as f`,
		},
		{
			input:  `select * from generate_series('1', '10', '1')`,
			output: `select * from generate_series(1, 10, 1)`,
		},
		{
			input:  `select * from generate_series('1', '10', '1') g`,
			output: `select * from generate_series(1, 10, 1) as g`,
		},
		{
			input:  `select * from generate_series(1, 10, 1)`,
			output: `select * from generate_series(1, 10, 1)`,
		},
		{
			input:  `select * from generate_series(1, 10, 1) as g`,
			output: `select * from generate_series(1, 10, 1) as g`,
		},
		{
			input:  `create table t1 (a int low_cardinality, b int not null low_cardinality)`,
			output: `create table t1 (a int low_cardinality, b int not null low_cardinality)`,
		},
		{
			input:  `select mo_show_visible_bin('a',0) as m`,
			output: `select mo_show_visible_bin(a, 0) as m`,
		},
		//https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
		{
			input:  `select avg(a) over () from t1`,
			output: "select avg(a) over () from t1",
		},
		{
			input:  `select avg(a) over (partition by col1, col2) from t1`,
			output: "select avg(a) over (partition by col1, col2) from t1",
		},
		{
			input:  `select avg(a) over (partition by col1, col2 order by col3 desc) from t1`,
			output: "select avg(a) over (partition by col1, col2 order by col3 desc) from t1",
		},
		//https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
		{
			input:  `select count(a) over (partition by col1, col2 order by col3 desc rows 1 preceding) from t1`,
			output: "select count(a) over (partition by col1, col2 order by col3 desc rows 1 preceding) from t1",
		},
		{
			input: `select sum(a) over (partition by col1, col2 order by col3 desc rows between 1 preceding and 20 following) from t1`,
		},
		{
			input:  `select count(a) over (partition by col1, col2 order by col3 desc range unbounded preceding) from t1`,
			output: "select count(a) over (partition by col1, col2 order by col3 desc range unbounded preceding) from t1",
		},
		{
			input: "alter account if exists abc",
		},
		{
			input:  "alter account if exists abc admin_name 'root' identified by '111' open comment 'str'",
			output: "alter account if exists abc admin_name 'root' identified by '******' open comment 'str'",
		},
		{
			input: "alter account if exists abc open comment 'str'",
		},
		{
			input: "alter account if exists abc comment 'str'",
		},
		{
			input: "alter account if exists abc open",
		},
		{
			input:  "alter account if exists abc restricted",
			output: "alter account if exists abc restricted",
		},
		{
			input:  "alter account if exists abc admin_name 'root' identified by '111' open",
			output: "alter account if exists abc admin_name 'root' identified by '******' open",
		},
		{
			input:  "alter account if exists abc admin_name 'root' identified by '111' comment 'str'",
			output: "alter account if exists abc admin_name 'root' identified by '******' comment 'str'",
		},
		{
			input: `create cluster table a (a int)`,
		},
		{
			input: `insert into a values (1, 2), (1, 2)`,
		},
		{
			input: `insert into a select a, b from a`,
		},
		{
			input: `insert into a (a, b) values (1, 2), (1, 2)`,
		},
		{
			input:  `insert into a () values (1, 2), (1, 2)`,
			output: `insert into a values (1, 2), (1, 2)`,
		},
		{
			input: `insert into a (a, b) select a, b from a`,
		},
		{
			input:  `insert into a set a = b, b = b + 1`,
			output: `insert into a (a, b) values (b, b + 1)`,
		},
		{
			input:  "load data infile 'test/loadfile5' ignore INTO TABLE T.A FIELDS TERMINATED BY  ',' (@,@,c,d,e,f)",
			output: "load data infile test/loadfile5 ignore into table t.a fields terminated by , (, , c, d, e, f)",
		},
		{
			input:  "load data infile 'data.txt' into table db.a fields terminated by '\t' escaped by '\t'",
			output: "load data infile data.txt into table db.a fields terminated by \t escaped by \t",
		},
		{
			input:  `create function helloworld () returns int language sql as 'select id from test_table limit 1'`,
			output: `create function helloworld () returns int language sql as 'select id from test_table limit 1'`,
		},
		{
			input:  `create function twosum (x int, y int) returns int language sql as 'select $1 + $2'`,
			output: `create function twosum (x int, y int) returns int language sql as 'select $1 + $2'`,
		},
		{
			input:  `create function charat (x int) returns char language sql as 'select $1'`,
			output: `create function charat (x int) returns char language sql as 'select $1'`,
		},
		{
			input:  `create function charat (x int default 15) returns char language sql as 'select $1'`,
			output: `create function charat (x int default 15) returns char language sql as 'select $1'`,
		},
		{
			input:  `create function t.increment (x float) returns float language sql as 'select $1 + 1'`,
			output: `create function t.increment (x float) returns float language sql as 'select $1 + 1'`,
		},
		{
			input:  `drop function helloworld ()`,
			output: `drop function helloworld ()`,
		},
		{
			input:  `drop function charat (int)`,
			output: `drop function charat (int)`,
		},
		{
			input:  `drop function twosum (int, int)`,
			output: `drop function twosum (int, int)`,
		},
		{
			input:  `drop function t.increment (float)`,
			output: `drop function t.increment (float)`,
		},
		{
			input:  `create extension python as strutil file 'stringutils.whl'`,
			output: `create extension python as strutil file stringutils.whl`,
		},
		{
			input:  `load strutil`,
			output: `load strutil`,
		},
		{
			input: `select * from (values row(1, 2), row(3, 3)) as a`,
		},
		{
			input: `select t1.* from (values row(1, 1), row(3, 3)) as a(c1, c2) inner join t1 on a.c1 = t1.b`,
		},
		{
			input:  "modump query_result '0adaxg' into '/Users/tmp/test'",
			output: "modump query_result 0adaxg into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			input:  `modump query_result "queryId" into '/Users/tmp/test' FIELDS TERMINATED BY ','`,
			output: "modump query_result queryId into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			input:  "modump query_result 'abcx' into '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'",
			output: "modump query_result abcx into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			input:  "modump query_result '098e32' into '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'TRUE'",
			output: "modump query_result 098e32 into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
		},
		{
			input:  "modump query_result '09eqr' into '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE'",
			output: "modump query_result 09eqr into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false",
		},
		{
			input:  "modump query_result 'd097i7' into '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE' MAX_FILE_SIZE 100",
			output: "modump query_result d097i7 into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false max_file_size 102400",
		},
		{
			input:  "modump query_result '09eqrteq' into '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE' MAX_FILE_SIZE 100 FORCE_QUOTE (a, b)",
			output: "modump query_result 09eqrteq into /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false max_file_size 102400 force_quote a, b",
		},
		{
			input: "show accounts",
		},
		{
			input:  "show accounts like '%dafgda_'",
			output: "show accounts like %dafgda_",
		},
		{
			input:  "create table test (`col` varchar(255) DEFAULT b'0')",
			output: "create table test (col varchar(255) default 0b0)",
		},
		{
			input:  "select trim(a)",
			output: "select trim(a)",
		},
		{
			input: "select trim(a from a)",
		},
		{
			input: "select trim(leading a from b)",
		},
		{
			input: "select trim(trailing b from a)",
		},
		{
			input: "select trim(both a from b) from t",
		},
		{
			input:  "LOCK TABLES t READ",
			output: "Lock Tables t READ",
		},
		{
			input:  "LOCK TABLES t READ LOCAL",
			output: "Lock Tables t READ LOCAL",
		},
		{
			input:  "LOCK TABLES t WRITE",
			output: "Lock Tables t WRITE",
		},
		{
			input:  "LOCK TABLES t LOW_PRIORITY WRITE",
			output: "Lock Tables t LOW_PRIORITY WRITE",
		},
		{
			input:  "LOCK TABLES t LOW_PRIORITY WRITE, t1 READ, t2 WRITE",
			output: "Lock Tables t LOW_PRIORITY WRITE, t1 READ, t2 WRITE",
		},
		{
			input:  "UNLOCK TABLES",
			output: "UnLock Tables",
		},
		{
			input:  "create sequence s as smallint unsigned increment by 1 minvalue -100 maxvalue 100 start with -90 cycle",
			output: "create sequence s as smallint unsigned increment by 1 minvalue -100 maxvalue 100 start with -90 cycle",
		},
		{
			input:  "ALTER SEQUENCE my_sequence START WITH 100;",
			output: "alter sequence my_sequence start with 100 ",
		},
		{
			input:  "ALTER SEQUENCE my_sequence INCREMENT BY 5;",
			output: "alter sequence my_sequence increment by 5 ",
		},
		{
			input:  "ALTER SEQUENCE my_sequence MINVALUE 1 MAXVALUE 1000;",
			output: "alter sequence my_sequence minvalue 1 maxvalue 1000 ",
		},
		{
			input:  "ALTER SEQUENCE my_sequence CYCLE;",
			output: "alter sequence my_sequence cycle",
		},
		{
			input:  "alter table t1 modify column b int",
			output: "alter table t1 modify column b int",
		},
		{
			input:  "alter table t1 modify column b VARCHAR(20) first",
			output: "alter table t1 modify column b varchar(20) first",
		},
		{
			input:  "alter table t1 modify column b VARCHAR(20) after a",
			output: "alter table t1 modify column b varchar(20) after a",
		},
		{
			input:  "alter table t1 modify b VARCHAR(20) after a",
			output: "alter table t1 modify column b varchar(20) after a",
		},
		{
			input:  "alter table t1 change column a b int",
			output: "alter table t1 change column a b int",
		},
		{
			input:  "alter table t1 change column a b int first",
			output: "alter table t1 change column a b int first",
		},
		{
			input:  "alter table t1 change a x varchar(20) after b",
			output: "alter table t1 change column a x varchar(20) after b",
		},
		{
			input:  "alter table t1 change column a x varchar(20) after b",
			output: "alter table t1 change column a x varchar(20) after b",
		},
		{
			input:  "alter table emp rename column deptno to deptid",
			output: "alter table emp rename column deptno to deptid",
		},
		{
			input:  "alter table t1 alter a set default 100",
			output: "alter table t1 alter column a set default 100",
		},
		{
			input:  "alter table t1 alter column a drop default",
			output: "alter table t1 alter column a drop default",
		},
		{
			input:  "alter table t1 alter column b set visible",
			output: "alter table t1 alter column b set visible",
		},
		{
			input:  "alter table t1 order by a ASC, b DESC",
			output: "alter table t1 order by a asc, b desc",
		},
		{
			input:  "alter table t1 order by a, b DESC",
			output: "alter table t1 order by a, b desc",
		},
		{
			input: "alter table tbl1 drop column col1",
		},
		{
			input: "alter table tbl1 drop column col1, drop column col2",
		},
		{
			input: "alter table tbl1 drop index idx_name",
		},
		{
			input:  "alter table tbl1 drop index idx_name, drop key idx_name, drop column col1, drop primary key, comment = 'aa'",
			output: "alter table tbl1 drop index idx_name, drop key idx_name, drop column col1, drop primary key, comment = 'aa'",
		},
		{
			input: "alter table tbl1 drop key idx_name",
		},
		{
			input: "alter table tbl1 drop primary key",
		},
		{
			input: "alter table tbl1 drop foreign key fk_name",
		},
		{
			input: "alter table tbl1 add foreign key sdf (a, b) references b(a asc, b desc)",
		},
		{
			input:  "alter table tbl1 checksum = 0, COMMENT = 'asdf'",
			output: "alter table tbl1 checksum = 0, comment = 'asdf'",
		},
		{
			input:  "alter table t1 alter index c visible",
			output: "alter table t1 alter index c visible",
		},
		{
			input:  "alter table t1 alter index c invisible",
			output: "alter table t1 alter index c invisible",
		},
		{
			input:  "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 unique key (col3)",
			output: "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 unique key (col3)",
		},
		{
			input:  "alter table t1 add constraint unique key (col3, col4)",
			output: "alter table t1 add unique key (col3, col4)",
		},
		{
			input:  "alter table t1 add constraint unique key zxxxxxx (col3, col4)",
			output: "alter table t1 add unique key zxxxxxx (col3, col4)",
		},
		{
			input:  "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 unique key zxxxxx (col3)",
			output: "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 unique key zxxxxx (col3)",
		},
		{
			input:  "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 unique key (col3)",
			output: "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 unique key (col3)",
		},
		{
			input:  "alter table t1 add constraint fk_6dotkott2kjsp8vw4d0m25fb7 foreign key fk1 (col4) references dept(deptno)",
			output: "alter table t1 add constraint fk_6dotkott2kjsp8vw4d0m25fb7 foreign key fk1 (col4) references dept(deptno)",
		},
		{
			input:  "alter table t1 add constraint fk_6dotkott2kjsp8vw4d0m25fb7 foreign key (col4) references dept(deptno)",
			output: "alter table t1 add constraint fk_6dotkott2kjsp8vw4d0m25fb7 foreign key (col4) references dept(deptno)",
		},
		{
			input:  "alter table t1 add constraint foreign key fk1 (col4) references dept(deptno)",
			output: "alter table t1 add foreign key fk1 (col4) references dept(deptno)",
		},
		{
			input:  "alter table t1 add constraint foreign key (col4) references dept(deptno)",
			output: "alter table t1 add foreign key (col4) references dept(deptno)",
		},
		{
			input:  "alter table t1 add constraint pk primary key pk1 (col1, col4)",
			output: "alter table t1 add constraint pk primary key pk1 (col1, col4)",
		},
		{
			input:  "alter table t1 add constraint pk primary key (col4)",
			output: "alter table t1 add constraint pk primary key (col4)",
		},
		{
			input:  "alter table t1 add constraint pk primary key (col1, col4)",
			output: "alter table t1 add constraint pk primary key (col1, col4)",
		},
		{
			input:  "alter table t1 add primary key (col1, col4)",
			output: "alter table t1 add primary key (col1, col4)",
		},
		{
			input:  "alter table t1 add primary key pk1 (col1, col4)",
			output: "alter table t1 add primary key pk1 (col1, col4)",
		},
		{
			input:  "alter table t1 comment 'abc'",
			output: "alter table t1 comment = 'abc'",
		},
		{
			input: "alter table t1 rename to t2",
		},
		{
			input: "alter table t1 add column a int, add column b int",
		},
		{
			input: "alter table t1 drop column a, drop column b",
		},
		{
			input:  "ALTER TABLE employees ADD PARTITION (PARTITION p05 VALUES LESS THAN (500001))",
			output: "alter table employees add partition (partition p05 values less than (500001))",
		},
		{
			input:  "alter table t add partition (partition p4 values in (7), partition p5 values in (8, 9))",
			output: "alter table t add partition (partition p4 values in (7), partition p5 values in (8, 9))",
		},
		{
			input:  "ALTER TABLE t1 DROP PARTITION p1",
			output: "alter table t1 drop partition p1",
		},
		{
			input:  "ALTER TABLE t1 DROP PARTITION p0, p1",
			output: "alter table t1 drop partition p0, p1",
		},
		{
			input:  "ALTER TABLE t1 TRUNCATE PARTITION p0",
			output: "alter table t1 truncate partition p0",
		},
		{
			input:  "ALTER TABLE t1 TRUNCATE PARTITION p0, p3",
			output: "alter table t1 truncate partition p0, p3",
		},
		{
			input:  "ALTER TABLE t1 TRUNCATE PARTITION ALL",
			output: "alter table t1 truncate partition all",
		},
		{
			input:  "ALTER TABLE titles partition by range(to_days(from_date)) (partition p01 values less than (to_days('1985-12-31')), partition p02 values less than (to_days('1986-12-31')), partition p03 values less than (to_days('1987-12-31')))",
			output: "alter table titles partition by range(to_days(from_date)) (partition p01 values less than (to_days(1985-12-31)), partition p02 values less than (to_days(1986-12-31)), partition p03 values less than (to_days(1987-12-31)))",
		},
		{
			input:  "create table pt2 (id int, date_column date) partition by range(year(date_column)) (partition p1 values less than (2010) comment 'p1 comment', partition p2 values less than maxvalue comment 'p3 comment')",
			output: "create table pt2 (id int, date_column date) partition by range(year(date_column)) (partition p1 values less than (2010) comment = 'p1 comment', partition p2 values less than (MAXVALUE) comment = 'p3 comment')",
		},
		{
			input: "create publication pub1 database db1",
		},
		{
			input: "create publication pub1 database db1 account acc0",
		},
		{
			input: "create publication pub1 database db1 account acc0, acc1",
		},
		{
			input: "create publication pub1 database db1 account acc0, acc1, acc2 comment 'test'",
		},
		{
			input: "create publication pub1 database db1 comment 'test'",
		},
		{
			input: "create publication pub1 table t1",
		},
		{
			input: "create publication pub1 table t1 account acc0",
		},
		{
			input: "create publication pub1 table t1 account acc0, acc1",
		},
		{
			input: "create publication pub1 table t1 account acc0, acc1, acc2 comment 'test'",
		},
		{
			input: "create publication pub1 table t1 comment 'test'",
		},
		{
			input:  "CREATE STAGE my_ext_stage URL='s3://load/files/'",
			output: "create stage my_ext_stage url='s3://load/files/'",
		},
		{
			input:  "CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};",
			output: "create stage my_ext_stage1 url='s3://load/files/' crentiasl={'AWS_KEY_ID'='1a2b3c','AWS_SECRET_KEY'='4x5y6z'}",
		},
		{
			input:  "CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c', 'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;",
			output: "create stage my_ext_stage1 url='s3://load/files/' crentiasl={'AWS_KEY_ID'='1a2b3c','AWS_SECRET_KEY'='4x5y6z'} enabled",
		},
		{
			input:  "DROP STAGE my_ext_stage1",
			output: "drop stage my_ext_stage1",
		},
		{
			input:  "DROP STAGE if exists my_ext_stage1",
			output: "drop stage if not exists my_ext_stage1",
		},
		{
			input:  "ALTER STAGE my_ext_stage SET URL='s3://loading/files/new/'",
			output: "alter stage my_ext_stage set  url='s3://loading/files/new/'",
		},
		{
			input:  "ALTER STAGE my_ext_stage SET CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};",
			output: "alter stage my_ext_stage set  crentiasl={'AWS_KEY_ID'='1a2b3c','AWS_SECRET_KEY'='4x5y6z'}",
		},
		{
			input:  "SHOW STAGES LIKE 'my_stage'",
			output: "show stages like my_stage",
		},
		{
			input: "create database db1 from acc0 publication pub1",
		},
		{
			input: "create table t1 from acc0 publication pub1",
		},
		{
			input: "create temporary table t1 from acc0 publication pub1",
		},
		{
			input: "create table if not exists t1 from acc0 publication pub1",
		},
		{
			input: "create temporary table if not exists t1 from acc0 publication pub1",
		},
		{
			input: "drop publication pub1",
		},
		{
			input: "drop publication if exists pub1",
		},
		{
			input: "alter publication pub1 account all",
		},
		{
			input: "alter publication pub1 account acc0",
		},
		{
			input: "alter publication pub1 account acc0, acc1",
		},
		{
			input: "alter publication pub1 account add acc0",
		},
		{
			input: "restore cluster from snapshot snapshot_01",
		},
		{
			input: "restore account account_01 from snapshot snapshot_01",
		},
		{
			input: "restore account account_01 database db1 from snapshot snapshot_01",
		},
		{
			input: "restore account account_01 database db1 table t1 from snapshot snapshot_01",
		},
		{
			input:  "restore account account_01 from snapshot snapshot_01 to account account_02",
			output: "restore account account_01 from snapshot snapshot_01 to account account_02",
		},
		{
			input: `create cdc 'test_create_task' 'mysql://dump:111@127.0.0.1:6001' 'mysql' 'mysql://root:123456@127.0.0.1:3306' 'a,b' { "StartTS"='',"EndTS"='',"NoFull"='false',"FullConcurrency"='16',"IncrementalConcurrency"='16',"ConfigFile"='',"FullTaskRetry"='',"IncrementalTaskRetry"='',"FullDDLRetry"='0',"FullDMLRetry"='0',"IncrementalDDLRetry"='0',"IncrementalDMLRetry"='0',};`,
		},
		{
			input: `show cdc 'mysql://dump:111@127.0.0.1:6001' all;`,
		},
		{
			input: `show cdc 'mysql://dump:111@127.0.0.1:6001' task 't1';`,
		},
		{
			input: `drop cdc 'mysql://dump:111@127.0.0.1:6001' all;`,
		},
		{
			input: `drop cdc 'mysql://dump:111@127.0.0.1:6001' task 't1';`,
		},
		{
			input: `pause cdc 'mysql://dump:111@127.0.0.1:6001' all;`,
		},
		{
			input: `pause cdc 'mysql://dump:111@127.0.0.1:6001' task 't1';`,
		},
		{
			input: `resume cdc 'mysql://dump:111@127.0.0.1:6001' task 't1';`,
		},
		{
			input: `resume cdc 'mysql://dump:111@127.0.0.1:6001' task 't1' 'restart';`,
		},
		{
			input: "alter publication pub1 account add acc0, acc1",
		},
		{
			input: "alter publication pub1 account drop acc0",
		},
		{
			input: "alter publication if exists pub1 account drop acc0, acc1",
		},
		{
			input: "alter publication pub1 account drop acc1 comment 'test'",
		},
		{
			input: "alter publication if exists pub1 account acc1 comment 'test'",
		},
		{
			input: "show create publication pub1",
		},
		{
			input: "show publications",
		},
		{
			input: "show subscriptions",
		},
		{
			input: "show subscriptions all",
		},
		{
			input:  "show subscriptions all like '%pub'",
			output: "show subscriptions all like %pub",
		},
		{
			input:  "insert into tbl values ($$this is a dollar-quoted string$$)",
			output: "insert into tbl values (this is a dollar-quoted string)",
		},
		{
			input:  "select $tag$this is a dollar-quoted string$tag$",
			output: "select this is a dollar-quoted string",
		},
		{
			input:  "select $1 + $q$\\n\\t\\r\\b\\0\\_\\%\\\\$q$",
			output: "select $1 + \\n\\t\\r\\b\\0\\_\\%\\\\",
		},
		{
			input:  "show table_size from test",
			output: "show table size from test",
		},
		{
			input:  "show table_size from mo_role from mo_catalog",
			output: "show table size from mo_role from mo_catalog",
		},
		{
			input:  "show roles",
			output: "show roles",
		},
		{
			input:  "show roles like '%dafgda_'",
			output: "show roles like %dafgda_",
		},
		{
			input:  "create procedure test1 (in param1 int) 'test test'",
			output: "create procedure test1 (in param1 int) 'test test'",
		},
		{
			input:  "create procedure test2 (param1 int, inout param2 char(5)) 'test test'",
			output: "create procedure test2 (in param1 int, inout param2 char(5)) 'test test'",
		},
		{
			input:  "drop procedure test1",
			output: "drop procedure test1",
		},
		{
			input:  "call test1()",
			output: "call test1()",
		},
		{
			input:  "call test1(@session, @increment)",
			output: "call test1(@session, @increment)",
		},
		{
			input:  "select cast(123 as binary)",
			output: "select cast(123 as binary)",
		},
		{
			input:  "select BINARY 124",
			output: "select BINARY(124)",
		},
		{
			input:  "set transaction isolation level read committed;",
			output: "set transaction isolation level read committed",
		},
		{
			input:  "set global transaction isolation level read committed , read write , isolation level read committed , read only;",
			output: "set global transaction isolation level read committed , read write , isolation level read committed , read only",
		},
		{
			input:  "set session transaction isolation level read committed , read write , isolation level read committed , read only;",
			output: "set transaction isolation level read committed , read write , isolation level read committed , read only",
		},
		{
			input:  "set session transaction isolation level read committed , isolation level read uncommitted , isolation level repeatable read , isolation level serializable;",
			output: "set transaction isolation level read committed , isolation level read uncommitted , isolation level repeatable read , isolation level serializable",
		},
		{
			input:  "create table t1(a int) STORAGE DISK;",
			output: "create table t1 (a int) tablespace =  STORAGE DISK",
		}, {
			input:  "create table t1 (a int) STORAGE DISK;",
			output: "create table t1 (a int) tablespace =  STORAGE DISK",
		}, {
			input: "create table t1 (a numeric(10, 2))",
		}, {
			input: "create table t1 (a mediumint)",
		}, {
			input:  "drop schema if exists ssb",
			output: "drop database if exists ssb",
		}, {
			input:  "drop table if exists ssb RESTRICT",
			output: "drop table if exists ssb",
		}, {
			input:  "drop table if exists ssb CASCADE",
			output: "drop table if exists ssb",
		}, {
			input: "create table t1 (a int) AUTOEXTEND_SIZE = 10",
		}, {
			input:  "create table t1 (a int) ENGINE_ATTRIBUTE = 'abc'",
			output: "create table t1 (a int) ENGINE_ATTRIBUTE = abc",
		}, {
			input: "create table t1 (a int) INSERT_METHOD = NO",
		}, {
			input: "create table t1 (a int) INSERT_METHOD = FIRST",
		}, {
			input: "create table t1 (a int) INSERT_METHOD = LAST",
		}, {
			input: "create table t1 (a int) START TRANSACTION",
		}, {
			input:  "create table t1 (a int) SECONDARY_ENGINE_ATTRIBUTE = 'abc'",
			output: "create table t1 (a int) SECONDARY_ENGINE_ATTRIBUTE = abc",
		}, {
			input:  "create table /*! if not exists */ t1 (a int)",
			output: "create table if not exists t1 (a int)",
		}, {
			input:  "create table /*!50100 if not exists */ t1 (a int)",
			output: "create table if not exists t1 (a int)",
		}, {
			input:  "create table /*!50100 if not exists */ t1 (a int) /*!AUTOEXTEND_SIZE = 10*/",
			output: "create table if not exists t1 (a int) AUTOEXTEND_SIZE = 10",
		}, {
			input:  "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */",
			output: "set OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT",
		}, {
			input:  "SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '65c4c218-d343-11eb-8106-525400f4f901:1-769275'",
			output: "set global GTID_PURGED = 65c4c218-d343-11eb-8106-525400f4f901:1-769275",
		}, {
			input:  " /*!40103 SET TIME_ZONE='+00:00' */",
			output: "set time_zone = +00:00",
		}, {
			input:  "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */",
			output: "set OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT",
		}, {
			input:  "/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */",
			output: "set OLD_TIME_ZONE = @@TIME_ZONE",
		}, {
			input:  "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */",
			output: "set OLD_SQL_MODE = @@SQL_MODE, sql_mode = NO_AUTO_VALUE_ON_ZERO",
		}, {
			input:  "/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;",
			output: "set OLD_SQL_NOTES = @@SQL_NOTES, sql_notes = 0",
		}, {
			input:  "SELECT /*+ RESOURCE_GROUP(resouce_group_name) */ * from table_name;",
			output: "select * from table_name",
		}, {
			input:  "SELECT /*+ qb_name(viewSub, v@sel_1 . @sel_2) use_index(e3@viewSub, idx) hash_agg(viewSub) */ * FROM v;",
			output: "select * from v",
		}, {
			input:  "SELECT * FROM t1 dt WHERE EXISTS( WITH RECURSIVE qn AS (SELECT a AS b UNION ALL SELECT b+1 FROM qn WHERE b=0 or b = 1) SELECT * FROM qn dtqn1 where exists (select /*+ NO_DECORRELATE() */ b from qn where dtqn1.b+1))",
			output: "select * from t1 as dt where exists (with recursive qn as (select a as b union all select b + 1 from qn where b = 0 or b = 1) select * from qn as dtqn1 where exists (select b from qn where dtqn1.b + 1))",
		}, {
			input:  "select /*+use_index(tmp1, code)*/ * from tmp1 where code > 1",
			output: "select * from tmp1 where code > 1",
		}, {
			input:  "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1",
			output: "explain (analyze) select t1.k from t as t1 cross join t as t2 where t1.v = t2.v + 1",
		}, {
			input:  "prepare stmt from 'select /*+ inl_join(t2) */ * from t t1 join t t2 on t1.a = t2.a and t1.c = t2.c where t2.a = 1 or t2.b = 1;';",
			output: "prepare stmt from select /*+ inl_join(t2) */ * from t t1 join t t2 on t1.a = t2.a and t1.c = t2.c where t2.a = 1 or t2.b = 1;",
		}, {
			input:  "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `ucl360_demo_v3` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;",
			output: "create database if not exists ucl360_demo_v3 default character set utf8mb4 collate utf8mb4_0900_ai_ci encryption N",
		}, {
			input:  "alter table t1 algorithm = DEFAULT",
			output: "alter table t1 alter algorithm not enforce",
		}, {
			input:  "alter table t1 algorithm = INSTANT",
			output: "alter table t1 alter algorithm not enforce",
		}, {
			input:  "alter table t1 algorithm = INPLACE",
			output: "alter table t1 alter algorithm not enforce",
		}, {
			input:  "alter table t1 algorithm = COPY",
			output: "alter table t1 alter algorithm not enforce",
		}, {
			input:  "alter table t1 default CHARACTER SET = a COLLATE = b",
			output: "alter table t1 charset = a",
		}, {
			input:  "alter table t1 CONVERT TO CHARACTER SET a COLLATE b",
			output: "alter table t1 charset = a",
		}, {
			input:  "alter table t1 DISABLE KEYS",
			output: "alter table t1 charset = DISABLE",
		}, {
			input:  "alter table t1 ENABLE KEYS",
			output: "alter table t1 charset = ENABLE",
		}, {
			input:  "alter table t1 DISCARD TABLESPACE",
			output: "alter table t1 charset = DISCARD",
		}, {
			input:  "alter table t1 IMPORT TABLESPACE",
			output: "alter table t1 charset = IMPORT",
		}, {
			input:  "alter table t1 FORCE",
			output: "alter table t1 charset = FORCE",
		}, {
			input:  "alter table t1 LOCK = DEFAULT",
			output: "alter table t1 charset = LOCK",
		}, {
			input:  "alter table t1 LOCK = NONE",
			output: "alter table t1 charset = LOCK",
		}, {
			input:  "alter table t1 LOCK = SHARED",
			output: "alter table t1 charset = LOCK",
		}, {
			input:  "alter table t1 LOCK = EXCLUSIVE",
			output: "alter table t1 charset = LOCK",
		}, {
			input:  "alter table t1 WITHOUT VALIDATION",
			output: "alter table t1 charset = WITHOUT",
		}, {
			input:  "alter table t1 WITH VALIDATION",
			output: "alter table t1 charset = WITH",
		}, {
			input:  "alter table t1 alter CHECK a ENFORCED",
			output: "alter table t1 alter CHECK enforce",
		}, {
			input:  "alter table t1 alter CONSTRAINT a NOT ENFORCED",
			output: "alter table t1 alter CONSTRAINT not enforce",
		}, {
			input:  "create SQL SECURITY DEFINER VIEW t2 as select * from t1",
			output: "create view t2 as select * from t1",
		}, {
			input:  "create SQL SECURITY INVOKER VIEW t2 as select * from t1",
			output: "create view t2 as select * from t1",
		}, {
			input:  "create VIEW t2 as select * from t1 WITH CASCADED CHECK OPTION",
			output: "create view t2 as select * from t1",
		}, {
			input:  "create VIEW t2 as select * from t1 WITH LOCAL CHECK OPTION",
			output: "create view t2 as select * from t1",
		}, {
			input:  "insert into t1 values(_binary 0x123)",
			output: "insert into t1 values (123)",
		}, {
			input:  "backup '123' filesystem '/home/abc' parallelism '1'",
			output: "backup 123 filesystem /home/abc parallelism 1",
		}, {
			input:  "backup '125' filesystem '/tmp/backup' parallelism '1';",
			output: "backup 125 filesystem /tmp/backup parallelism 1",
		}, {
			input:  "backup '123' s3option {\"bucket\"='dan-test1', \"filepath\"='ex_table_dan_gzip.gz',\"role_arn\"='arn:aws:iam::468413122987:role/dev-cross-s3', \"external_id\"='5404f91c_4e59_4898_85b3', \"compression\"='auto'}",
			output: "backup 123 s3option {'bucket'='dan-test1', 'filepath'='ex_table_dan_gzip.gz', 'role_arn'='arn:aws:iam::468413122987:role/dev-cross-s3', 'external_id'='5404f91c_4e59_4898_85b3', 'compression'='auto'}",
		}, {
			input:  "backup '123' s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='XXX', 'secret_access_key'='XXX', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}",
			output: "backup 123 s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}",
		}, {
			input:  "backup '123' s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='XXX', 'secret_access_key'='XXX', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}",
			output: "backup 123 s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}",
		}, {
			input:  `backup '123' s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='jsonline/jsonline_object.jl', 'region'='us-west-2', 'compression'='none', 'format'='jsonline', 'jsondata'='object'}`,
			output: `backup 123 s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='jsonline/jsonline_object.jl', 'region'='us-west-2', 'compression'='none', 'format'='jsonline', 'jsondata'='object'}`,
		},
		{
			input:  "backup '123' filesystem '/home/abc' parallelism '1' type 'incremental' timestamp 'xxxxx-xxxxx'",
			output: "backup 123 filesystem /home/abc parallelism 1 backuptype incremental backupts xxxxx-xxxxx",
		}, {
			input:  "backup '125' filesystem '/tmp/backup' parallelism '1' type 'incremental' timestamp 'xxxxx-xxxxx';",
			output: "backup 125 filesystem /tmp/backup parallelism 1 backuptype incremental backupts xxxxx-xxxxx",
		}, {
			input:  "backup '123' s3option {\"bucket\"='dan-test1', \"filepath\"='ex_table_dan_gzip.gz',\"role_arn\"='arn:aws:iam::468413122987:role/dev-cross-s3', \"external_id\"='5404f91c_4e59_4898_85b3', \"compression\"='auto'} type 'incremental' timestamp 'xxxxx-xxxxx'",
			output: "backup 123 s3option {'bucket'='dan-test1', 'filepath'='ex_table_dan_gzip.gz', 'role_arn'='arn:aws:iam::468413122987:role/dev-cross-s3', 'external_id'='5404f91c_4e59_4898_85b3', 'compression'='auto'} backuptype incremental backupts xxxxx-xxxxx",
		}, {
			input:  "backup '123' s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='XXX', 'secret_access_key'='XXX', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'} type 'incremental' timestamp 'xxxxx-xxxxx'",
			output: "backup 123 s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'} backuptype incremental backupts xxxxx-xxxxx",
		}, {
			input:  "backup '123' s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='XXX', 'secret_access_key'='XXX', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'} type 'incremental' timestamp 'xxxxx-xxxxx'",
			output: "backup 123 s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'} backuptype incremental backupts xxxxx-xxxxx",
		}, {
			input:  `backup '123' s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='jsonline/jsonline_object.jl', 'region'='us-west-2', 'compression'='none', 'format'='jsonline', 'jsondata'='object'} type 'incremental' timestamp 'xxxxx-xxxxx'`,
			output: `backup 123 s3option {'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='jsonline/jsonline_object.jl', 'region'='us-west-2', 'compression'='none', 'format'='jsonline', 'jsondata'='object'} backuptype incremental backupts xxxxx-xxxxx`,
		}, {
			input:  "/*!50001 CREATE ALGORITHM=UNDEFINED *//*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER *//*!50001 VIEW `pga0010` AS select distinct `a`.`FACDIV` AS `FACDIV`,`a`.`BLDCD` AS `BLDCD`,`a`.`PRDCD` AS `PRDCD`,`a`.`PRDNAM` AS `PRDNAM`,`a`.`PRDLNG` AS `PRDLNG`,`a`.`PRDWID` AS `PRDWID`,`a`.`PRDGAG` AS `PRDGAG`,`a`.`AREA` AS `AREA`,`a`.`GLZTYP` AS `GLZTYP`,`a`.`TECTYP` AS `TECTYP`,`a`.`PRDCATE` AS `PRDCATE`,`a`.`PRCCD` AS `PRCCD`,`a`.`PRCDSC` AS `PRCDSC`,`a`.`GLSSTR` AS `GLSSTR`,`a`.`REMARK` AS `REMARK`,`a`.`USEYN` AS `USEYN`,`a`.`ISMES` AS `ISMES` from (select 'N' AS `ISMES`,`skim`.`bga0010`.`USEYN` AS `USEYN`,`skim`.`bga0010`.`FACDIV` AS `FACDIV`,`skim`.`bga0010`.`BLDCDFATHER` AS `BLDCD`,substring_index(`skim`.`bga0010`.`PRDCD`,'-',1) AS `PRDCD`,`skim`.`bga0010`.`PRDNAM` AS `PRDNAM`,`skim`.`bga0010`.`PRDLNG` AS `PRDLNG`,`skim`.`bga0010`.`PRDWID` AS `PRDWID`,`skim`.`bga0010`.`PRDGAG` AS `PRDGAG`,`skim`.`bga0010`.`AREA` AS `AREA`,`skim`.`bga0010`.`GLZTYP` AS `GLZTYP`,`skim`.`bga0010`.`TECTYP` AS `TECTYP`,`skim`.`bga0010`.`PRDCATE` AS `PRDCATE`,`skim`.`bga0010`.`MATCST` AS `MATCST`,`skim`.`bga0010`.`PRCCD` AS `PRCCD`,`skim`.`bga0010`.`PRCDSC` AS `PRCDSC`,`skim`.`bga0010`.`GLSSTR` AS `GLSSTR`,`skim`.`bga0010`.`REMARK` AS `REMARK` from `skim`.`bga0010` where ((`skim`.`bga0010`.`ISMES` = 'Y') and (`skim`.`bga0010`.`USEYN` = 'Y') and (not(substring_index(`skim`.`bga0010`.`PRDCD`,'-',1) in (select `skim`.`bga0010`.`PRDCD` from `skim`.`bga0010` where ((`skim`.`bga0010`.`ISMES` = 'N') and (`skim`.`bga0010`.`USEYN` = 'Y')))))) union all select `skim`.`bga0010`.`ISMES` AS `ISMES`,`skim`.`bga0010`.`USEYN` AS `USEYN`,`skim`.`bga0010`.`FACDIV` AS `FACDIV`,`skim`.`bga0010`.`BLDCD` AS `BLDCD`,`skim`.`bga0010`.`PRDCD` AS `PRDCD`,`skim`.`bga0010`.`PRDNAM` AS `PRDNAM`,`skim`.`bga0010`.`PRDLNG` AS `PRDLNG`,`skim`.`bga0010`.`PRDWID` AS `PRDWID`,`skim`.`bga0010`.`PRDGAG` AS `PRDGAG`,`skim`.`bga0010`.`AREA` AS `AREA`,`skim`.`bga0010`.`GLZTYP` AS `GLZTYP`,`skim`.`bga0010`.`TECTYP` AS `TECTYP`,`skim`.`bga0010`.`PRDCATE` AS `PRDCATE`,`skim`.`bga0010`.`MATCST` AS `MATCST`,`skim`.`bga0010`.`PRCCD` AS `PRCCD`,`skim`.`bga0010`.`PRCDSC` AS `PRCDSC`,`skim`.`bga0010`.`GLSSTR` AS `GLSSTR`,`skim`.`bga0010`.`REMARK` AS `REMARK` from `skim`.`bga0010` where ((`skim`.`bga0010`.`ISMES` = 'N') and (`skim`.`bga0010`.`USEYN` = 'Y'))) `a` order by `a`.`BLDCD` */;",
			output: "create view pga0010 as select distinct a.FACDIV as FACDIV, a.BLDCD as BLDCD, a.PRDCD as PRDCD, a.PRDNAM as PRDNAM, a.PRDLNG as PRDLNG, a.PRDWID as PRDWID, a.PRDGAG as PRDGAG, a.AREA as AREA, a.GLZTYP as GLZTYP, a.TECTYP as TECTYP, a.PRDCATE as PRDCATE, a.PRCCD as PRCCD, a.PRCDSC as PRCDSC, a.GLSSTR as GLSSTR, a.REMARK as REMARK, a.USEYN as USEYN, a.ISMES as ISMES from (select N as ISMES, skim.bga0010.USEYN as USEYN, skim.bga0010.FACDIV as FACDIV, skim.bga0010.BLDCDFATHER as BLDCD, substring_index(skim.bga0010.PRDCD, -, 1) as PRDCD, skim.bga0010.PRDNAM as PRDNAM, skim.bga0010.PRDLNG as PRDLNG, skim.bga0010.PRDWID as PRDWID, skim.bga0010.PRDGAG as PRDGAG, skim.bga0010.AREA as AREA, skim.bga0010.GLZTYP as GLZTYP, skim.bga0010.TECTYP as TECTYP, skim.bga0010.PRDCATE as PRDCATE, skim.bga0010.MATCST as MATCST, skim.bga0010.PRCCD as PRCCD, skim.bga0010.PRCDSC as PRCDSC, skim.bga0010.GLSSTR as GLSSTR, skim.bga0010.REMARK as REMARK from skim.bga0010 where ((skim.bga0010.ISMES = Y) and (skim.bga0010.USEYN = Y) and (not (substring_index(skim.bga0010.PRDCD, -, 1) in (select skim.bga0010.PRDCD from skim.bga0010 where ((skim.bga0010.ISMES = N) and (skim.bga0010.USEYN = Y)))))) union all select skim.bga0010.ISMES as ISMES, skim.bga0010.USEYN as USEYN, skim.bga0010.FACDIV as FACDIV, skim.bga0010.BLDCD as BLDCD, skim.bga0010.PRDCD as PRDCD, skim.bga0010.PRDNAM as PRDNAM, skim.bga0010.PRDLNG as PRDLNG, skim.bga0010.PRDWID as PRDWID, skim.bga0010.PRDGAG as PRDGAG, skim.bga0010.AREA as AREA, skim.bga0010.GLZTYP as GLZTYP, skim.bga0010.TECTYP as TECTYP, skim.bga0010.PRDCATE as PRDCATE, skim.bga0010.MATCST as MATCST, skim.bga0010.PRCCD as PRCCD, skim.bga0010.PRCDSC as PRCDSC, skim.bga0010.GLSSTR as GLSSTR, skim.bga0010.REMARK as REMARK from skim.bga0010 where ((skim.bga0010.ISMES = N) and (skim.bga0010.USEYN = Y))) as a order by a.BLDCD",
		}, {
			input:  "/*!50001 CREATE ALGORITHM=UNDEFINED *//*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER *//*!50001 VIEW `sale_employee` AS select `ct`.`ENTID` AS `ORGANIZATION_ID`,`cu`.`SYSUSERID` AS `SALE_EMPLOYEE_ID`,`cu`.`SYSUSERID` AS `EMPLOYEE_ID`,`cu`.`ACTIVED` AS `ISUSEABLE`,`cu`.`CREATOR` AS `CREATED_BY`,`cu`.`CREATETIME` AS `CREATION_DATE`,`cu`.`UPDATOR` AS `LAST_UPDATED_BY`,`cu`.`UPDATETIME` AS `LAST_UPDATE_DATE`,'' AS `ATTRIBUTE11`,'' AS `ATTRIBUTE21`,'' AS `ATTRIBUTE31`,0 AS `ATTRIBUTE41`,0 AS `ATTRIBUTE51`,0 AS `AREA_ID` from (`kaf_cpcuser` `cu` join `kaf_cpcent` `ct`) where (`cu`.`ISSALEEMPLOYEE` = 2) */;",
			output: "create view sale_employee as select ct.ENTID as ORGANIZATION_ID, cu.SYSUSERID as SALE_EMPLOYEE_ID, cu.SYSUSERID as EMPLOYEE_ID, cu.ACTIVED as ISUSEABLE, cu.CREATOR as CREATED_BY, cu.CREATETIME as CREATION_DATE, cu.UPDATOR as LAST_UPDATED_BY, cu.UPDATETIME as LAST_UPDATE_DATE,  as ATTRIBUTE11,  as ATTRIBUTE21,  as ATTRIBUTE31, 0 as ATTRIBUTE41, 0 as ATTRIBUTE51, 0 as AREA_ID from kaf_cpcuser as cu inner join kaf_cpcent as ct where (cu.ISSALEEMPLOYEE = 2)",
		}, {
			input:  "/*!50001 CREATE ALGORITHM=UNDEFINED *//*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER *//*!50001 VIEW `xab0100` AS (select `a`.`SYSUSERID` AS `sysuserid`,`a`.`USERID` AS `userid`,`a`.`USERNAME` AS `usernm`,`a`.`PWDHASH` AS `userpwd`,`a`.`USERTYPE` AS `usertype`,`a`.`EMPID` AS `empid`,`a`.`EMAIL` AS `email`,`a`.`TELO` AS `telo`,`a`.`TELH` AS `telh`,`a`.`MOBIL` AS `mobil`,(case `a`.`ACTIVED` when '1' then 'N' when '2' then 'Y' else 'Y' end) AS `useyn`,`a`.`ENABLEPWD` AS `enablepwd`,`a`.`ENABLEMMSG` AS `enablemmsg`,`a`.`FEECENTER` AS `feecenter`,left(concat(ifnull(`c`.`ORGID`,''),'|'),(char_length(concat(ifnull(`c`.`ORGID`,''),'|')) - 1)) AS `orgid`,left(concat(ifnull(`c`.`ORGNAME`,''),'|'),(char_length(concat(ifnull(`c`.`ORGNAME`,''),'|')) - 1)) AS `orgname`,ifnull(`a`.`ISPLANNER`,'') AS `isplanner`,ifnull(`a`.`ISWHEMPLOYEE`,'') AS `iswhemployee`,ifnull(`a`.`ISBUYER`,'') AS `isbuyer`,ifnull(`a`.`ISQCEMPLOYEE`,'') AS `isqceemployee`,ifnull(`a`.`ISSALEEMPLOYEE`,'') AS `issaleemployee`,`a`.`SEX` AS `sex`,ifnull(`c`.`ENTID`,'3') AS `ORGANIZATION_ID`,ifnull(`a`.`NOTICEUSER`,'') AS `NOTICEUSER` from ((`kaf_cpcuser` `a` left join `kaf_cpcorguser` `b` on((`a`.`SYSUSERID` = `b`.`SYSUSERID`))) left join `kaf_cpcorg` `c` on((`b`.`ORGID` = `c`.`ORGID`))) order by `a`.`SYSUSERID`,`a`.`USERID`,`a`.`USERNAME`,`a`.`USERPASS`,`a`.`USERTYPE`,`a`.`EMPID`,`a`.`EMAIL`,`a`.`TELO`,`a`.`TELH`,`a`.`MOBIL`,`a`.`ACTIVED`,`a`.`ENABLEPWD`,`a`.`ENABLEMMSG`,`a`.`FEECENTER`,`a`.`ISPLANNER`,`a`.`ISWHEMPLOYEE`,`a`.`ISBUYER`,`a`.`ISQCEMPLOYEE`,`a`.`ISSALEEMPLOYEE`,`a`.`SEX`,`c`.`ENTID`) */;",
			output: "create view xab0100 as (select a.SYSUSERID as sysuserid, a.USERID as userid, a.USERNAME as usernm, a.PWDHASH as userpwd, a.USERTYPE as usertype, a.EMPID as empid, a.EMAIL as email, a.TELO as telo, a.TELH as telh, a.MOBIL as mobil, (case a.ACTIVED when 1 then N when 2 then Y else Y end) as useyn, a.ENABLEPWD as enablepwd, a.ENABLEMMSG as enablemmsg, a.FEECENTER as feecenter, left(concat(ifnull(c.ORGID, ), |), (char_length(concat(ifnull(c.ORGID, ), |)) - 1)) as orgid, left(concat(ifnull(c.ORGNAME, ), |), (char_length(concat(ifnull(c.ORGNAME, ), |)) - 1)) as orgname, ifnull(a.ISPLANNER, ) as isplanner, ifnull(a.ISWHEMPLOYEE, ) as iswhemployee, ifnull(a.ISBUYER, ) as isbuyer, ifnull(a.ISQCEMPLOYEE, ) as isqceemployee, ifnull(a.ISSALEEMPLOYEE, ) as issaleemployee, a.SEX as sex, ifnull(c.ENTID, 3) as ORGANIZATION_ID, ifnull(a.NOTICEUSER, ) as NOTICEUSER from kaf_cpcuser as a left join kaf_cpcorguser as b on ((a.SYSUSERID = b.SYSUSERID)) left join kaf_cpcorg as c on ((b.ORGID = c.ORGID)) order by a.SYSUSERID, a.USERID, a.USERNAME, a.USERPASS, a.USERTYPE, a.EMPID, a.EMAIL, a.TELO, a.TELH, a.MOBIL, a.ACTIVED, a.ENABLEPWD, a.ENABLEMMSG, a.FEECENTER, a.ISPLANNER, a.ISWHEMPLOYEE, a.ISBUYER, a.ISQCEMPLOYEE, a.ISSALEEMPLOYEE, a.SEX, c.ENTID)",
		},
		{
			input:  "CREATE TABLE `ecbase_push_log` (`id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间') ENGINE=InnoDB AUTO_INCREMENT=654 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='推送记录表'/*!50500 PARTITION BY RANGE  COLUMNS(create_time)(PARTITION p20240115 VALUES LESS THAN ('2024-01-15 00:00:00') ENGINE = InnoDB,PARTITION p20240116 VALUES LESS THAN ('2024-01-16 00:00:00') ENGINE = InnoDB,PARTITION p20240117 VALUES LESS THAN ('2024-01-17 00:00:00') ENGINE = InnoDB,PARTITION p20240118 VALUES LESS THAN ('2024-01-18 00:00:00') ENGINE = InnoDB,PARTITION p20240119 VALUES LESS THAN ('2024-01-19 00:00:00') ENGINE = InnoDB,PARTITION p20240120 VALUES LESS THAN ('2024-01-20 00:00:00') ENGINE = InnoDB,PARTITION p20240121 VALUES LESS THAN ('2024-01-21 00:00:00') ENGINE = InnoDB,PARTITION p20240122 VALUES LESS THAN ('2024-01-22 00:00:00') ENGINE = InnoDB,PARTITION p20240123 VALUES LESS THAN ('2024-01-23 00:00:00') ENGINE = InnoDB,PARTITION p20240124 VALUES LESS THAN ('2024-01-24 00:00:00') ENGINE = InnoDB,PARTITION p20240125 VALUES LESS THAN ('2024-01-25 00:00:00') ENGINE = InnoDB) */;",
			output: "create table ecbase_push_log (id bigint not null auto_increment comment 主键, create_time datetime not null default CURRENT_TIMESTAMP() comment 创建时间) engine = innodb auto_increment = 654 charset = utf8mb4 Collate = utf8mb4_general_ci comment = '推送记录表' partition by range columns (create_time) (partition p20240115 values less than (2024-01-15 00:00:00) engine = innodb, partition p20240116 values less than (2024-01-16 00:00:00) engine = innodb, partition p20240117 values less than (2024-01-17 00:00:00) engine = innodb, partition p20240118 values less than (2024-01-18 00:00:00) engine = innodb, partition p20240119 values less than (2024-01-19 00:00:00) engine = innodb, partition p20240120 values less than (2024-01-20 00:00:00) engine = innodb, partition p20240121 values less than (2024-01-21 00:00:00) engine = innodb, partition p20240122 values less than (2024-01-22 00:00:00) engine = innodb, partition p20240123 values less than (2024-01-23 00:00:00) engine = innodb, partition p20240124 values less than (2024-01-24 00:00:00) engine = innodb, partition p20240125 values less than (2024-01-25 00:00:00) engine = innodb)",
		},
		{
			input:  "show connectors",
			output: "show connectors",
		},
		{
			input:  "show index from t1 from db",
			output: "show index from t1 from db",
		},
		{
			input:  "show index from t1",
			output: "show index from t1",
		},
		{
			input:  "show index from db.t1",
			output: "show index from db.t1",
		},
		{
			input:  "show index from db.t1 from db",
			output: "show index from db.t1 from db",
		},
		{
			input:  "create table t1(a vecf32(3), b vecf64(3), c int)",
			output: "create table t1 (a vecf32(3), b vecf64(3), c int)",
		},
		{
			input:  "alter table tbl1 drop constraint fk_name",
			output: "alter table tbl1 drop foreign key fk_name",
		},
		{
			input:  "explain force execute st using @a",
			output: "explain execute st using @a",
		},
		{
			input:  "explain analyze force execute st using @a",
			output: "explain (analyze) execute st using @a",
		},
		{
			input:  "explain verbose force execute st using @a",
			output: "explain (verbose) execute st using @a",
		},
		{
			input:  "explain analyze verbose force execute st using @a",
			output: "explain (analyze,verbose) execute st using @a",
		},
		{
			input:  "explain force execute st",
			output: "explain execute st",
		},
		{
			input:  "explain analyze force execute st",
			output: "explain (analyze) execute st",
		},
		{
			input:  "explain verbose force execute st",
			output: "explain (verbose) execute st",
		},
		{
			input:  "explain analyze verbose force execute st",
			output: "explain (analyze,verbose) execute st",
		},
		{
			input:  "explain analyze verbose force execute st",
			output: "explain (analyze,verbose) execute st",
		},
		{
			input:  "create pitr `pitr1` for cluster range 1 'd'",
			output: "create pitr pitr1 for cluster range 1  d",
		},
		{
			input:  "create pitr `pitr2` for account acc01 range 1 'd'",
			output: "create pitr pitr2 for account acc01 range 1  d",
		},
		{
			input:  "create pitr `pitr3` range 1 'h'",
			output: "create pitr pitr3 for self account range 1  h",
		},
		{
			input:  "create pitr `pitr4` for database db01 range 1 'h'",
			output: "create pitr pitr4 for database db01 range 1  h",
		},
		{
			input:  "create pitr `pitr5` for database db01 table t01 range 1 'h'",
			output: "create pitr pitr5 for database db01 table t01 range 1  h",
		},
		{
			input: "show pitr",
		},
		{
			input:  "drop pitr `pitr1`",
			output: "drop pitr pitr1",
		},
		{
			input:  "drop pitr if exists `pitr2`",
			output: "drop pitr if exists pitr2",
		},
		{
			input:  "alter pitr `pitr3` range 2 'h'",
			output: "alter pitr pitr3 range 2  h",
		},
		{
			input:  "alter pitr if exists `pitr01` range 2 'h'",
			output: "alter pitr if exists pitr01 range 2  h",
		},
		{
			input:  "restore from pitr pitr01 '2021-01-01 00:00:00'",
			output: "restore self account from pitr pitr01 timestamp = 2021-01-01 00:00:00",
		},
		{
			input:  "restore database db01 from pitr pitr01 '2021-01-01 00:00:00'",
			output: "restore database db01 from pitr pitr01 timestamp = 2021-01-01 00:00:00",
		},
		{
			input:  "restore database db01 table t01 from pitr pitr01 '2021-01-01 00:00:00'",
			output: "restore database db01 table t01 from pitr pitr01 timestamp = 2021-01-01 00:00:00",
		},
	}
)

func TestValid(t *testing.T) {
	ctx := context.TODO()
	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		ast, err := ParseOne(ctx, tcase.input, 1)
		if err != nil {
			t.Errorf("Parse(%q) err: %v", tcase.input, err)
			continue
		}
		out := tree.String(ast, dialect.MYSQL)
		if tcase.output != out {
			t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, out)
		}
		ast.StmtKind()
	}
}

var (
	validStrSQL = []struct {
		input  string
		output string
	}{
		{
			input:  "create table pt1 (id int, category varchar(50)) partition by list columns(category) (partition p1 values in ('A', 'B') comment 'Category A and B', partition p2 values in ('C', 'D') comment 'Category C and D')",
			output: "create table pt1 (id int, category varchar(50)) partition by list columns (category) (partition p1 values in ('A', 'B') comment = 'Category A and B', partition p2 values in ('C', 'D') comment = 'Category C and D')",
		},
		{
			input:  "create table titles (emp_no int not null, title varchar(50) not null, from_date date not null, to_date date, primary key (emp_no, title, from_date)) partition by range(to_days(from_date)) (partition p01 values less than (to_days('1985-12-31')), partition p02 values less than (to_days('1986-12-31')))",
			output: "create table titles (emp_no int not null, title varchar(50) not null, from_date date not null, to_date date, primary key (emp_no, title, from_date)) partition by range(to_days(from_date)) (partition p01 values less than (to_days('1985-12-31')), partition p02 values less than (to_days('1986-12-31')))",
		},
		{
			input:  "create table pt2 (id int, date_column date, value int) partition by range(year(date_column)) (partition p1 values less than (2010) comment 'Before 2010', partition p2 values less than (2020) comment '2010 - 2019', partition p3 values less than (MAXVALUE) comment '2020 and Beyond')",
			output: "create table pt2 (id int, date_column date, value int) partition by range(year(date_column)) (partition p1 values less than (2010) comment = 'Before 2010', partition p2 values less than (2020) comment = '2010 - 2019', partition p3 values less than (MAXVALUE) comment = '2020 and Beyond')",
		},
	}
)

// Test whether strings in SQL can be restored in string format
func TestSQLStringFmt(t *testing.T) {
	ctx := context.TODO()
	for _, tcase := range validStrSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		ast, err := ParseOne(ctx, tcase.input, 1)
		if err != nil {
			t.Errorf("Parse(%q) err: %v", tcase.input, err)
			continue
		}
		out := tree.StringWithOpts(ast, dialect.MYSQL, tree.WithSingleQuoteString())
		if tcase.output != out {
			t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, out)
		}
	}
}

var (
	multiSQL = []struct {
		input  string
		output string
	}{{
		input:  "use db1; select * from t;",
		output: "use db1; select * from t",
	}, {
		input: "use db1; select * from t",
	}, {
		input: "use db1; select * from t; use db2; select * from t2",
	}, {
		input: `BEGIN
					DECLARE x,y VARCHAR(10);
					DECLARE z INT;
					DECLARE fl FLOAT DEFAULT 1.0;
				END`,
		output: `begin declare x y varchar(10) default null; declare z int default null; declare fl float default 1.0; end`,
	}, {
		input: `BEGIN
					CASE v
						WHEN 2 THEN SELECT v;
						WHEN 3 THEN SELECT 0;
					ELSE
						BEGIN
							CASE v
								WHEN 4 THEN SELECT v;
								WHEN 5 THEN SELECT 0;
							ELSE
								BEGIN
								END
							END CASE;
						END
					END CASE; 
				END`,
		output: "begin case v when 2 then select v; when 3 then select 0; else begin case v when 4 then select v; when 5 then select 0; else begin end; end case; end; end case; end",
	}, {
		input: `BEGIN
					IF n > m THEN SET s = '>';
					ELSEIF n = m THEN SET s = '=';
					ELSE SET s = '<';
					END IF;
				END`,
		output: "begin if n > m then set s = >; elseif n = m then set s = =; else set s = <; end if; end",
	}, {
		input: `BEGIN					
					IF n = m THEN SET s = 'equals';
					ELSE
						IF n > m THEN SET s = 'greater';
						ELSE SET s = 'less';
						END IF;
						SET s = CONCAT('is ', s, ' than');
					END IF;
					SET s = CONCAT(n, ' ', s, ' ', m, '.');
				END`,
		output: "begin if n = m then set s = equals; else if n > m then set s = greater; else set s = less; end if; set s = CONCAT(is , s,  than); end if; set s = CONCAT(n,  , s,  , m, .); end",
	}, {
		input: `BEGIN					
					label1: LOOP
						SET p1 = p1 + 1;
						IF p1 < 10 THEN
							ITERATE label1;
						END IF;
						LEAVE label1;
					END LOOP label1;
					SET @x = p1;
				END`,
		output: "begin label1: loop set p1 = p1 + 1; if p1 < 10 then iterate label1; end if; leave label1; end loop label1; set x = p1; end",
	}, {
		input: `BEGIN
					SET @x = 0;
					REPEAT
						SET @x = @x + 1;
					UNTIL @x > p1 END REPEAT;
				END`,
		output: "begin set x = 0; repeat set x = @x + 1; until @x > p1 end repeat; end",
	}, {
		input: `BEGIN
					DECLARE v1 INT DEFAULT 5;
					WHILE v1 > 0 DO
						SET v1 = v1 - 1;
					END WHILE;
				END`,
		output: "begin declare v1 int default 5; while v1 > 0 do set v1 = v1 - 1; end while; end",
	}}
)

func TestMulti(t *testing.T) {
	ctx := context.TODO()
	for _, tcase := range multiSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		asts, err := Parse(ctx, tcase.input, 1)
		if err != nil {
			t.Errorf("Parse(%q) err: %v", tcase.input, err)
			continue
		}
		var res string
		prefix := ""
		for _, ast := range asts {
			res += prefix
			out := tree.String(ast, dialect.MYSQL)
			res += out
			prefix = "; "
		}
		if tcase.output != res {
			t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, res)
		}
	}
}

// Fault tolerant use cases
var (
	invalidSQL = []struct {
		input string
	}{
		{
			input: "alter table t1 add constraint index (col3, col4)",
		},
		{
			input: "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 index (col3)",
		},
		{
			input: "alter table t1 add constraint uk_6dotkott2kjsp8vw4d0m25fb7 index zxxx (col3)",
		},
		{
			input: "create table t (a int, b char, constraint sdf index (a, b) )",
		},
		{
			input: "create table t (a int, b char, constraint sdf index idx(a, b) )",
		},
		{
			input: "create table t (a int, b char, constraint index idx(a, b) )",
		},
		{
			input: "ALTER TABLE t1 TRUNCATE PARTITION ALL, p0",
		},
		{
			input: "ALTER TABLE pt5 add column a INT NOT NULL, ADD PARTITION (PARTITION p4 VALUES LESS THAN (2022))",
		},
		{
			input: "ALTER TABLE pt5 ADD PARTITION (PARTITION p4 VALUES LESS THAN (2022)),add column a INT NOT NULL",
		},
		{
			input: "ALTER TABLE t1 ADD PARTITION (PARTITION p5 VALUES IN (15, 17)",
		},
	}
)

func TestFaultTolerance(t *testing.T) {
	ctx := context.TODO()
	for _, tcase := range invalidSQL {
		_, err := ParseOne(ctx, tcase.input, 1)
		if err == nil {
			t.Errorf("Fault tolerant ases (%q) should parse errors", tcase.input)
			continue
		}
	}
}
