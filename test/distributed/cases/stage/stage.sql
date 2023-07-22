CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
SELECT stage_name, stage_status from mo_catalog.mo_stages;

ALTER STAGE my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;


-- @bvt:issue#8544
CREATE STAGE my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
-- @session
drop account default_1;
drop stage my_ext_stage;


CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SHOW STAGES;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SHOW STAGES;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
CREATE STAGE my_ext_stage4 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE COMMENT 'self stage';
SHOW STAGES;
SHOW STAGES like 'my_ext_stage3';

ALTER STAGE my_ext_stage5 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage5 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage4 SET COMMENT 'user stage';
SHOW STAGES;
SHOW STAGES like 'my_ext_stage1';

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;
DROP STAGE my_ext_stage4;
-- @bvt:issue