drop stage if exists `sdk 'quoted' {name}`;
create stage `sdk 'quoted' {name}` url='s3://bucket/o''connor/' credentials={'AWS_SECRET_KEY'='one''\\two=three'} enable=false comment='qa ''quoted'' comment';
select stage_name, cast(url='s3://bucket/o''connor/' as int) as url_ok, cast(stage_status='disabled' as int) as disabled, cast(comment='qa ''quoted'' comment' as int) as comment_ok, cast(hex(stage_credentials)='4157535F5345435245545F4B45593D6F6E65275C74776F3D7468726565' as int) as credential_ok from mo_catalog.mo_stages where stage_name='sdk ''quoted'' {name}';
alter stage `sdk 'quoted' {name}` set url='file:///tmp/quoted''path/' enable=true comment='changed ''comment''';
select cast(url='file:///tmp/quoted''path/' as int) as url_ok, cast(stage_status='disabled' as int) as disabled, cast(comment='changed ''comment''' as int) as comment_ok from mo_catalog.mo_stages where stage_name='sdk ''quoted'' {name}';
drop stage `sdk 'quoted' {name}`;
select count(*) as remaining from mo_catalog.mo_stages where stage_name='sdk ''quoted'' {name}';
