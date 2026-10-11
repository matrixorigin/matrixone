-- test remove files from stage with single file, wildcard, and if exists
create stage remove_stage_files_stage url = 'file://$resources/into_outfile/';

-- create files in stage
select save_file(cast('stage://remove_stage_files_stage/remove_stage_files/remove_case_1.txt' as datalink), 'alpha');
select save_file(cast('stage://remove_stage_files_stage/remove_stage_files/remove_case_2.txt' as datalink), 'beta');
select save_file(cast('stage://remove_stage_files_stage/remove_stage_files/remove_keep.log' as datalink), 'keep');

-- remove a single file
remove files from stage 'stage://remove_stage_files_stage/remove_stage_files/remove_case_1.txt';

-- verify single file removed
select load_file(cast('stage://remove_stage_files_stage/remove_stage_files/remove_case_1.txt' as datalink));

-- remove files by wildcard
remove files from stage 'stage://remove_stage_files_stage/remove_stage_files/remove_case_*.txt';

-- verify wildcard removal and keep file remains
select load_file(cast('stage://remove_stage_files_stage/remove_stage_files/remove_case_2.txt' as datalink));
select load_file(cast('stage://remove_stage_files_stage/remove_stage_files/remove_keep.log' as datalink));

-- if exists should ignore missing files
remove files from stage if exists 'stage://remove_stage_files_stage/remove_stage_files/no_match_*.txt';

-- without if exists should error when no file matches
remove files from stage 'stage://remove_stage_files_stage/remove_stage_files/no_match_*.txt';

-- A frontend-only removal must not reuse an enabled definition after another
-- session disables the stage. Keep the two removals adjacent in session 0:
-- an engine-backed query between them would clear the stale cache itself.
select save_file(cast('stage://remove_stage_files_stage/remove_stage_files/warm.txt' as datalink), 'warm');
select save_file(cast('stage://remove_stage_files_stage/remove_stage_files/protected.txt' as datalink), 'protected');
remove files from stage 'stage://remove_stage_files_stage/remove_stage_files/warm.txt';
-- @session:id=1{
alter stage remove_stage_files_stage set enable = false;
-- @session}
remove files from stage 'stage://remove_stage_files_stage/remove_stage_files/protected.txt';
remove files from stage if exists 'stage://remove_stage_files_stage/remove_stage_files/protected.txt';

-- Read via a direct file URL so the content oracle does not require the
-- disabled stage. The rejected removal must leave the original bytes intact.
select load_file(cast('file://$resources/into_outfile/remove_stage_files/warm.txt' as datalink));
select load_file(cast('file://$resources/into_outfile/remove_stage_files/protected.txt' as datalink));
-- @session:id=1{
alter stage remove_stage_files_stage set enable = true;
-- @session}
remove files from stage 'stage://remove_stage_files_stage/remove_stage_files/protected.txt';
select load_file(cast('stage://remove_stage_files_stage/remove_stage_files/protected.txt' as datalink));

-- cleanup created file
remove files from stage if exists 'stage://remove_stage_files_stage/remove_stage_files/remove_keep.log';

drop stage remove_stage_files_stage;
