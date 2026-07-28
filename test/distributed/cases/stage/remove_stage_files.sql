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

-- cleanup created file
remove files from stage if exists 'stage://remove_stage_files_stage/remove_stage_files/remove_keep.log';

drop stage remove_stage_files_stage;
