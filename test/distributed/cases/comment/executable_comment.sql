-- Executable comments use the fixed MySQL compatibility version 80030 (#31056).
-- Pure wrappers must reach the server: future bodies produce no result set,
-- while the matching wrapper is a positive control for the same client path.
/*!99999 select 42 */;
/*!80030 select 6 as matched_statement */;

-- Skipping the select list produces a syntax error; the connection stays usable.
select /*!99999 6 */;
select /*!80031 6 */;
select 7 as after_error;

-- Boundary, older-version and unversioned bodies continue to execute.
select /*!80030 6 */ as current_version;
select /*!80029 6 */ as lower_version;
select /*!50000 6 */ as old_version;
select /*! 6 */ as unversioned;

-- Consume the entire immediate digit run, including leading zeros.
select 2 /*!999999999 */ as long_guard;
select 2 /*!800300 + 1 */ as full_guard;
select /*!00080030 6 */ as leading_zero_match;
select 2 /*!00080031 + 1 */ as leading_zero_future;

-- Ignored executable comments must not mutate session state.
set @executable_comment_saved_mode = @@sql_mode;
set sql_mode = 'NO_BACKSLASH_ESCAPES';
/*!99999 set sql_mode='ANSI_QUOTES' */;
select @@sql_mode as mode_after_future_comment;
set sql_mode = @executable_comment_saved_mode;
select @@sql_mode = @executable_comment_saved_mode as mode_restored;
set @executable_comment_saved_mode = null;
