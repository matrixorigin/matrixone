set global enable_privilege_cache = off;
create account acc_idx ADMIN_NAME 'user1' IDENTIFIED BY '123456';
-- @session:id=1&user=acc_idx:user1&password=123456
SET GLOBAL validate_password = on;
SET GLOBAL validate_password.check_user_name = ON;
SET GLOBAL validate_password.changed_characters_percentage = 10;
SET GLOBAL validate_password.policy = 0;
SET GLOBAL validate_password.length = 12;

-- user name in password
alter user 'user1' identified by 'user1';
alter user 'user1' identified by 'user1';
alter user 'user1' identified by '1resu';
alter user 'user1' identified by'user1abc123';
alter user 'user1' identified by 'abc123user1';
alter user 'user1' identified by 'abc123##user1dhidh##';
alter user 'user1' identified by 'abc1231resu';
alter user 'user1' identified by '1resudhwhdi##';
alter user 'user1' identified by 'bsjbcjs1resu%shdis';

-- user name characters percentage
alter user 'user1' identified by '12345678998a';

-- user name length
alter user 'user1' identified by '123456789a';

SET GLOBAL validate_password.policy = 1;
SET GLOBAL validate_password.length = 12;
SET GLOBAL validate_password.mixed_case_count = 2;
SET GLOBAL validate_password.number_count = 6;
SET GLOBAL validate_password.special_char_count = 1;

-- user name length
alter user 'user1' identified by '1234a6789a#';

-- lower case 
alter user 'user1' identified by '1234567896472aA#';

-- upper case
alter user 'user1' identified by '123456789647aaA#';

-- number 
alter user 'user1' identified by '1234aadsjTTTTTT#';

-- special char
alter user 'user1' identified by '123446dsjT78TTT';

-- all pass
alter user 'user1' identified by '123446dsjT78TTT!!';
-- @session
drop account acc_idx;
set global enable_privilege_cache = on;
