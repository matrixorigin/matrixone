SET GLOBAL validate_password = on;
SET GLOBAL validate_password.check_user_name = ON;
SET GLOBAL validate_password.changed_characters_percentage = 10;
SET GLOBAL validate_password.policy = 0;
SET GLOBAL validate_password.length = 12;


-- user name in password
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY 'user1';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '1resu';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY 'user1abc123';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY 'abc123user1';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY 'abc123##user1dhidh##';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY 'abc1231resu';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '1resudhwhdi##';
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY 'bsjbcjs1resu%shdis';
drop user if exists user1;

-- user name characters percentage
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '12345678998a';
drop user if exists user1;

-- user name length
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '123456789a';
drop user if exists user1;



SET GLOBAL validate_password.policy = 1;
SET GLOBAL validate_password.length = 12;
SET GLOBAL validate_password.mixed_case_count = 2;
SET GLOBAL validate_password.number_count = 6;
SET GLOBAL validate_password.special_char_count = 1;

-- user name length
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '1234a6789a#';
drop user if exists user1;

-- lower case 
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '1234567896472aA#';
drop user if exists user1;

-- upper case
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '123456789647aaA#';
drop user if exists user1;

-- number 
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '1234aadsjTTTTTT#';
drop user if exists user1;

-- special
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '123446dsjT78TTT';
drop user if exists user1;

-- all pass
drop user if exists user1;
CREATE USER user1  IDENTIFIED BY '123446dsjT78TTT!!';
drop user if exists user1;

SET GLOBAL validate_password = default;
SET GLOBAL validate_password.check_user_name = default;
SET GLOBAL validate_password.changed_characters_percentage = default;
SET GLOBAL validate_password.policy = default;
SET GLOBAL validate_password.length = default;
SET GLOBAL validate_password.mixed_case_count = default;
SET GLOBAL validate_password.number_count = default;
SET GLOBAL validate_password.special_char_count = default;
