SET GLOBAL validate_password = default;
SET GLOBAL validate_password.check_user_name = default;
SET GLOBAL validate_password.changed_characters_percentage = default;
SET GLOBAL validate_password.policy = default;
SET GLOBAL validate_password.length = default;
SET GLOBAL validate_password.mixed_case_count = default;
SET GLOBAL validate_password.number_count = default;
SET GLOBAL validate_password.special_char_count = default;

select @@validate_password;
select @@validate_password.changed_characters_percentage;
select @@validate_password.policy;
select @@validate_password.length;
select @@validate_password.mixed_case_count;
select @@validate_password.number_count;
select @@validate_password.special_char_count;

select @@global.validate_password;
select @@global.validate_password.check_user_name;
select @@global.validate_password.changed_characters_percentage;
select @@global.validate_password.policy;
select @@global.validate_password.length;
select @@global.validate_password.mixed_case_count;
select @@global.validate_password.number_count;
select @@global.validate_password.special_char_count;



SET GLOBAL validate_password = on;
SET GLOBAL validate_password.check_user_name = ON;
SET GLOBAL validate_password.changed_characters_percentage = 10;
SET GLOBAL validate_password.policy = 0;
SET GLOBAL validate_password.length = 10;


select @@global.validate_password;
select @@global.validate_password.check_user_name;
select @@global.validate_password.changed_characters_percentage;
select @@global.validate_password.policy;
select @@global.validate_password.length;


SET GLOBAL validate_password.policy = 1;
SET GLOBAL validate_password.length = 16;
SET GLOBAL validate_password.mixed_case_count = 8;
SET GLOBAL validate_password.number_count = 9;
SET GLOBAL validate_password.special_char_count = 10;

select @@global.validate_password.policy;
select @@global.validate_password.length;
select @@global.validate_password.mixed_case_count ;
select @@global.validate_password.number_count;
select @@global.validate_password.special_char_count;

SET GLOBAL validate_password.policy = "LOW";
select @@global.validate_password.policy;
SET GLOBAL validate_password.policy = "MEDIUM";
select @@global.validate_password.policy;

SET GLOBAL validate_password = default;
SET GLOBAL validate_password.check_user_name = default;
SET GLOBAL validate_password.changed_characters_percentage = default;
SET GLOBAL validate_password.policy = default;
SET GLOBAL validate_password.length = default;
SET GLOBAL validate_password.mixed_case_count = default;
SET GLOBAL validate_password.number_count = default;
SET GLOBAL validate_password.special_char_count = default;
