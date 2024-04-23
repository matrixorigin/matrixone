-- @bvt:issue#14784

-- table level noraml
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;
create snapshot snapshot_01 for account sys;
delete from test_snapshot_read where a <= 50;
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read {snapshot = 'snapshot_01'};
create snapshot snapshot_02 for account sys;
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40);
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_02'};
restore account 'sys' database 'snapshot_read' table 'test_read' from snapshot 'snapshot_01';
select count(*) from snapshot_read.test_snapshot_read;
restore account 'sys' database 'snapshot_read' table 'test_read' from snapshot 'snapshot_02';
select count(*) from snapshot_read.test_snapshot_read;
drop database snapshot_read;
drop snapshot snapshot_01;
drop snapshot snapshot_02;


-- table level pk col
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select id, username, email from snapshot_read.users where email = 'john@example.com';
select id, username, email from snapshot_read.users where email = 'alice.jones@gmail.com';

create snapshot sp_01 for account sys;

DELETE FROM  users where email = 'john@example.com';
UPDATE users SET password = 'newsecurepassword123' WHERE email = 'alice.jones@gmail.com';

select id, username, email from snapshot_read.users where email = 'john@example.com';
select id, username, email from snapshot_read.users where email = 'alice.jones@gmail.com';

restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select id, username, email from snapshot_read.users where email = 'john@example.com';
select id, username, email from snapshot_read.users where email = 'alice.jones@gmail.com';


CREATE TABLE new_users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

insert into new_users select * from snapshot_read.users where email = 'john@example.com';
insert into new_users select * from snapshot_read.users where email = 'alice.jones@gmail.com';

select id, username, email from snapshot_read.new_users;

drop snapshot sp_01;
drop database if exists snapshot_read;

-- table level drop table
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select count(*) from snapshot_read.users;
create snapshot sp_01 for account sys;
drop table users;
select count(*) from snapshot_read.users;
restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select count(*) from snapshot_read.users;
drop snapshot sp_01;
drop database if exists snapshot_read;

-- table level drop database
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select count(*) from snapshot_read.users;
create snapshot sp_01 for account sys;
drop database snapshot_read;
select count(*) from snapshot_read.users;
restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select count(*) from snapshot_read.users;
drop snapshot sp_01;
drop database if exists snapshot_read;

-- table level view
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

create view users_view as select * from snapshot_read.users;
select count(*) from snapshot_read.users_view;
create snapshot sp_01 for account sys;

drop view users_view;
select count(*) from snapshot_read.users_view;
restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
restore account 'sys' database 'snapshot_read' table 'users_view' from snapshot 'sp_01';
select count(*) from snapshot_read.users_view;
drop snapshot sp_01;
drop database if exists snapshot_read;



-- normal account
create account test_account admin_name = 'test_user' identified by '111';
-- @session:id=2&user=test_account:test_user&password=111
-- table level noraml
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;
create snapshot snapshot_01 for account test_account;
delete from test_snapshot_read where a <= 50;
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read {snapshot = 'snapshot_01'};
create snapshot snapshot_02 for account test_account;
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40);
select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.test_snapshot_read{snapshot = 'snapshot_02'};
restore account 'test_account' database 'snapshot_read' table 'test_snapshot_read' from snapshot 'snapshot_01';
select count(*) from snapshot_read.test_snapshot_read;
restore account 'test_account' database 'snapshot_read' table 'test_snapshot_read' from snapshot 'snapshot_02';
select count(*) from snapshot_read.test_snapshot_read;
drop database snapshot_read;
drop snapshot snapshot_01;
drop snapshot snapshot_02;


-- table level pk col
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select id, username, email from snapshot_read.users where email = 'john@example.com';
select id, username, email from snapshot_read.users where email = 'alice.jones@gmail.com';

create snapshot sp_01 for account test_account;

DELETE FROM users where email = 'john@example.com';
UPDATE users SET password = 'newsecurepassword123' WHERE email = 'alice.jones@gmail.com';

select id, username, email from snapshot_read.users where email = 'john@example.com';
select id, username, email from snapshot_read.users where email = 'alice.jones@gmail.com';

restore account 'test_account' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select id, username, email from snapshot_read.users where email = 'john@example.com';
select id, username, email from snapshot_read.users where email = 'alice.jones@gmail.com';


CREATE TABLE new_users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

insert into new_users select * from snapshot_read.users where email = 'john@example.com';
insert into new_users select * from snapshot_read.users where email = 'alice.jones@gmail.com';

select id, username, email from snapshot_read.new_users;

drop snapshot sp_01;
drop database if exists snapshot_read;

-- table level drop table
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select count(*) from snapshot_read.users;
create snapshot sp_01 for account test_account;
drop table users;
select count(*) from snapshot_read.users;
restore account 'test_account' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select count(*) from snapshot_read.users;
drop snapshot sp_01;
drop database if exists snapshot_read;

-- table level drop database
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

select count(*) from snapshot_read.users;
create snapshot sp_01 for account test_account;
drop database snapshot_read;
select count(*) from snapshot_read.users;
restore account 'test_account' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select count(*) from snapshot_read.users;
drop snapshot sp_01;
drop database if exists snapshot_read;

-- table level view
create database if not exists snapshot_read;
use snapshot_read;
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

create view users_view as select * from snapshot_read.users;
select count(*) from snapshot_read.users_view;
create snapshot sp_01 for account test_account;

drop view users_view;
select count(*) from snapshot_read.users_view;
restore account 'test_account' database 'snapshot_read' table 'users' from snapshot 'sp_01';
restore account 'test_account' database 'snapshot_read' table 'users_view' from snapshot 'sp_01';
select count(*) from snapshot_read.users_view;
drop snapshot sp_01;
drop database if exists snapshot_read;
-- @session

drop account test_account;


-- sys account restore normal account
create account test_account admin_name = 'test_user' identified by '111';
-- @session:id=3&user=test_account:test_user&password=111
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
insert into test_snapshot_read (a) values(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

Create view v1 as select * from snapshot_read.users;
select count(*) from snapshot_read.users;
select count(*) from snapshot_read.v1;
-- @session

create snapshot sp_01 for account test_account;

-- @session:id=4&user=test_account:test_user&password=111
drop view v1;
DELETE FROM users where email = 'john@example.com';
select count(*) from snapshot_read.users;
-- @session

restore account 'test_account' database 'snapshot_read' table 'users' from snapshot 'sp_01';
restore account 'test_account' database 'snapshot_read' table 'v1' from snapshot 'sp_01';

-- @session:id=5&user=test_account:test_user&password=111
select count(*) from snapshot_read.users;
select count(*) from snapshot_read.v1;
-- @session

drop snapshot sp_01;
drop account test_account;


-- sys account restore normal account to new account
create account test_account admin_name = 'test_user' identified by '111';
-- @session:id=6&user=test_account:test_user&password=111
-- @session
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
insert into test_snapshot_read (a) values(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from snapshot_read.test_snapshot_read;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (username, email, password) VALUES ('john_doe', 'john@example.com', 'securepassword123');
INSERT INTO users (username, email, password) VALUES ('jane_smith', 'jane.smith@example.com', 'password123'),('alice_jones', 'alice.jones@gmail.com', 'ilovecats'),('bob_brown', 'bob.brown@yahoo.com', 'mysecretpassword'),('charlie_lee', 'charlie.lee@protonmail.ch', 'secure123'),('diana_wilson', 'diana.wilson@outlook.com', 'D1anaPass');
INSERT INTO users (username, email, password) VALUES ('emily_adams', 'emily.adams@icloud.com', 'Em1Ly123'), ('francis_nguyen', 'francis.nguyen@domain.com', 'fNguyenPass'), ('grace_parker', 'grace.parker@server.com', 'G1race123'), ('henry_miller', 'henry.miller@company.org', 'hMillerSecret'), ('isabella_grant', 'isabella.grant@university.edu', 'iGrantPass');

Create view v1 as select * from snapshot_read.users;
select count(*) from snapshot_read.users;
select count(*) from snapshot_read.v1;
-- @bvt:issue

create snapshot sp_01 for account test_account;

-- @session:id=7&user=test_account:test_user&password=111
drop view v1;
DELETE FROM users where email = 'john@example.com';
select count(*) from snapshot_read.users;
-- @session

create account test_account_01 admin_name = 'test_user_01' identified by '111';
restore account 'test_account' database 'snapshot_read' table 'users' from snapshot 'sp_01' to NEWACCOUNT 'test_account_01';
restore account 'test_account' database 'snapshot_read' table 'v1' from snapshot 'sp_01' to NEWACCOUNT 'test_account_01';

-- @session:id=8&user=test_account_01:test_user_01&password=111
select count(*) from snapshot_read.users;
select count(*) from snapshot_read.v1;
-- @session

drop snapshot sp_01;
drop account test_account;
drop account test_account_01;

-- @bvt:issue