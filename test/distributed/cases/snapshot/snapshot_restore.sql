-- @bvt:issue#14784
-- table level noraml
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from test_snapshot_read;
create snapshot snapshot_01 for account sys;
delete from test_snapshot_read where a <= 50;
select count(*) from test_snapshot_read;
select count(*) from test_snapshot_read {snapshot = 'snapshot_01'};
create snapshot snapshot_02 for account sys;
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40);
select count(*) from test_snapshot_read;
select count(*) from test_snapshot_read{snapshot = 'snapshot_02'};
restore account 'sys' database 'snapshot_read' table 'test_read' from snapshot 'snapshot_01';
select count(*) from test_snapshot_read;
restore account 'sys' database 'snapshot_read' table 'test_read' from snapshot 'snapshot_02';
select count(*) from test_snapshot_read;
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

select id, username, email from users where email = 'john@example.com';
select id, username, email from users where email = 'alice.jones@gmail.com';

create snapshot sp_01 for account sys;

DELETE FROM  users where email = 'john@example.com';
UPDATE users SET password = 'newsecurepassword123' WHERE email = 'alice.jones@gmail.com';

select id, username, email from users where email = 'john@example.com';
select id, username, email from users where email = 'alice.jones@gmail.com';

restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select id, username, email from users where email = 'john@example.com';
select id, username, email from users where email = 'alice.jones@gmail.com';


CREATE TABLE new_users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

insert into new_users select * from users where email = 'john@example.com';
insert into new_users select * from users where email = 'alice.jones@gmail.com';

select id, username, email from new_users;

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

select count(*) from users;
create snapshot sp_01 for account sys;
drop table users;
select count(*) from users;
restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select count(*) from users;
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

select count(*) from users;
create snapshot sp_01 for account sys;
drop database snapshot_read;
select count(*) from users;
restore account 'sys' database 'snapshot_read' table 'users' from snapshot 'sp_01';
select count(*) from users;
drop snapshot sp_01;
drop database if exists snapshot_read;

-- database level update/delete/insert
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
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
select count(*) from test_snapshot_read;
select count(*) from users;
create snapshot sp_01 for account sys;
delete from test_snapshot_read where a <= 50;
DELETE FROM  users where email = 'john@example.com';
UPDATE users SET password = 'newsecurepassword123' WHERE email = 'alice.jones@gmail.com';
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40);
INSERT INTO users (username, email, password) VALUES ('natalie_green', 'natalie.green@greenfutures.com', 'N4t4lieG'), ('olivia_woods', 'olivia.woods@woodlandassociates.com', '0liv1aW'), ('william_rivera', 'william.rivera@riveraindustries.com', 'W1ll1amR'), ('samantha_parker', 'samantha.parker@parkergroup.net', 'S4m4nth4P'), ('daniel_miller', 'daniel.miller@millerventures.com', 'D4ni3lM1ll3r')('alexander_scott', 'alexander.scott@blueocean.com', 'A1exanderS'),('brooke_myers', 'brooke.myers@financialgroup.org', 'B2rookeM'),('caleb_anderson', 'caleb.anderson@eagletech.com', 'C3alebA'),('diana_price', 'diana.price@priceconsulting.net', 'D1anaP'),('eric_baker', 'eric.baker@bakertopia.com', 'E2ricB'),('faith_garcia', 'faith.garcia@garciadesigns.com', 'F3ithG'),('gabriel_ross', 'gabriel.ross@rossindustries.org', 'G4abrielR'),('hannah_hunter', 'hannah.hunter@hunterpublishing.com', 'H1annahH'),('isaac_perez', 'isaac.perez@pereztech.net', 'I2saacP'),('julia_lee', 'julia.lee@leesolutions.com', 'J3uliaL'),('kevin_grant', 'kevin.grant@grantenterprises.net', 'K1evinG'),('laura_ford', 'laura.ford@fordmotorsports.com', 'L2auraF'),('matthew_woods', 'matthew.woods@woodsinnovations.com', 'M3atthewW'),('natalie_johnson', 'natalie.johnson@johnsongroup.org', 'N4talieJ'),('olivia_simpson', 'olivia.simpson@simpsonsolutions.com', 'O1iviaS'),('paul_brown', 'paul.brown@brownconstruction.net', 'P2aulB'),('quinn_miller', 'quinn.miller@millerventures.com', 'Q3uinnM'),('ruby_walker', 'ruby.walker@walkertech.com', 'R4ubyW'),('sarah_lee', 'sarah.lee@leesenterprises.com', 'S1arahL'),('thomas_oliver', 'thomas.oliver@oliverconsulting.net', 'T2homasO'),('umair_garcia', 'umair.garcia@garciagroup.com', 'U3mairG'),('vanessa_perez', 'vanessa.perez@perezassociates.com', 'V1anessaP'),('warren_lee', 'warren.lee@leesinternational.com', 'W2arrenL'),('xavier_rivera', 'xavier.rivera@riveraimports.com', 'X3avierR'),('yolanda_smith', 'yolanda.smith@smithmarketing.com', 'Y1olandaS'),('zoe_johnson', 'zoe.johnson@johnsoncreative.net', 'Z2oeJ');
select count(*) from test_snapshot_read;
select count(*) from users;

restore account 'sys' database 'snapshot_read' from snapshot 'sp_01';
select count(*) from test_snapshot_read;
select count(*) from users;

drop database snapshot_read;
drop snapshot sp_01;

-- database level drop table
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
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

select count(*) from test_snapshot_read;
select count(*) from users;

create snapshot sp_01 for account sys;

drop table users;
drop table test_snapshot_read;

select count(*) from test_snapshot_read;
select count(*) from users;

restore account 'sys' database 'snapshot_read' from snapshot 'sp_01';

select count(*) from test_snapshot_read;
select count(*) from users;

drop database snapshot_read;
drop snapshot sp_01;


-- database level drop database
create database if not exists snapshot_read;
use snapshot_read;
create table test_snapshot_read (a int);
INSERT INTO test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
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

select count(*) from test_snapshot_read;
select count(*) from users;

create snapshot sp_01 for account sys;

drop database snapshot_read;

select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.users;

restore account 'sys' database 'snapshot_read' from snapshot 'sp_01';

select count(*) from snapshot_read.test_snapshot_read;
select count(*) from snapshot_read.users;

drop snapshot sp_01;
drop database if exists snapshot_read;
-- @bvt:issue

