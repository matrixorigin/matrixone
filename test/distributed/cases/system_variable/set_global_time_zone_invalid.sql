-- SET GLOBAL time_zone rejects an unknown zone like SET SESSION does, so a
-- typo cannot be persisted for every new session (issue #31714).
select @@global.time_zone;
set global time_zone = 'Mars/Phobos';
set global time_zone = '+25:00';
set global time_zone = 'not a zone';
select @@global.time_zone;
set time_zone = 'Mars/Phobos';
select @@session.time_zone;
