select @@global.validnode_checking;
select @@global.invited_nodes;

SET GLOBAL validnode_checking = on;
SET GLOBAL invited_nodes = '';
SET GLOBAL invited_nodes = '127.0.0.1';
SET GLOBAL invited_nodes = '*';
SET GLOBAL invited_nodes = '192.168.1.0/24';
SET GLOBAL invited_nodes = '127.0.0.1,192.168.1.1';
SET GLOBAL invited_nodes = '127.0.0.1,192.168.1.1/24';
SET GLOBAL invited_nodes = '192.168.1.1, 10.0.0.0/33';
SET GLOBAL invited_nodes = '192.168.1.1, invalid_ip';
SET GLOBAL invited_nodes = '192.168.1.1, *';
SET GLOBAL invited_nodes = '192.168.1.1, 10.0.0.0/33, invalid_ip';
SET GLOBAL invited_nodes = '127.0.0.1';

SET GLOBAL invited_nodes = '*';
drop user if exists user1;
create user user1 identified by '123456';
-- @session:id=1&user=sys:user1&password=123456
alter user user1 identified by '1234567';
-- @session

drop user user1;

SET GLOBAL validnode_checking = default;
SET GLOBAL invited_nodes = default;
