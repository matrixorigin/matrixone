drop role if exists rolex;
create role rolex;
drop user if exists userx;
create user userx identified by '111';
grant moadmin to root,dump;
grant moadmin to userx;
grant moadmin to rolex;
grant rolex to moadmin;
grant public to root,dump;
grant public to userx;
grant public to rolex;
grant rolex to public;

drop role rolex;
drop user userx;