-- Regression case for issue #31806: role GRANT ... WITH ADMIN OPTION.
-- The ADMIN clause is accepted for role grants only, persisted into the
-- existing with_grant_option flag (mo_user_grant) and enforced on downward
-- re-grants; privilege grants keep rejecting it and the legacy WITH GRANT
-- OPTION clause keeps working.

-- @session:id=0&user=dump&password=111
-- Clean up leftovers so the case is repeatable on the same instance.
drop role if exists r31806_admin_bvt;
drop user if exists u31806_admin_bvt;
drop user if exists u31806_grantee_bvt;

create role r31806_admin_bvt;
create user u31806_admin_bvt identified by 'Passw0rd_bvt1';
create user u31806_grantee_bvt identified by 'Passw0rd_bvt2';

-- ADMIN option accepted on the role grant and persisted as with_grant_option.
grant r31806_admin_bvt to u31806_admin_bvt with admin option;
select cast(g.with_grant_option as signed) as wgo from mo_catalog.mo_user_grant g, mo_catalog.mo_role r, mo_catalog.mo_user u where g.role_id = r.role_id and g.user_id = u.user_id and r.role_name = 'r31806_admin_bvt' and u.user_name = 'u31806_admin_bvt';

-- Legacy clause still accepted and maps to the same persisted flag.
grant r31806_admin_bvt to u31806_admin_bvt with grant option;
select cast(g.with_grant_option as signed) as wgo from mo_catalog.mo_user_grant g, mo_catalog.mo_role r, mo_catalog.mo_user u where g.role_id = r.role_id and g.user_id = u.user_id and r.role_name = 'r31806_admin_bvt' and u.user_name = 'u31806_admin_bvt';

-- Privilege grants reject the ADMIN clause.
grant select on database mo_catalog.* to u31806_admin_bvt with admin option; -- fail
-- @session

-- Grantee delegates the role onward under its active role.
-- @session:id=1&user=u31806_admin_bvt&password=Passw0rd_bvt1
set role r31806_admin_bvt;
grant r31806_admin_bvt to u31806_grantee_bvt;
-- @session

-- @session:id=0&user=dump&password=111
-- Delegation persisted the grant without the option.
select cast(g.with_grant_option as signed) as wgo from mo_catalog.mo_user_grant g, mo_catalog.mo_role r, mo_catalog.mo_user u where g.role_id = r.role_id and g.user_id = u.user_id and r.role_name = 'r31806_admin_bvt' and u.user_name = 'u31806_grantee_bvt';

-- Nonexistent grantee fails without partial state.
grant r31806_admin_bvt to u31806_nosuch_bvt; -- fail
select count(*) as granted_rows from mo_catalog.mo_user_grant g, mo_catalog.mo_user u where g.user_id = u.user_id and u.user_name = 'u31806_nosuch_bvt';

-- Cleanup: leave nothing behind, keeping the case repeatable.
drop user if exists u31806_admin_bvt;
drop user if exists u31806_grantee_bvt;
drop role if exists r31806_admin_bvt;
select count(*) as leftover_roles from mo_catalog.mo_role where role_name = 'r31806_admin_bvt';
select count(*) as leftover_users from mo_catalog.mo_user where user_name in ('u31806_admin_bvt', 'u31806_grantee_bvt');
-- @session
