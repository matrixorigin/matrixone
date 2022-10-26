# Privilege Control in MatrixOne - Access Control Overview

MatrixOne 的权限控制是结合了基于角色的访问控制 (RBAC，Role-based access control) 和自主访问控制 (DAC，Discretionary access control) 两种安全模型设计和实现的，它既保证了数据访问的安全性，又给数据库运维人员提供了灵活且便捷的管理方法。

MatrixOne's permission control is designed and implemented by combining two security models of role-based access control (RBAC, Role-based access control) and discretionary access control (DAC, Discretionary access control), which ensures the security of data access, and it provides a flexible and convenient management method for database operation and maintenance personnel.

- Role-based access control: Assign permissions to roles, and then assign roles to users.

- Discretionary Access Control: Every object has an owner who can set and grant access to that object.

## Basic concepts

### Object

An object is an entity that encapsulates permissions in MatrixOne, and these entities have a certain hierarchical structure. For example, a **Cluster** contains multiple **Account** (Account), and a **Account** contains multiple **User**, **Role** , **Database**, and a **Database** contains multiple **Table**, **View, etc. This hierarchical structure is shown in the following figure:

- Each object has one and only one **Owner**; otherwise, the **Owner** has the **Ownership** permission of the object.

   !!! note
        - Owner is for a specific object rather than a class of objects. For example, Role_1 is the Owner of db1, Role_2 is the Owner of db2, and Role_1 and db2 do not have a necessary permission relationship.
        - The Owner of an upper-level object does not necessarily have access rights to the lower-level object. For example, if Role_1 is the Owner of db1, and Role_2 created db1.table1, then Role_2 is the Owner of db1.table1, and Role_1 cannot view its data.

- The initial Owner of an object is its creator, and the Owner identity can be transferred by the Owner itself or an owner with advanced control.
- If the object owner is deleted, the object's owner is automatically changed to the owner of the deleted object.
- The ACCOUNTADMIN role has Ownership permissions on all objects by default.
- The set of access permissions for each object is different. For more information, please refer to [Access Control Permissions](access-control.md).

### Role

A role is an entity with access rights. Any database user who wants to access an object must first be granted a role that has access rights to the object. The roles of MatrixOne are divided into system roles and user-defined roles. When the system is first started, or a new account is created, some system default roles will be initialized. They are often the highest administrator roles and users and cannot be modified or deleted. Use them to create more custom roles to manage the database as needed.

- A role can be granted multiple access rights to an object.
- A role can be granted access to multiple objects.
- Permissions can be passed between roles, usually there are two methods of **Grant** and **Inheritance**.

   + **授予**：权限的授予具有一次授予永久生效的特性。例如*角色1*拥有权限 a， b， c； *角色2*拥有权限 d，此时将*角色1*的权限授予给*角色2*，则*角色2*拥有权限 a， b， c， d，当删除 角色1 后， *角色2*仍拥有权限 a， b， c， d。
   + **继承**：权限的继承具有动态传递的特性。例如*角色1*拥有权限 a， b， c； *角色3*拥有权限 e， f，可以指定*角色3*继承*角色1*，则*角色3*拥有权限 a， b， c， e， f； 当删除*角色1*后，*角色3*仅拥有权限 e， f。
   + Grant: The grant of permission has the characteristic of granting a permanent effect. For example, role 1 has permissions a, b, c, and role 2 has permission d. At this time, if the permissions of role 1 are granted to role 2, then role 2 has permissions a, b, c, and d. When role 1 is deleted, role 2 Still have permissions a, b, c, d.
   + Inheritance: Inheritance of permissions has the characteristics of dynamic transfer. For example, role 1 has permissions a, b, c, role 3 has permissions e, f, you can specify role 3 to inherit role 1, then role 3 has permissions a, b, c, e, f, when role 1 is deleted, role 3 Only have permissions e, f.

     !!! note
          1.Manipulating granted and inherited roles requires Ownership permission on the object or one of the advanced grant permissions.
          2.The inheritance relationship of roles cannot be looped.

- 在 MatrixOne 中，实现了租户间权限隔离，即 Account 内的角色和用户仅在该 Account 内生效，不会影响和传递给其他 Account，角色之间的授权也仅限在 Account 内。

### Switching Role

When a user wants to gain access to an object, he must first grant permission to a role and then grant the role to the user. A user can have multiple roles, but at the same time, the user can only use one role to access the database, we call this role the **primary role**, and the rest of the roles are **secondary roles**. When a user executes a specific SQL, the system will determine whether the primary role has the permissions required to execute the SQL and then decide whether to execute the SQL.

In some scenarios (for example, administrators do not have a well-established role system), SQL needs to be executed in combination with the permissions of multiple roles. MatrixOne provides the function of using secondary roles to help such users complete queries:

```sql
use secondary role { all | none }
```

The default parameter is `all`. If you choose to use `all`, the permissions of all secondary roles can be provided to the user; if you choose to use `none`, only the permissions of the primary role can be used.

### Deleting Object

When deleting a tenant, you need to close or suspend the tenant first, that is, you need to execute the SQL statement `close` or `suspend`.
When deleting a role, it will force all authorized role revoke users, that is, you need to execute the SQL statement `revoke`.
When deleting a user, the deletion of the user fails if the user currently has a session.

## Initialize access control

For the convenience of management, after the cluster or account is created, the system will automatically generate some **default users** and **default roles**.

#### Default users

|	Username| Description|
|---|---|
|root|After the cluster is created, it is automatically created and granted the MOADMIN role, which cannot be modified or deleted.|
|admin|Account is automatically created after creation and is granted the ACCOUNTADMIN role, which cannot be modified or deleted.|

#### Default roles

|	Username| Description|
|---|---|
|MOADMIN|The super administrator role is created automatically after the cluster is initialized and cannot be modified or deleted.|
|ACCOUNTADMIN|Account is created automatically after initialization and is the super administrator role of the ACCOUNT|
|PUBLIC|After the Account is created, it is automatically created. All users under the Account will be added to the PUBLIC role when they are created, and users cannot be removed. The initial permission of the PUBLIC role is CONNECT.|
