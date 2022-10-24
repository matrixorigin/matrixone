# **SET ROLE**

## **语法说明**

设置会话的活动/当前主要角色。为当前活动的主角色设置上下文，以确定当前用户是否拥有执行 `CREATE <object>` 语句或执行任何其他 SQL 操作所需的权限。

注意，除了创建对象之外，任何 SQL 操作的授权都可以由次级角色执行。

## **语法结构**

```
> SET SECONDARY ROLE {
    NONE
  | ALL  
}
SET ROLE role
```

### 语法说明

#### SET SECONDARY ROLE ALL

将该用户所有的 ROLE 取并集。

#### SET SECONDARY ROLE NONE

将除 PRIMARY ROLE 之外的所有角色从当前会话中去除。

#### SET ROLE role

将当前角色切换为新角色。

## **示例**

```sql
