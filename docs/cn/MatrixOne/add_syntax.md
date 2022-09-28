本文将通过一个例子讲诉如何在 MatrixOne 中添加语法。

### 语法规则

首先我们可以通过 MySQL 8.0 的文档（<https://dev.mysql.com/doc/refman/8.0/en/clone.html>），CLONE 的语法规则如下：

```sqs q
CLONE clone_action

clone_action: {
    LOCAL DATA DIRECTORY [=] 'clone_dir'
  | INSTANCE FROM 'user'@'host':port
    IDENTIFIED BY 'password'
    [DATA DIRECTORY [=] 'clone_dir']
    [REQUIRE [NO] SSL]
}
```

可以看到 clone_action 可以是：

```
LOCAL DATA DIRECTORY [=] 'clone_dir';
```

或者是：

```
INSTANCE FROM 'user'@'host':port
    IDENTIFIED BY 'password'
    [DATA DIRECTORY [=] 'clone_dir']
    [REQUIRE [NO] SSL]
```

本文会举例添加第一个 clone_action 的规则，在第一个 clone_action 规则中，LOCAL, DATA, DIRECTORY 是 MySQL 中的关键字 （<https://dev.mysql.com/doc/refman/8.0/en/keywords.html>）。 [=] 是可选项，可以有等号或者没有，'clone_dir' 是字符串。对于这些 token， 词法分析阶段都会做区分。

我们可以先定义语法树，因为 CLONE 是新语句，我们可以在 tree 目录创建 clone.go 然后定义：

```
type Clone struct {
	statementImpl
	CloneDir string
}
```

在 mysql_sql.y 中添加如下规则：

```
...
%union {
	// Clone 实现了 statement 接口
	statment tree.Statement
}

...

// 定义终结符 CLONE
%token <str> CLONE

...

// 定义非终结符 clone_stmt
%type <statement> clone_stmt

...

%%
...

// clone_stmt 是 stmt 的具体实现
stmt:
	...
|	clone_stmt

// 定义 CLONE 语法规则
clone_stmt:
	CLONE LOCAL DATA DIRECTORY equal_opt STRING
	{
		$$ = &tree.Clone{IsLocal: true, CloneDir: $6}
	}

...

non_reserved_keyword:
	...
|	CLONE
	...

...

%%
```

其中 LOCAL, DATA, DIRECTORY 关键字是已经定义好的。

只需要定义新的关键字 CLONE，可以参考其中一个是怎么定义的。注意要在 MySQL 文档 <https://dev.mysql.com/doc/refman/8.0/en/keywords.html> 中查看，是保留关键字，还是非保留关键字。然后在 keywords.go 中添加：

```
	keywords = map[string]int{
		...
		"clone":               CLONE,
		...
	}
```

让词法分析器通过 map 识别 CLONE 为关键字。

STRING 表示字符串，会在词法分析中区分。equal_opt 表示 [=]，可以有等号，或者没有：

```
equal_opt:
    {
        $$ = ""
    }
|   '='
    {
        $$ = string($1)
    }
```

## 生成解析器

MO parser 写了 Makefile ，通过 goyacc 生成语法分析器。可以直接进入到 parsers 目录下，直接

```
make
```

就会生成新的语法分析器 (mysql_sql.y)。注意： 当 make 后报 shift/reduce 或者 reduce/reduce 冲突，表示编写的语法规则有问题，需要修改。

## Format

format 会将 ast 转化为 SQL 字符串， 主要作用是方便测试，在 plan 的构造中也会用到。COLNE 的 format 函数如下：

```
func (node *Clone) Format(ctx *FmtCtx) {
	ctx.WriteString("clone")
	if node.IsLocal {
		ctx.WriteString("local data directory = ")
		ctx.WriteString(node.CloneDir)
	}
}
```

## 测试

MO parser 的测试主要是单侧，我们可以在 mysql_test.go 中添加

```
validSQL = []struct {
		input  string
		output string
	}{{
		input:  "CLONE LOCAL DATA DIRECTORY = '/tmp'",
		// 因为在 Format 函数中，关键字都被固定为小写，所以 Format 后也会变成小写
		output: "clone local data directory = /tmp",
	}, ...
```

然后跑 TestValid 测试。

如果只是单纯地添加语法，首先在构造执行计划时，需要抛出功能不支持错误。可以调用 moerr 抛出

```
func NewNotSupported(msg string, args ...any) *Error
```

然后需要在 test 目录下添加 bvt 测试，这是一个端到端的测试，预期结果是抛出相应的错误，或正确的结果集。最后可以用 mo-tester (使用可以看 readme )作为检验。

到此，为 MO parser 添加一个简单的语法成功，在 MO 最新的代码中，该语法还未被添加，大家可以尝试验证这个语法是否能解析成功，或者添加其他新的语法。
