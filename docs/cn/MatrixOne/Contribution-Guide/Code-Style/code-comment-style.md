# **代码注释规范**

本文描述了MatrixOne所使用的代码注释的规范和样式。当你提交代码时，请务必遵循已有的代码注释规范。

## 为什么注释很重要?

- 加快了代码审查流程
- 有助于维护代码
- 提高API文档的可读性
- 提高整个团队的开发效率

## 什么时候需要注释?

以下类型的代码时很有必要做出注释：

- 关键性代码
- 艰深晦涩的代码
- 复杂却有趣的代码
- 如果代码存在错误但您无法修复，或者只想暂时忽略
- 如果代码并非最优，但你现在没有更好的方法
- 提醒自己或其他人代码中存在缺失的功能或即将开发的功能

以下部分也需要进行注释：

- Package (Go)
- File
- Type
- Constant
- Function
- Method
- Variable
- Typical algorithm
- Exported name
- Test case
- TODO
- FIXME

## 如何进行注释?

### 格式

- Go   
    - 使用 `//` 来进行单行注释
    - 使用 `/* ... */` 对代码块进行注释
    - 使用 **gofmt** 来格式化你的代码

- 把单行注释、代码块注释放在代码上方
- 折叠多列注释
- 注释中的每行文本不超过100个词

- 包含URL的注释：
    - 如果文本链接到同一个GitHub存储库中的文件，则使用 **relative URL** ，
    - 如果带有此注释的代码是从另一个存储库复制来的，则使用 **absolute URL** ，

### 语言

- 单词
    - 请统一使用 **美式英语**       
        - color, canceling, synchronize     (推荐)
        - colour, cancelling, synchronise   (不推荐)

    - 注意拼写正确

    - 使用**标准或官方的大写**

        - MatrixOne, Raft, SQL  (正确)
        - matrixone, RAFT, sql  (错误)

    - 使用一致性的短语和词汇

        - "dead link" vs. "broken link" （在一篇文章或文件中只能出现其中一个）

    - 不要使用冗长的复合词

    - 尽量不使用缩写

    - 用 *We* 用来代指作者和读者

- 句子

    - 使用标准的语法和标点符号
    - 尽量使用短句

- 句子首字母大写，并以句号结尾

    - 如果一个小写的标识符位于句子开头，可以不用大写

        ```
        // enterGame causes Players to enter the
        // video game, which is about a romantic
        // story in ancient China.
        func enterGame() os.Error {
            ...
        }
        ```

- 当注释用来描述代码时，应该保证使用 **描述性语句** 而非 **祈使句**

    - Opens the file   (正确)
    - Open the file    (错误)       

- 使用 "this" 而非"the"来指代当前事物

    - Gets the toolkit for this component   (推荐)
    - Gets the toolkit for the component    (不推荐)

- 允许使用Markdown语法格式

    - Opens the `log` file  

### Tips

- 尽量在写代码的同时就完成注释，减少事后返工
- 不要假设代码是不证自明的
- 对于简单代码尽量避免使用过多注释
- 有“代入感”地做注释（仿佛在进行对话一般）
- 确保及时更新注释
- 保证注释清楚易懂

感谢您的贡献!
