# **提交信息&PR规范**

本文档描述了应用于MatrixOrigin的所有存储库的提交消息(commit mesage)和PR(pull request)的样式规范。当你提交代码时，务必遵循这种规范，保证提交的质量。

## **一条好的commit message有多重要？**

- 加速审阅流程
    - 帮助审阅者更好地理解PR内容
    - 可以忽略不重要的信息
- 有助于撰写发布公告
- 帮助其他人了解前因后果

## **什么是好的commit message？**

我们认为有以下要素：

1. **What is your change? (必要)**


    它可能修复了一个特定的bug，添加了一个feature，提高了性能、可靠性或稳定性，或者只是保障安全性而进行的更改。

2. **Why this change was made? (必要)**

    对于简要的补丁，这部分可以省略。

3. **What effect does the commit have? (可选)**

    除了必然会产生的影响之外，可能还包括基准测试性能变化、对安全性的影响等。对于简要的改动，这部分可以省略。

## **如何写好一条commit message**？

要写出一条优质的commit message，我们建议您遵循规定的格式，培养良好的习惯并使用规范的语言。

### **规定的格式**

请在提交时遵循以下格式：

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

+ 第一行
    - 不超过70个字符
    - 如果该改动影响了两个模块，请使用逗号和（带空格）进行分隔，如`util/codec, util/types:`。
    - 如果该改动影响了三个及以上的模块，请使用`*`，如`*:`。
    - 在冒号后的文本中使用小写字母。例如："media: **update** the DM architecture image"
    - 不要在最后添加句号。
- 第二行请留白
- 第三行“why”部分，如果没有特定的原因，您可以使用以下表述，如"Improve performance", "Improve test coverage."
- 其他行不超过80个字符。

### **良好的习惯**

- 进行总结
- 清楚地描述该方案的逻辑，避免`misc fixes`等表达
- 叙述当前方案的限制
- 不要以句号结尾
- 注意代码的证明和测试
- 交代前因后果

### **规范的语言**

- 在第一行使用祈使句
- 使用简单的动词 (如"add" not "added")
- 保证标准无误的语法
- 前后使用的单词、短语保持一致
- 使用短句
- 不要使用过长的复合词
- 非必要不缩写


## **Pull Request规范**

关于Pull Request中的描述，请参考下面的Pull Request模板，涵盖必要信息：


```
**What type of PR is this?**

- [ ] API-change
- [ ] BUG
- [ ] Improvement
- [ ] Documentation
- [ ] Feature
- [ ] Test and CI
- [ ] Code Refactoring

**Which issue(s) this PR fixes:**

issue #

**What this PR does / why we need it:**


**Special notes for your reviewer:**


**Additional documentation (e.g. design docs, usage docs, etc.):**

```

如果需要，您也可以使用清单来列举内容，Markdown语法如下：

```
- [x] A checked line, something already done or fulfilled
- [ ] An unchecked line, something not finished yet
```

对于非常简要的Pull Requests，你可以省略上面的一些信息。
非常感谢您的贡献！
