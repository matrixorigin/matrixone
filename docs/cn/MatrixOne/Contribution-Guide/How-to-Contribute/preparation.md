# **准备工作**

非常欢迎您参与到MatrixOne（以下简称MO）项目的建设中来！无论你是初识MatrixOne，还是已经迫切地想参与到开发工作中来，亦或是在阅读文档、使用产品的过程中发现了一些问题，都欢迎你提出意见和建议，与我们共同打造更加强大、稳定的MatrixOne！
当然，在您向MatrixOne项目提出改进之前，我们需要提前说明一些基本规范与流程，以提高整个贡献过程的质量与流畅性，同时也能保障MO的稳定性与安全性。
此外，在本章中，我们为尚未熟知MO的贡献者们提供一些了解我们的渠道，希望能为你们带来一些便利！

## **了解MatrixOne**

### 特点与框架

在[MatrixOne简介](./../../Overview/matrixone-introduction.md)中您可以了解到MO的超融合、云边协同特性以及其所创造的优异表现与独特价值。
此外，在[MatrixOne框架](./../../Overview/matrixone-architecture.md)中您可以详细地了解 MatrixOne 的整体架构，以及存储层、日志层等具体组成情况。
同时，你也可以查阅[MatrixOne术语表](../../Glossary/glossary.md)来了解一些复杂的词汇。
在技术层面，[SQL参考指南](./../../Reference/SQL-Reference/Data-Definition-Statements/create-database.md) 为您提供了详细的SQL语言的参考，其中对语法和示例都有详细解释；同样，[自定义函数](./../../Reference/Builtin-Functions/Datetime/year.md)提供了MO中自定义函数的相关解释。

### 建设情况

目前，MatrixOne v0.5.1 已经发布了，您可以通过[版本发布指南](./../../Release-Notes/v0.5.1.md)来了解最新的发布信息，其中包含了最新的修改与优化。
同时，我们当前正在开发 v0.6.0 版本，对应的工作任务在GitHub的milestone[0.6.0](https://github.com/matrixorigin/matrixone/milestone/8)中列出。
关于长期的项目规划，请参阅[MatrixOne roadmap](https://github.com/matrixorigin/matrixone/issues/613)。

## **你可以做些什么？**

对MatrixOne的贡献可分为以下几类：

* 报告代码中的bug或文档中的谬误。请在GitHub上提出[issue](https://github.com/matrixorigin/matrixone/issues/new/choose)，并提供问题的详细信息。请记得选取合适的[issue模板](report-an-issue.md)，并打上标签。
* 提议新的功能。请在[Feature Request](https://github.com/matrixorigin/matrixone/issues/new/choose)中描述详情并与社区中的开发人员商议。一旦我们的团队认可了您的计划，您就可以按照[工作流程](contribute-code.md#workflow)进行具体开发。
* 实现某个功能或修复既有问题，请按照[工作流程](contribute-code.md#workflow)完成开发。如果你需要关于某一特定问题的更多背景信息，请就该问题发表评论。

## **工作目录与文件介绍**

我们将为Github上matrixorigin/matrixone的项目目录及其中关键文件进行简单介绍，以期为您的深入了解和开发提供指导性帮助。
[matrixone](https://github.com/matrixorigin/matrixone)是MatrixOne代码所在的主库，我们将介绍其中的项目目录以及关键文件，以期为您的深入了解和开发提供指导性帮助。

| 目录              | 内容                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| **/LICENSES** | 相关依赖库的许可 |
| **/cmd** | Go的可执行文件的binary entry|
| **optools** | 测试与部署工具 |
| **pkg** | MatrixOne项目的主要代码库  |

对于不同的技术模块，`/pkg`喜爱的代码结构如下表所示。

| 目录            | 模块    |
| ------------------------------ | ------------------------------------------------------------ |
| **frontend/** | SQL前端|
| **sql/parser** | SQL解析 |
| **sql/** | MPP SQL Execution  |
| **sql/vectorize** | SQL的向量化执行   |
| **catalog/** | 存储元数据的Catalog |
| **vm/engine** |存储引擎 |
| **vm/engine/aoe** |  AOE引擎（分析优化引擎） |
| **vm/engine/tpe** |  TPE引擎（事务处理引擎）  |
| **buildin/** |  系统的内置函数 |

在文档方面，[matrixone](https://github.com/matrixorigin/matrixone), [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io) 与 [artwork](https://github.com/matrixorigin/artwork)都是在贡献过程中可能使用的库，详情参见[文档贡献](contribute-documentation.md)。

| 目录              | 内容                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| **matrixone/docs/en/MatrixOne** | 文档网站的具体内容文件（.md文件）  |
| **matrixone/docs/rfcs** |MatrixOne项目的设计文档|
| **matrixorigin.io/mkdocs.yml** | 文档网站的配置文件 |
| **artwork/docs** | 文档官网出现的图片和图表|

## **开发环境**  

MO主要由Go语言编写，因此需要提前安装部署好相关的开发环境，简要的示例流程如下：

1. 安装版本为1.18的Go，您可以通过[Download Go](https://go.dev/dl/)与[Installation instructions](https://go.dev/doc/install)教程来完成整个过程。
2. 定义环境变量并修改路径，您可以遵循以下示例流程：

```sh
export GOPATH=$HOME/go  
export PATH=$PATH:$GOPATH/bin
```

!!! Note 提示
    MatrixOne使用[`Go Modules`](https://github.com/golang/go/wiki/Modules) 来管理相关依赖。

若您需要补充Go语言的相关知识，可以通过[How to Write Go Code](http://golang.org/doc/code.html)进行了解。

此外，确保您至少已经安装了单机版本的MatrixOne，具体过程可参照 [Install Standalone MatrixOne](./../../Get-Started/install-standalone-matrixone.md)。

## **Github & Git**

为更好地开发建设MatrixOne，我们采取了开源运营的方式，通过Github为项目维护人员和其他开发者提供了一个协作平台。因此，如果您想参与到MO的开发中来，我们强烈建议您采取Github的渠道。  
若您还未使用过Github或缺少相关开发经验，您首先需要熟悉**GitHub**上的相关操作，并学习基本的**git**命令。  
如果您没有Github帐户，请在[https://github.com](https://github.com)上完成注册。  
如果你没有SSH密钥，你可以按照GitHub上关于[SSH](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/about-ssh)的教程来生成、添加密钥。  
更多详情请参见[Github Docs](https://docs.github.com/en)。   

此外，我们建议您学习并使用**git**命令来完成github上的各种流程，因为我们提供的相关工作流程大多通过**git**命令完成，这有助于您提高效率。  
您可通过[install git](http://git-scm.com/downloads)安装git环境。  
并且可以通过以下教程来学习如何使用：

* [简易版](https://education.github.com/git-cheat-sheet-education.pdf)
* [详细版](https://git-scm.com/book/en/v2)
