# **文档贡献指南**

欢迎对MatrixOne文档的提出贡献。MatrixOne社区一直在努力简化整个贡献流程，为此，我们创建本节来一步步地指导您完成文档贡献的过程。

## **准备工作**

开始之前请尽量熟悉基本的[Markdown](https://www.markdownguide.org/basic-syntax/)语法并阅读[行为守则](../Code-Style/code-of-conduct.md)和[谷歌开发者文档风格指南](https://developers.google.com/style/)，以便您写出更高质量的文档。

## **文档管理逻辑**

MatrixOne文档通过三个仓库来协调管理：
 
* [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io)仓库包含文档框架（配置文件）和CI设置  

* [matrixone](https://github.com/matrixorigin/matrixone)仓库中的`docs/en/MatrixOne`及`docs/cn/MatrixOne`文件夹包含文档的具体内容（.md文件）

* [artwork](https://github.com/matrixorigin/artwork) 仓库包含了文档所用到的图像等非结构性文件

`matrixorigin.io`仓库中有一个子模块`submodule`，其引用了`matrixone`仓库中的文档内容，当`matrixone`仓库的内容更新时，`submodule`也会同步更新；而图像等非结构化文件则直接引用`artwork`仓库的网站链接，如：

```
https://github.com/matrixorigin/artwork/blob/main/docs/overview/overall-architecture.png?raw=true
```

`matrixorigin.io`部署了一个CI程序，当有新的代码被合并时将自动触发，将文档发布到我们的[官方文档网站](https://docs.matrixorigin.io/)。  
我们的文档是基于 [mkdocs-material](https://github.com/squidfunk/mkdocs-material)组件进行开发的，您可以在此链接中了解更多信息。

## **文档内容架构**

MatrixOne文档内容可以分为如下几个模块：

* **Overview**: MatrixOne的简介，包含了项目特点、架构、设计思路和技术细节。

* **Get Started**: 介绍如何在单机或分布式环境中快速部署和运行MatrixOne。

* **Reference**: 包括SQL参考指南、配置参数设置、错误代码。

* **FAQs**: 关于产品、技术设计、SQL、部署的常见疑难问题。

* **Release Notes**: 所有版本的发布说明。

* **Contribution Guide**: 介绍如何为MatrixOne项目做出贡献。

* **Glossary**: 名词释义表。

## **简易的修改**

如果您发现了错别字或语法错误，可以点击本页面的`Edit this Page`按键直接进行修改。

## **一般工作流程**

当您需要更改文档的具体内容但不涉及章节顺序、架构组织的调整时，只需要对`matrixone`进行操作；反之，若您需要调整文档框架，改动`mkdocs.yml`文件时，则需要对`matrixorigin.io`库进行修改。
以下流程演示的是对二者均做修改的情况，实际情况可以根据您的需求进行简化。

**1.** 在GitHub上[提出issue](https://github.com/matrixorigin/matrixone/issues/new/choose)，简单介绍您发现的问题。并且在issue下面评论认领该问题。

**2.** Fork [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io) 和 [matrixone](https://github.com/matrixorigin/matrixone) 仓库.

**3.** 克隆[matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io)仓库，使用`--repository`来引用`matrixone`中的内容。

```
git clone --recursive git@github.com:yourusername/matrixorigin.io.git
```

克隆[matrixone](https://github.com/matrixorigin/matrixorigin.io)仓库。

```
git clone git@github.com:yourusername/matrixone.git
```

**4.** 在您的本地matrixone文件夹中将`matrixone`仓库添加为远程仓库。

```
git remote add upstream https://github.com/matrixorigin/matrixone.git
```

在您的本地matrixorigin.io文件夹中将`matrixorigin.io`仓库添加为远程仓库。 

```
git remote add upstream https://github.com/matrixorigin/matrixorigin.io.git
```

**5.** 本地的matrixorigin.io文件夹中将包含文档所需要的全部文件，因此您可以运行 `mkdocs serve` 命令, 然后在`http://localhost:8000`网址中预览文档，检查整个项目文件是否可以正常运行，并且后续也可以检查您所做的修改是否正确。

```
mkdocs serve
```

**6.** 进行文档的修改和完善，如果您想对项目的设置进行改动，或者添加新的page来更新sitemap，或更新 CI&CD 工作流代码，您也可以通过`http://localhost:8000`来查看您的修改是否有效。  
如果您对文档内容（.md文件）进行了修改，则需要同步地在`matrixone`仓库中进行改动，否则仅仅更新`matrixorigin.io`仓库中的`docs`子模块是无用的。

**7.** 确认修改无误后，使用`git add`和`git commit`命令在本地提交修改，并推送至您Fork的远程仓库`matrixorigin.io` 与 `matrixone` 。  
我们建议您推送至远程仓库的新分支：

```
git push origin main:NEW_BRANCH
```

**8.** 在Github上相应仓库的`NEW_BRANCH`分支提交Pull Request。 

**9.** 一旦您的修改通过，CI 工作流将开始运行并更新文档网站，这可能需要一些时间。

**10.**  最后，还有一些操作可以帮助保持MatrixOne远端仓库，您的远端仓库和本地仓库均保持一致。  

覆盖本地提交历史： 

```
git pull --force upstream main:main
```

更新Github上的`main`分支：

```
git push --force origin main:main
```

!!! info 注意
    若您在两个仓库都做了修改，那么以上大部分操作都需要分别针对两个仓库都执行一遍。  

## **写一篇博文**

如果您有意写一篇关于MatrixOne的博文，请在GitHub上提出[issue](https://github.com/matrixorigin/matrixone/issues/new/choose) ，或者将您的想法发送到[dengnan@matrixorigin.io](mailto:dengnan@matrixorigin.io)，无论是简单的idea还是完整的草案，我们统统接受。我们会尽快审查所有内容，如果您的文章或想法很契合我们的博客，我们会直接联系您。
