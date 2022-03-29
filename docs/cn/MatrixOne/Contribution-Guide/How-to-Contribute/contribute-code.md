# **代码贡献**
MatrixOne是一个由项目管理者、社区开发者共同维护、改进和扩展的开源项目。
本文档主要描述了开发的准则与一般流程，并提供了在编写代码、提交PR过程中需要使用的样式和模板。如果您在参与MatrixOne的贡献过程中遇到任何问题或发现一些错误，请在Github上提出[issue](https://github.com/matrixorigin/matrixone/issues) 或通其他平台联系我们。

## **前置准备**
在正式开发之前，请确保您已经阅读了[准备工作](preparation.md), 已经对MatrixOne的核心理念、基础架构有一定了解，并准备好了开发所需要的相应环境、语言、工具等。


## **风格规范指南** <a name="get-familiar-with-style"></a>
在对MatrixOne进行开发和完善时，应该使代码、代码注释、提交信息(Committing Message)和拉取请求(Pull Request，简称PR)保持一致的风格。当您提交PR时，我们强烈建议您确保所作出的修改符合我们的一贯风格，这不仅会提高PR的通过率，并且也能使MatrixOne易于审查、维护和进一步开发。

* **代码规范**  
MatrixOne采用了Golang社区建议的编码规范，详情请见 [Effective Go](https://go.dev/doc/effective_go)。

* **代码注释规范**
关于代码注释，请参考[代码注释规范](../Code-Style/code-comment-style.md)。

* **提交信息 & PR 规范**  
可参考[Commit&PR规范](../Code-Style/code-comment-style.md)。

## **一般工作流程<c name="workflow"></c>**
您可以按照以下工作流程来进行开发并在Github上提交修改，如果您还需要更加详细的解释，可以查看[Make Your First Contribution](../make-your-first-contribution.md)

**1.** 在Github上 Fork [matrixorigin/matrixone仓库](https://github.com/matrixorigin/matrixone).

**2.** 将 Fork 的仓库克隆至本地:  
```
git clone git@github.com:<yourname>/matrixone.git
```    
并且把matrixone仓库添加为远程仓库:  
```
git remote add upstream https://github.com/matrixorigin/matrixone.git
```  
**3.** 创建一个新的分支，分支名自定义：
```
git checkout -b topic-branch
``` 
**4.** 在本地进行开发，完成相关修改，并完成必要的单元测试，最后进行提交。 

**5.** 将修改推送至仓库的一个新分支:
```
git push origin main:NEW_BRANCH
```  
**6.** 在仓库中的新分支`NEW_BRANCH`中创建 Pull Request，并添加相应标签、[建立与相关issue的关联](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue)。
您
**7.** PR通过后，覆盖本地提交历史：
```
git pull --force upstream main:main
```  
**8.** 更新您的仓库的`main` 分支：
```
git push --force origin main:main
```

如果您仍然有一些困惑，可以参考 [GitHub官方文档](https://docs.github.com/en) 寻求帮助；若您发现我们提供的工作流程有错误或想要提出改善的方法，欢迎您[提出建议](https://github.com/matrixorigin/matrixone/issues/new/choose)！

## **代码审阅**
当您创建PR请求时，您可以指定一些审阅者，或者留空。并且您可以添加一些相关的标签，这样更容易识别PR的类型、优先级等。在代码审阅期间，审阅者会对您的代码片段给出意见，您可以相应地在本地修改您的分支上的代码，提交更改，然后推送到GitHub，新的提交会自动附加到PR上。

