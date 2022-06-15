# **快速贡献**

MatrixOne社区欢迎所有开发者的加入和贡献！本章节旨在帮助您快速完成首次贡献。

## **如何贡献?**

在哪些方面可以大展拳脚呢？详情请参见[贡献种类](How-to-Contribute/types-of-contributions.md)。
如果您是完全的新手，您可以从以下两种类别中选择一种进行尝试，这些类别的问题对您技术背景的要求很少，所以不必担心！

* 报告代码中出现的bug
* 完善MatrixiOne文档

在开始处理问题之前，建议您先将找到的问题作为GitHub上的一项issue提出。此外，我们准备了一系列带有`good-first-issue`标签的issue，它们包含了明晰的实现步骤和预期结果，您可以此作为突破口！

## **认领任务**

当您提出issue之后或者是在浏览`good-first-issue`后决定上手解决，您需要认领这次issue。在相应issue的评论中输入如“I'd like to work on this issue”，社区人员会将任务分配给您，此时，您可以在右侧的Assignees板块看见自己，接下来便可以正式着手解决问题。

## **前置准备**

请确保您至少安装了单机版MatrixOne并部署了相关开发环境。具体请参考[准备工作](How-to-Contribute/preparation.md)。

## **工作流程**

### **步骤 1: Fork 项目仓库**

首先前往Github上的[matrixorigin/matrixone](https://github.com/matrixorigin/matrixone)仓库.  
在页面右上角处，点击 `Fork` 按键，创建主库的分叉，并作为您之后主要工作的仓库。  
![Fork our repo](https://github.com/matrixorigin/artwork/blob/main/docs/contribution-guide/contribution-guide-fork.png?raw=true)

### **步骤 2: 将仓库克隆至本地**

前往刚才您创建的Fork仓库，点击 `Code` 按键，然后再点击“复制”图标，将库的网址复制到剪贴板。
![Clone your fork](https://github.com/matrixorigin/artwork/blob/main/docs/contribution-guide/contribution-guide-clone.png?raw=true)  
然后，在您本地挑选一个合适的工作目录，打开命令行输入以下 Git 命令将文件克隆至您本地的目录： 

```
git clone <content you just copied>
```  

例如：   

```
git clone git@github.com:<yourname>/matrixone.git
```

`<yourname>` 是您的Github账号名，换言之，您要用您自己的账号名替换它。

### **步骤 3: 添加 matrixone 仓库作为本地的远程仓库**

您可以将matrixorigin/matrixone 添加为本地的远程仓库，以便后续步骤进行操作：  

```
git remote add upstream https://github.com/matrixorigin/matrixone.git  
```

其中，`upstream` 是该远程仓库的名字，您可以自行替换，但注意后续对该词也应该一并替换。

### **步骤 4: 修改、开发**

#### **修改**

克隆代码库之后您就可以开始您的开发过程，可以在您所需要的地方进行任何修改。

#### **试运行**

如果您在修改完成后想知道改动是否有效，能否解决最初的问题，或者是否影响程序运行，您可以运行MatrixOne进行简单的预览和测试。
当然，请确保您已经按照教程完成了[安装和部署](./../Get-Started/install-standalone-matrixone.md)，并能成功连接到[MatrixOne服务器](./../Get-Started/connect-to-matrixone-server.md)。

### **步骤 5: 提交修改**

当完成以上修改和测试后，您就可以开始提交修改。首先使用将您所更改的文件添加至 git 所管理的目录中：

```
git add <filename>
```

`<filename>` 是您所修改的文件的名称。
您也可以使用如下命令，直接将当前文件夹中的所有文件都添加至管理目录：

```
git add .
```

之后,您可以提交您的修改

```
git commit -m "<commit message>"  -s
```

 `<commit message>`是您对本次修改的简单总结和描述，试着做到简明扼要。
 
### **步骤 6: 代码推送**

提交修改后，您需要将本地的提交推送至远程仓库———我们强烈推荐您推送至目标仓库的一个新分支：

```
git push origin main:NEW_BRANCH
```

`NEW_BRANCH` 您创建并推送至的新分支名，您也可以随意替换它，但也记得后续一并修改。

### **步骤 7 创建PR**

推送后，可以在您所Fork的仓库中看到相关提示信息，点击 **Compare & Pull Request** 按键来创建一个PR（该PR应该是从个人仓库的`NEW_BRANCH` 分支到主库的`main`分支。
!!! Note 注意
    建议按照PR中所给出的模板要求，撰写相关信息，如此可以准确表达您的问题以及所做的修改，从而提高审查效率。

![Pull Request](https://github.com/matrixorigin/artwork/blob/main/docs/contribution-guide/contribution-guide-pr.png?raw=true)
![Pull Request Template](https://github.com/matrixorigin/artwork/blob/main/docs/contribution-guide/contribution-guide-pr-template.png?raw=true)

您的PR一旦创建，就有社区资深的开发人员审核您的代码，他将会检查您做的修改并且进行回复，请及时与您沟通，然后按照要求进行修改。

### **步骤 8 同步代码库**

当进行到这一步时，恭喜您的修改已经被接受并且merge进入项目中，感谢您做出的贡献！  
但工作还没有结束，还有一些工作要做（这些工作有助于保证提交记录的干净，有利于项目进一步发展）。  
可以将远端仓库的代码拉取下来覆盖本地，以保持同步：

```
git pull --force upstream main:main
```

最后，再将代码推送到您的仓库中的`main`分支，这样就能保证三个仓库代码均一致。

```
git push --force origin main:main
```
