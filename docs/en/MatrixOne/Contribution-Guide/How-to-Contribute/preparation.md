# **Preparation**

Before contributing, it's necessary to make some preparations to learn more about MatrixOne and other prerevalant information which can improve developing and reviewing efficiency.

Thanks for understanding that this repository uses English as a shared language. Maintainers do not use machine translation to avoid miscommunication due to errors in translation. If the description of an issue / PR is written in non-English languages, it may be closed. If English is not your native language and you aren't sure about any of these, don't hesitate to ask for help in your pull request!

## **What is MatrixOne**

### Basic Information

You can see [MatrixOne Introduction](../../Overview/matrixone-introduction.md) for key features of MatrixOne, [MatrixOne Architecture](../../Overview/matrixone-architecture.md) for MatrixOne architecture, and operational logic.

Besides, you can browse [SQL Reference](../../Reference/SQL-Reference/Data-Definition-Statements/create-database.md) and [Custom Functions](../../Reference/Builtin-Functions/Datetime/year.md) to know more about our interactive details.

These introductions will help you go through you the key concepts and user details to understand the project.

### Roadmap

MatrixOne v0.3.0 has been released, you can see [Release Notes](../../Release-Notes/v0.4.0.md) know more information.

Currently, we are working on v0.4.0 and v0.5.0 releases, the job tasks of these releases are listed in the Github milestones [0.4.0](https://github.com/matrixorigin/matrixone/milestone/5).

For the long-term project roadmap, please refer to [MatrixOne roadmap](https://github.com/matrixorigin/matrixone/issues/613) for a more general overview.

## **What Contributions Can I Make**

Contributions to MatrixOne are not limited to writing code. What follows are different ways to participate in the MatrixOne project and engage with our vibrant open-source community. See [Types of Contributions](types-of-contributions.md) for more details.  

* To report a bug or a problem with the documentation, please file an [issue](https://github.com/matrixorigin/matrixone/issues/new/choose) providing the details of the problem. Don't forget to add a proper label for your issue, and follow the [issue templates](report-an-issue.md#issue-templates).  
* To propose a new feature, please file a [new feature request](https://github.com/matrixorigin/matrixone/issues/new/choose). Describe the intended feature and discuss the design and implementation with the team and community. Once the team agrees on the plan, you can follow the [Contribution Workflow](contribute-code.md#workflow) to implement it.  
* To implement a feature or bug-fix for an existing outstanding issue, follow the [Contribution workflow](contribute-code.md#workflow). If you need more context on a particular issue, comment on the issue to let people know.

## **Working Directories and Files**

For contributing code, [matrixone](https://github.com/matrixorigin/matrixone) is the main repository you'll be working on. The main working directories are listed below:

| Directory              | Working Files                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| **/LICENSES** | The license of dependant libraries |
| **/cmd** | The binary entry of Go executables  |
| **optools** | The test and deployment utilities  |
| **pkg** | The main codebase of MatrixOne project  |

For different technical modules, a code structure under `/pkg` is as shown in the following table.  

| Directory              | Modules                                                 |
| ------------------------------ | ------------------------------------------------------------ |
| **frontend/** | SQL Frontend |
| **sql/parser** | SQL Parser  |
| **sql/** | MPP SQL Execution  |
| **sql/vectorize** | Vectorization of SQL Execution   |
| **catalog/** | Catalog for storing metadata  |
| **vm/engine** | Storage engine  |
| **vm/engine/aoe** |  Analytics Optimized Engine  |
| **vm/engine/tpe** |  Transaction Processing Engine  |
| **buildin/** |  System builtin functions  |

For contributing documentation, [matrixone](https://github.com/matrixorigin/matrixone), [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io) and [artwork](https://github.com/matrixorigin/artwork) are all the main repositories you'll be working on. For more details, please refer to [Contribute Documentation](contribute-documentation.md).

| Directory              | Working Files                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| **matrixone/docs/en/MatrixOne** | The content files of MatrixOne documentation website  |
| **matrixone/docs/rfcs** | The design docs of MatrixOne project |
| **matrixorigin.io/mkdocs.yml** | The configuration file of the documentation website |
| **artwork/docs** | The images, screenshots and diagrams of documentation webstie |

## **Set up your Development Environment**  

### **Go Environment**

MatrixOne is written in Go. Before you start contributing code to MatrixOne, you need to set up your GO development environment.

1. Install `Go` version **1.18**. You can see [How to Write Go Code](http://golang.org/doc/code.html) for more information.  
2. Define `GOPATH` environment variable and modify `PATH` to access your Go binaries. A common setup is as follows. You could always specify it based on your own flavor.

```sh
export GOPATH=$HOME/go  
export PATH=$PATH:$GOPATH/bin
```

!!! Note  
    MatrixOne uses [`Go Modules`](https://github.com/golang/go/wiki/Modules) to manage dependencies.

### **Github & Git**

MatrixOne is an open-source project built on Github, providing project maintainers and contributors with a platform to work together. Thus, in order to start working with MatrixOne repository, you will need a **GitHub** account and learn basic **git** commands.   
If you don't have a Github account, please register at [https://github.com](https://github.com). In case you do not have [SSH](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/about-ssh) keys, you should generate them and then upload them on GitHub. It is required for sending over your patches. It is also possible to use the same SSH keys that you use with any other SSH servers - probably you already have those.  
For detailed information about Github, you can see [Github Docs](https://docs.github.com/en).  

To work with git repositories, please [install git](http://git-scm.com/downloads).
And you can learn how to use it throuth following introduction:  

* A brief manual can be found [here](https://education.github.com/git-cheat-sheet-education.pdf).
* A detailed manual can be found [here](https://git-scm.com/book/en/v2).

### **Install and Run MatrixOne**

See [Install Standalone MatrixOne](../../Get-Started/install-standalone-matrixone.md) for more details.
