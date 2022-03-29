# **提出问题**
您在使用或开发MatrixOne过程中遇见的任何问题都能以[issues](https://github.com/matrixorigin/matrixone/issues/new/choose)的形式提出来，我们也鼓励您按照我们设定的模板和标签对issues进行详细描述和分类，以便更高效地解决问题。  
本节旨在介绍提出问题时需要遵循的模板、标签和注意事项。


## **避免重复问题**

在提出issue之前，尽量先确认其他人是否已经提出过相同或类似的问题，避免重复，您可以使用[search bar 工具](https://docs.github.com/en/issues/tracking-your-work-with-issues/filtering-and-searching-issues-and-pull-requests)帮助您筛选查找。

## **模板**
针对不同种类的问题，MatrixOne使用了不同的模板对其内容进行刻画，其中大多描述了问题的关键信息，有助于审查者与其他开发者理解并参与其中。  

例如，`bug report`问题模板包含以下信息：

* **Detail Environment**  
  Describe the details about the operating environment including version, hardware parameters, OS type and so on.
* **Steps to Reproduce**  
  List steps to reproduce what you encountered.
* **Expected & Actual Behavior**  
  Describe what's the observed and your expected behavior respectively.


`Enhancement` 问题模板包含以下信息：

* **What would you like to be added**  
A concise description of what you're expecting/suggesting.
* **Why is this needed**  
A concise description of the reason/motivation.
* **Anything else**  
Anything that will give us more dectail about your issue!

`Feature Request`问题模板包含以下信息： 

* **Is your feature request related to a problem?**  
A clear and concise description of what the problem is and state your reasons why you need this feature.
* **Describe the feature you'd like:**  
A clear and concise description of what you want to happen.
* **Describe alternatives you've considered:**  
A clear and concise description of any alternative solutions or features you've considered. 
* **Teachability, Documentation, Adoption, Migration Strategy:**  
If you can, explain some scenarios how users might use this, situations it would be helpful in. Any API designs, mockups, or diagrams are also helpful. 

`Performance Question`问题模板包含以下信息：

* **Detail Environment**  
  Describe the details about the operation environment including version, hardware parameters, OS type and so on.
* **Steps to Reproduce**  
  List steps detailedly to reproduce the operations to test performance.
* **Expected & Actual Performance**  
  Describe what's the observed and your expected performance respectively.
* **Additional context**  
  Add any other context about the problem here. For example:  
    * Have you compared TiDB with other databases? If yes, what's their difference?

`Documentation Issue`问题模板包含以下信息：  

* **Describe the issue**  
  A clear and concise description of what's wrong in documentation.
* **Additional context**  
  Add any other context about the problem here.

`Refactoring Request`问题模板包含以下信息：

* **Is your refactoring request related to a problem?**  
A clear and concise description of what the problem is.
* **Describe the solution you'd like**  
A clear and concise description of the refactoring you want to.
* **Describe alternatives you've considered**  
A clear and concise description of any alternative solutions or refactoring method you've considered.
* **Additional context**  
Add any other context or screenshots about the refactoring request here.



## **标签**

除了描述问题的详细信息外，您还可以根据问题所属的组件以及问题所属版本为其添加适当的标签。当您的issue提交之后，会自动打上`needs-triage`的标签，之后项目维护者会详细阅读您的issue，然后为之打上合适的标签并分配给合适的开发者。  
如果您想自己亲手处理这个问题，可以在评论中提出，社区管理员会给您分配这个issue。若在Assignees部分可以看见您自己，说明操作成功。

## **Good First Issues**

当您首次参与贡献时,您可以选择[`good-first-issue`](https://github.com/matrixorigin/matrixone/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)标签下的问题着手解决，其下的每个问题都是相对来说容易解决的。  
详情请阅读[快速上手](../make-your-first-contribution.md)章节。 

