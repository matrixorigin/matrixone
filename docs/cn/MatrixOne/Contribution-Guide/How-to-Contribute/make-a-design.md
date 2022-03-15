# **提交设计方案**
前面章节提到了很多种类的修改，比如bug修复、文档完善，这些都可以通过GitHub的PR工作流程来实现；但与此不同的是，如果您想要在MatrixOne中实现新的功能或增添新的组件，都不仅仅是一个idea这么简单，我们鼓励您提出想法的同时还制定相应的设计方案，将其表达为技术设计文档。
因此，本节的目的正是引导您撰写一份技术设计文档，以期可以为这个新功能提供一个权威的、大众的理解渠道，各方人员可以更深入地了解这个模块的核心理念与发展方向。

## **准备工作**

与其他工作一样，在开始之前，尽量做足准备，这样不仅可以提高你的工作效率，还可以增加通过的可能性；相反，一份粗糙而随意的设计文件可能会因为质量太差而吃到闭门羹。
我们鼓励您向有经验的开发人员寻求帮助，通过他们的建议您可以修正设计架构并完善技术细节。
编写设计文档的最常见渠道是Github，你可以在此处提交一个`Feature Request` 或 `Refactoring Request`来向大家展示你的想法。


## **一般流程**
通常，从头到尾地完成一项技术设计需要以下步骤：

* 在Github上提出issue，描述该功能想要解决的问题、目标、大致解决方案。
* 在得到管理者与其他开发者的回应，听取相关建议，然后对你的想法做进一步修改。
* 按照[模板,需要给出链接](xxxxxx)撰写技术设计文档并创建PR。
[此处为tidb给出的模板](https://github.com/pingcap/tidb/blob/7f4f5c02364b6578da561ec14f409a39ddf954a5/docs/design/TEMPLATE.md)
* 与审查者交流，并按要求做相应的修改。
* 当至少有两个提交者达成一致且其他提交者没有异议时，设计文件即被接受，反之亦然。The design document is accepted or rejected when at least two committers reach consensus and no objection from the committer.  **(我们的要求是怎样的)**
* 如果提议被接受，那就请为该提议创建一个[跟踪性议题(tracking issue)](https://github.com/matrixorigin/matrixone/issues/new)，或者将之前讨论的议题转换为跟踪议题，后续不断跟踪子任务和开发进度。并设计文档中替换模板中的占位符的跟踪问题。 And refer the tracking issue in the design document replacing placeholder in the template. 
* Merge the pull request of design.
* Start the implementation.
Please refer to the tracking issue from subtasks to track the progress.