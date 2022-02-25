# **Make a Design Proposal**
Many changes, including bug fixes and documentation improvements, can be implemented and reviewed via the normal GitHub pull request workflow, which we have already introduced in the previous chapter.  
Differently, when you come up with a new feature you need or expect MatrixOne to achieve, we encourage you to propose your idea and express it as technique design documents. 
Therefore, this page is intended to guide you to provide a consistent and controlled path for new features to enter the MatrixOne projects, so that all stakeholders can understand fully enough about the direction the project is evolving in.
This page defines the best practices procedure for making a proposal in MatrixOne projects.

## **Before writing a design document**
Making enough preparations ahead of the design document can not only promote your working efficiency but increase the likelihood to be accepted. Oppositely, a rough and casual design document may be so much solow-quality so that it'll be rejected quickly.  
We motivate you to ask for help from experienced developers in order to obtain valuable suggestions to frame your design architecture or fill in details, which undoubtedly will upgrade the document to be desirable.

The most common channel for preparing for writing a design document is on the Github issue. You can file a `Feature Request` or `Refactoring Request` to discuss about your ideas.


## **Process**
Generally, you can follow these steps to finish the whole process:  

* Create an issue describing the problem, goal, and solution.
* Get responses from other contributors to see if your proposal is generally acceptable and whether or not you should make some modifications.
* Create a pull request with a design document based on the [design template](https://github.com/matrixorigin/matrixone/blob/main/docs/rfcs/00000000-template.md).
* Make conversation with reviewers, revise the text in response.
* The design document is accepted or rejected when at least two committers reach consensus and no objection from the committer.  
* If accepted, create a tracking issue for the design document or convert one from a previous discussion issue. The tracking issue basically tracks subtasks and progress. And refer the tracking issue in the design document replacing placeholder in the template.
* Merge the pull request of design.
* Start the implementation.

Please refer to the tracking issue from subtasks to track the progress.