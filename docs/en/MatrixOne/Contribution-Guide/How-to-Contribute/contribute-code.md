# **Contribute Code**

MatrixOne is an open-source project maintained, improved, and extended by code contributors.  
This document describes the conventions of development workflow, and some styles and templates you need while contributing code to MatrixOne. If you encounter any problems or find some mistakes in participating in contribution to MatrixOne, please [file an issue](https://github.com/matrixorigin/matrixone/issues) on GitHub or contact us on other platforms we support.

## **Before Contributions**

Before you start developing, make sure you have read [Preparation](preparation.md) where you can follow the instructions to learn more about MatrixOne, development knowledge, and how to set up the development environment.

## **Get familiar with the style** <a name="get-familiar-with-style"></a>

It's necessary to keep a consistent style for code, code comments, commit messages, and pull requests when contributing to MatrixOne. When you put together your pull request, we highly recommend you comply with the following style guides which make MatrixOne easy to review, maintain, and develop.

* **Code Style**  
The coding style suggested by the Golang community is used in MatrixOne. See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

* **Code Comment Style**

  See the [code comment style](../Code-Style/code-comment-style.md) for details.

* **Commit Message & Pull Request Style**

   See the [Commit Message & Pull Request Style](../Code-Style/code-comment-style.md) for details.

## **Workflow<c name="workflow"></c>**

You can follow the workflow to operate development, and you can see [Make Your First Contribution](../make-your-first-contribution.md) if you are a newcomer and need more detailed instructions about workflow.  

*1.* Fork the [MatrixOne repository](https://github.com/matrixorigin/matrixone) on GitHub.
*2.* Clone your fork to your local machine with:  

```
git clone git@github.com:<yourname>/matrixone.git
```

And add MatrixOne repo as a remote repository with:

```
git remote add upstream https://github.com/matrixorigin/matrixone.git
```  

*3.* Create a new branch. The branch name is up to you.

```
git checkout -b topic-branch
```

*4.* Commit changes to your own branch locally, add necessary unit tests.

*5.* Push to a new branch in your own fork.

```
git push origin main:NEW_BRANCH
```

*6.* Go back to GitHub, and submit a pull request in `NEW_BRANCH` so that we can review your changes. Add some labels if needed, and don't forget to [refer to the related issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue).

*7.* Once your PR gets approved, it will be merged sooner. After merging, synchronize your local repository.

```
git pull --force upstream main:main
```

*8.* Synchronized the `main` branch of your remote repository in Github.

```
git push --force origin main:main
```

If you still have some trouble, please refer to [GitHub Docs](https://docs.github.com/en) for help.

## **Code review**

When you open a pull request, you can assign some reviewers, or just leave it blank. And you can add some related labels to make it easier to recognize the PR's type/priority/etc. During the review, reviewers would comment on your code snippet, and you can modify the code on your topic branch locally, commit the changes, and push it to GitHub where the new commits would be attached to the PR automatically.

For detailed code review tips, See [Review a pull request](review-a-pull-request.md) for details.
