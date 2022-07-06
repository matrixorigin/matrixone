# Contributing to MatrixOne
MatrixOne is an open source project, and you can make contributions in lots of ways. You can help with ideas, code, or documentation, any contributions are welcome.

Thanks for understanding that English is used as a shared language in this repository. Maintainers do not use machine translation to avoid miscommunication due to error in translation. If description of issue / PR are written in non-English languages, those may be closed. If English is not your native language and you aren't sure about any of these, don't hesitate to ask for help in your pull request!

## What contributions can I make
Contributions to MatrixOne fall into the following categories.
* To report a bug or a problem with documentation, please file an [issue](https://github.com/matrixorigin/matrixone/issues/new/choose) providing the details of the problem. Don't forget to add some proper labels, and follow the issue template.
* To propose a new feature, please file a new feature request [issue](https://github.com/matrixorigin/matrixone/issues/new/choose). Describe the intended feature and discuss the design and implementation with the team and community. Once the team agrees on the plan, you can follow the [Contribution workflow](https://github.com/matrixorigin/matrixone/blob/main/CONTRIBUTING.md#contribution-workflow) to implement it.
* To implement a feature or bug-fix for an existing outstanding issue, follow the [Contribution workflow](https://github.com/matrixorigin/matrixone/blob/main/CONTRIBUTING.md#contribution-workflow). If you need more context on a particular issue, comment on the issue to let people know.
## How to contribute code
### Contribution workflow
1. First, read the [README.md](https://github.com/matrixorigin/matrixone/blob/main/README.md) entirely for environment setup and build instructions.
2. Fork the repository on GitHub.
3. Clone your fork to your local machine with `git clone git@github.com:<yourname>/matrixone.git`.
4. Create a branch with `git checkout -b topic-branch`, the branch name is up to you.
5. Commit changes to your own branch locally, add necessary unit tests.
6. Go back to GitHub, and submit a pull request so that we can review your changes. Add some labels if needed, and don't forget to [refer to the related issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue).
7. Once your PR get approved, it would be merged sooner. Congratulations.
   
Remember to [sync your forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#keep-your-fork-synced) **before** submitting proposed changes upstream. If you have an existing local repository, please update it before you start, to minimize the chance of merge conflicts.
```shell
git remote add upstream git@github.com:matrixorigin/matrixone.git
git checkout main
git pull upstream main
git checkout -b topic-branch
```
If you still have some trouble, please refer to [GitHub Docs](https://docs.github.com/en) for help.
### Code review
When you open a pull request, you can assign some reviewers, or just leave it blank. And you can add some related labels so that it would be easier to recognize the PR's type/priority/etc. During reviewing, reviewers would comment on your code snippet, you could modify the code on your topic branch locally, commit the changes, and push to GitHub, the new commits would be attached to the PR automatically.

### Code style
The coding style suggested by the Golang community is used in MatrixOne. See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

Please follow this style to make MatrixOne easy to review, maintain and develop.
