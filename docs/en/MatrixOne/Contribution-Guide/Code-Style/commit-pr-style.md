# Commit Message and Pull Request Style

This document describes the commit message and Pull Request style applied to all MatrixOrigin repositories. When you are ready to commit, be sure to follow the style guide to write a good commit message, a good Pull Request title, and a description.

## Why a good commit message matters

- To speed up the reviewing process
    - Help the reviewers better understand the PR
    - Allow ignoring unimportant commits
- To help us write good Release Notes
- To help the future maintainers establish the context
    - Provide better information when browsing the history

## What is a good commit message

Elements of a good commit message:

1. **What is your change? (mandatory)**

    It can be fixing a specific bug, adding a feature, improving performance, reliability, and stability, or just being a change for the sake of correctness.

2. **Why was this change made? (mandatory)**

    For short and obvious patches, this part can be omitted, but it should be a clear description of what the approach was.

3. **What effect does the commit have? (optional)**

    In addition to the obvious effects, this may include benchmarks, side effects, etc. For short and obvious patches, this part can be omitted.

## How to write a good commit message

To write a good commitment message, we suggest following a good format, cultivating good habits, and using good language.

### Format of a good commit message

Please follow the following style for **all your commits**:

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

+ For the first subject line:
    - Use no more than 70 characters.
    - If the changes affect two subsystems, use a comma (and whitespace) to separate them like `util/codec, util/types:`.
    - If the changes affect three or more subsystems, you can use `*` instead, like `*:`.
    - Use a lowercase letter on the text that immediately follows the colon. For example: "media: **update** the DM architecture image"
    - Do not add a period at the end of a commit message.

- For the second line, always leave it blank.
- For the why part, if there is no specific reason for the change, you can use one of the generic reasons like "Improve performance", "Improve test coverage."
- For other lines, use no more than 80 characters.

### Habits for a good commit message

- Summarize your change
- Describe clearly one logical change and avoid lazy messages as `misc fixes`
- Describe any limitations of the current code
- Do not end the subject with a period "."
- Do not assume the code is self-evident
- Do not assume reviewers understand the original issue

### Language for a good commit message

- Use the imperative mood for the first subject line
- Use simple verb tenses (eg. use "add" not "added")
- Use correct and standard grammar
- Use words and expressions consistently
- Use relatively short sentences
- Do not use lengthy compound words
- Do not abbreviate unless it's absolutely necessary

## Pull Request description style

For the Pull Request description in the `Conversation` box, please refer to the following Pull Request description template and include the necessary information:

```
**What type of PR is this?**

- [ ] API-change
- [ ] BUG
- [ ] Improvement
- [ ] Documentation
- [ ] Feature
- [ ] Test and CI
- [ ] Code Refactoring

**Which issue(s) this PR fixes:**

issue #

**What this PR does / why we need it:**


**Special notes for your reviewer:**


**Additional documentation (e.g. design docs, usage docs, etc.):**

```

You may also use the checklist style to list contents if needed. The Markdown syntax is as follows:

```
- [x] A checked line, something already done or fulfilled
- [ ] An unchecked line, something not finished yet
```

For short and obvious Pull Requests, you can omit some of the above information.

Thanks for your contribution！
