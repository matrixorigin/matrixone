# **Code Comment Style**

This document describes the code comment style applied to MatrixOne style. When you are ready to commit, please follow the style to write good code comments.

## **Why does a good comment matter?**

- To speed up the reviewing process
- To help maintain the code
- To improve the API document readability
- To improve the development efficiency of the whole team

## **Where/When to comment?**

Write a comment where/when:

- For important code
- For obscure code
- For tricky or interesting code
- For a complex code block
- If a bug exists in the code but you cannot fix it or you just want to ignore it for the moment
- If the code is not optimal but you don't have a smarter way now
- To remind yourself or others of missing functionality or upcoming requirements not present in the code

A comment is generally required for:

- Package (Go)
- File
- Type
- Constant
- Function
- Method
- Variable
- Typical algorithm
- Exported name
- Test case
- TODO
- FIXME

## **How to comment?**

### Format of a good comment

- Go

    - Use `//` for a single-line comment and trailing comment
    - Use `/* ... */` for a block comment (used only when needed)
    - Use **gofmt** to format your code

- Place the single-line and block comment above the code it's annotating
- Fold long lines of comments
- Each line of text in your code and comment should be at most 100 characters long
- For a comment containing a URL

    - Use a **relative URL** if the text is linked to a file within the same GitHub repository
    - Use an **absolute URL** in docs and docs-cn repositories if the code with this comment is copied from another repository

### Language for a good comment

- Word

    - Use **American English** rather than British English

        - color, canceling, synchronize     (Recommended)
        - colour, cancelling, synchronise   (Not recommended)

    - Use correct spelling

    - Use **standard or official capitalization**

        - MatrixOne, Raft, SQL  (Right)
        - matrixone, RAFT, sql  (Wrong)

    - Use words and expressions consistently

        - "dead link" vs. "broken link" (Only one of them can appear in a single document)

    - Do not use lengthy compound words

    - Do not abbreviate unless it is absolutely necessary

    - *We* should be used only when it means the code writer *and* the reader

- Sentence

    - Use standard grammar and correct punctuation
    - Use relatively short sentences

- Capitalize the first letter of sentences and end them with periods

    - If a lower-case identifier comes at the beginning of a sentence, don't capitalize it

        ```
        // enterGame causes Players to enter the
        // video game, which is about a romantic
        // story in ancient China.
        func enterGame() os.Error {
            ...
        }
        ```

- When used for description, comments should be **descriptive** rather than **imperative**

    - Opens the file   (Right)
    - Open the file    (Wrong)       

- Use "this" instead of "the" to refer to the current thing

    - Gets the toolkit for this component   (Recommended)
    - Gets the toolkit for the component    (Not recommended)

- The Markdown format is allowed

    - Opens the `log` file  

### Tips for a good comment

- Comment code while writing it
- Do not assume the code is self-evident
- Avoid unnecessary comments for simple code
- Write comments as if they were for you
- Make sure the comment is up-to-date
- Let the code speak for itself

Thanks for your contribution!
