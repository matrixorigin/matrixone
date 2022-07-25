# **Reviews**

For MatrixOne, any type of review for a pull request is crucial, where you can classify the pull request to help look for more suitable experts to solve related problems or propose your suggestions to code for not only content but style.
It's not necessary to doubt whether your review is constructive and useful enough because no matter how tiny, a suggestion may profoundly influence MatrixOne. Certainly, before reviewing we ask that you browse this page to learn basic requirements and relative methods.

## **Principles of the code review**

When you review a pull request, there are several rules and suggestions you should take to propose better comments.  
Regardless of the specifics, we suggest you stay friendly first:  

* **Be respectful**  
Be respectful to every pull request author and other reviewers. Code review is a part of community activities so you should follow the community requirements.
* **Be careful with tone**    
Try to frame your comments as suggestions or questions instead of commands. If the tone is softer, your reviews will be easier to accept.   
* **Be generous with your compliments**  
We recommend you offer encouragement and appreciation to the authors for their good practices in the code segments. In many cases, telling the authors what they did is right is even more valuable than telling them what they did is wrong.

Additionally, here are some suggestions in terms of content:  

* **Provide more**  
We encourage you to provide additional details and context of your review process as much as you can. Undoubtedly, the more detailed your review, the more useful it will be to others. If you test the pull request, please report the result and your test environment details. If you request some changes, try to suggest how.
* **Keep objective**  
Avoid individual biased opinions and subjective emotions. Of course, everyone will comment with more or less subjective opinions. However, as a good reviewer, you should consider the technique and data facts rather than your own personal preferences.
* **Case by case**  
It's difficult to decide whether it's more reasonable to accept or reject when you are faced with a complex problem. Regrettably, we can't provide a certain answer because it always depends on the specific situation, which asks you to balance the pros and cons.  

## **Classifying pull requests**

Some pull request authors may not be familiar with MatrixOne, the MatriOne development workflow, or the MatrixOne community. They don't know what labels should be added to the pull requests or which expert could be asked for a review. If you are able to, it would be great for you to triage the pull requests, add suitable labels to the pull requests, and request corresponding experts to review the pull requests. These actions could help more contributors notice the pull requests and make quick responses.  

## **Checking pull requests**

There are some basic aspects to check when you review a pull request:

* **Concentration**  
  One pull request should only do one thing. No matter how small it is, the change does exactly one thing and gets it right. Don't mix other changes into it.
* **Tests**  
  A pull request should be test covered, whether the tests are unit tests, integration tests, or end-to-end tests. Tests should be sufficient, correct, and don't slow down the CI pipeline largely.
* **Functionality**  
  The pull request should implement what the author intends to do, fit well in the existing code base, and resolve a real problem for users. Thus you should check whether or not the pull request achieves the intention and you could follow the discussions in the corresponding [GitHub issue](https://github.com/matrixorigin/matrixone/issues/new/choose).  
* **Style**  
  Code in the pull request should follow a common programming [style](contribute-code.md#get-familiar-with-style). However, sometimes the existing code is inconsistent with the style guide. You should maintain consistency with the existing code or file a new issue to fix the existing code style first.
* **Documentation**  
  If a pull request changes how users build, test, interact with, or release code, you must check whether it also updates the related documentation such as `README.md` and any generated reference docs. Similarly, if a pull request deletes or deprecates code, you must check whether or not the corresponding documentation should also be deleted.
* **Performance**  
  If you find the pull request may affect performance, you could ask the author to provide a benchmark result.
