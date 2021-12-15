# About RFCs
Contributors will use Github pull request workflow to develop and review the 'minor' changes, such as bug fixes and document updates. For 'major' changes like new features or improvements to MatrixOne, RFC process can be used to provide clear and high-level description of the proposal.

# RFC Process

1. Fork the repository.
2. Copy the template file: cp 00000000-template.md text/00000000-feature_name.md
3. Fill the detail information into the RFC file and the status in RFC should be 'drafted'.
4. Commit the change with message prefix: "[RFC] feature name"
5. Submit the PR to the main branch.
6. Once the PR is reviewed, the filename should be change to "YYYYMMDD_feature_name.md" and the status in RFC should be 'in progress'.
7. Once the feature is implemented, the RFC status need to be change to 'completed'. If other implementation make an RFC obsolete, it need to be changed to 'obsolete' status and refer to new RFC or PR.