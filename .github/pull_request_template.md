<!-- Thanks for sending the pull request! -->

<!--
### Contribution Checklist

  - PR title format should be *type(scope): subject*. For details, see *[Pull Request Title](https://github.com/cubefs/cubefs/blob/master/.github/workflows/check_pull_request.yml)*.

  - Each pull request should address only one issue, not mix up code from multiple issues.

  - Each commit in the pull request has a meaningful commit message. For details, see *[Commit Message](https://github.com/cubefs/cubefs/blob/master/.github/workflows/check_pull_request.yml)*

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.
-->

Fixes: #xyz <!-- Either this PR fixes an issue, -->

Main Issue: #xyz <!-- or this PR is one task of an issue. -->


### Motivation
--------------

<!-- Explain here the context, and why you're making that change. What is the problem you're trying to solve. -->

### Modifications
-----------------

<!-- Describe the modifications you've done. -->

### Verifying this change
-------------------------

- [ ] Make sure that the change passes the testing checks.

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *This can be verified in development debugging*
  - *This can be realized in a mocked environment, like a test cluster consisting in docker*

*(or)*

This change *MUST* reappear in online clusters, or occur in that specific scenarios.

### Does this pull request potentially affect one of the following parts:
-----------------

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

*If the box was checked, please highlight the changes*

- [ ] Dependencies (add or upgrade a dependency)
- [ ] The public API
- [ ] The schema
- [ ] The default values of configurations
- [ ] The threading model
- [ ] The binary protocol
- [ ] The REST endpoints
- [ ] The admin CLI options
- [ ] The metrics
- [ ] Anything that affects deployment

### Documentation
-----------------

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

- [ ] `doc` <!-- Your PR contains doc changes. -->
- [ ] `doc-required` <!-- Your PR changes impact docs and you will update later -->
- [ ] `doc-not-needed` <!-- Your PR changes do not impact docs -->
- [ ] `doc-complete` <!-- Docs have been already added -->

### Matching PR in forked repository
-----------------

PR in forked repository: <!-- ENTER URL HERE -->

<!-- Thanks for contributing!  -->
