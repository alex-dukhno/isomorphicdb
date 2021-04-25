# Why it is here?

The longer I worked on this project the more ideas I have and less time I have to explore and implement them.
Also, there is a lack of documentation personally for me or for possible contributors.
Thus, I decided start with some sort of process to document proposals and maybe get a feedback from other developers.

I was inspired by [Rust RFCs](https://github.com/rust-lang/rfcs) and 
[Cockroach](https://github.com/cockroachdb/cockroach/tree/master/docs/RFCS) projects how this process can provide
high level design overview of product features.

## What to do if you think you need to write an RFC

If you have an idea - this is great! :) You can start with a [discussion](https://github.com/alex-dukhno/isomorphicdb/discussions)
in **Idea** category or create an [issue](https://github.com/alex-dukhno/isomorphicdb/issues/new/choose) or sneak into the
[discord server](https://discord.gg/PUcTcfU) and discuss it there. Depending on the complexity you would probably need 
to write an RFC. If you stuck in the process you can always submit a draft PR and ask for initial feedback.

## The Process

This is here so that I won't forget what to do when someone (including myself) wants to write another RFC... :)

1. Copy `YYYY-MM-DD_template.md` to `text/YYYY-MM-DD_my-feature.md` where "my-feature" is descriptive.
1. Submit a pull request. Please, don't combine other files with the RFC. If you have a prototype branch please link it
   with the PR. You can submit the PR before RFC is complete, however, make sure that it is either in 
   [Draft state](https://github.blog/2019-02-14-introducing-draft-pull-requests/) or it has [WIP] in the beginning of
   the title. If you want to receive initial feedback you can ping project maintainer.
1. Whenever you feel that RFC is ready for review move PR to 
   [Ready for Review state](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/changing-the-stage-of-a-pull-request)
   or removing [WIP] from the PR title.
1. When you get approval to merge
    - create or ask maintainers to create tracking issue and update the `RFC Tracking Issue` field
    - rename the RFC document to prefix it with the current date (`YYYY-MM-DD_`) if it was not done yet
    - update the `RFC PR` field
1. If during RFC implementation process it appears that an RFC is obsolete or its need to be postponed. Issue a PR that
   adds description and links to PR(s) or another RFC(s) that make it obsolete or postponed.
