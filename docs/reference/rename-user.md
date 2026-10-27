# RENAME USER: initial support contract

`RENAME USER 'old' TO 'new' [, 'old2' TO 'new2']` requires CREATE USER or
account ALL privilege. It retains stable user IDs, credentials, grants, default
roles and owned objects. Pairs execute in order atomically; any failed pair
aborts the statement. Do not automatically retry an unknown commit outcome.

Usernames remain unique independently of hosts. Omitted/`%` hosts retain the
stored host; explicit hosts must match. Protected built-ins, registered special
identities and administrators cannot be renamed. Source or destination textual
function/procedure DEFINER references reject rename; no definitions are rewritten.

Routine create/replace and routine-bearing CLONE use canonical creator names
under the same pessimistic-RC user-row exclusion. A CLONE containing functions or
procedures requires its actual transaction to be pessimistic RC; otherwise it
fails before target database creation. No-routine CLONE behavior is unchanged.
The statement does not change an existing explicit transaction's isolation mode.

## Operational prerequisites

Stop traffic and drain SQL/task writers, fully replace every participating CN
binary and restart before admitting RENAME. Mixed versions and online downgrade
are unsupported. Complete identical special-task bootstrap on all CNs before
traffic; do not dynamically change reserved bootstrap identities while serving.

Account/catalog restore must be exclusive with affected-account writers. Verify
restored user/grant identities before resuming. Partial historical restore keeps
existing conservative identity checks; no alias history is introduced.

Existing sessions retain historical login display, but authority and login
bookkeeping use IDs. SET DEFAULT ROLE may require reconnecting. Reverse rename
is a new operation, not guaranteed rollback if the former name has been reused.

Mock/unit results are not engine concurrency, multi-CN, restart or restore
acceptance. Consult the versioned design and final validation record before rollout.
