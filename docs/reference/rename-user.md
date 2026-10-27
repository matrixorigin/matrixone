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

Public SQL, selected deterministic engine interleavings, separate-process two-CN
bootstrap, snapshot restore, restart and exact pre-feature-base offline replacement
have been exercised. See the [current design validation boundary](../design/20261026-rename-user.md#current-validation-and-supported-rollout-boundary).
Historical released-version upgrades, rolling/mixed versions, downgrade and
multi-host deployment are not covered by that result.

## Destination names and login compatibility

After existing trimming/empty-name/colon/hash validation, new destinations reject
ASCII space/tab, backtick, single/double quotes, backslash, question mark, NUL,
and the exact uppercase substring `%3A`. These are existing login/SQL/protocol
consumer limitations, not a new general ASCII-only username grammar. Unicode,
underscore, dash, dot, other percent uses and lowercase `%3a` remain accepted.
All pairs are checked before the transaction starts; an invalid later target
cannot rename an earlier source. Source names do not gain these new restrictions,
so addressable legacy catalog labels can migrate to a safe name. Existing shared
credential/management guards and the login protocol are not loosened.
