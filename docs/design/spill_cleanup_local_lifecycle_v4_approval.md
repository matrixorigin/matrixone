# Revision 4 approval and implementation record

- Design commit: `b3b51d9a9db194ddca5f571933279930c988d71a`.
- Exact design blob: `b28a94c0146e07c10cdf7f872a4614d78a4cb0ed`.
- Decision date: 2026-10-25 (Asia/Shanghai).
- Decision owner: the human user directing the Ariznawlll PR repair task.
- Explicit decision: **批准 revision 4 并继续实现**.

The approval question named the exact commit/document and the material choices:
no background queue, required named-file FD rotation, creation fencing after
deletion failure, conservative disk accounting, and recovery only after old
process termination and successful startup cleanup, potentially requiring a CN
restart. The user explicitly selected approval and continued implementation.

This records that user's design decision, not an approval by fengttt, XuPeng-SH,
aunjgr or any GitHub implementation reviewer. Existing GitHub change requests
are not resolved automatically. The unchanged revision-4 document is retained
as the approved artifact despite its original proposed-status header.

Implementation validation and delivery status belong to the PR and CI evidence,
not this design decision record. Approval does not waive runtime verification
or certify whole-CN recovery, Linux execution or performance acceptance.
