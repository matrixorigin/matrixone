# GitHub PR and issue delivery

Use this reference when reviewing a PR description, creating or updating a
PR/issue, or investigating a description-dependent check. Metadata-only delivery
does not require rerunning product tests. In external review, inspect the stored
body and report actionable mismatches; do not edit it without authorization.

## Use the repository's current template

- Read the current template from the target repository's verified default
  branch; a long-lived working branch can contain an obsolete template.
- For PRs, start from `.github/PULL_REQUEST_TEMPLATE.md` (or the repository's
  selected PR template). Preserve its heading text, punctuation, level, and
  order; fill its sections and applicable type checkboxes. Keep validation and
  limitations in the description section unless the template provides another
  place. Replace placeholders with real content. MatrixOne PRs must link a real,
  relevant issue in the issue section; do not substitute `N/A` or a PR number.
  Search existing issues, including closed ones, before preparing a new issue.
  Use `Related to #...` for follow-up work on a closed issue, and closing keywords
  only for open issues this change actually closes. If no relevant issue exists,
  prepare one with the appropriate template and publish within the authorized
  scope; do not invent an issue number or attach unrelated work.
- For issues, select the appropriate `.github/ISSUE_TEMPLATE/*.yml` form or
  Markdown template. CLI/API creation does not enforce GitHub issue-form
  validation: render form field labels as Markdown headings and populate the
  required fields/checklists. Honor the selected template's title convention
  and metadata defaults within the authorized creation scope. Do not claim a
  duplicate search or reproduction was performed unless it was. Mark missing
  facts explicitly; never invent a commit, environment, or result.
- When updating an existing body, preserve useful author content and inspect
  the body immediately before the write to avoid overwriting concurrent edits.
  Template alignment is not a reason to relabel or reassign an existing issue.

## Normalize and verify the actual body

Before publication, serialize UTF-8 with LF line endings, removing a leading
UTF-8 BOM and converting CRLF or lone CR to LF. Keep real newlines, not literal
`\n` text used as paragraph separators. Preserve intentional escape sequences
inside code examples. Normalize the final file or structured argument being sent,
not just a displayed preview. For an already prepared body file:

```python
from pathlib import Path
body_path = Path("/tmp/mo-pr-body.md")  # use this task's actual body file
body = body_path.read_bytes().decode("utf-8-sig")
body = body.replace("\r\n", "\n").replace("\r", "\n")
body_path.write_bytes(body.encode("utf-8"))
```

Check required sections and their substantive content against the template.
If Mergify extracts a section, also inspect the current `.github/mergify.yml`
and compare its `get_section(...)` argument with the actual heading. Do not
paraphrase, decorate, indent, or put that heading inside a code block.
MatrixOne currently extracts `## What this PR does / why we need it:`; read
the current configuration rather than treating this spelling as permanent.

Template/content and encoding checks are the pre-publication gate. Remote
readback is a post-publication check, so it cannot block creation of a new PR.
Publish within the authorized scope with a structured tool argument or
`gh ... --body-file <file>`.
Read the stored body back through GitHub, check LF and required headings, and
compare it with the intended text. For a failed description check, inspect the
new check result when available; a successful body update alone does not prove
the automation recovered. Report pending checks explicitly.

## Diagnose without overclaiming

`Invalid commit message` / `section not found` identifies template rendering
failure. CRLF is a candidate cause when the expected heading is present; prove
it with the parser or a new successful check after changing only line endings,
with unchanged configuration, before calling it the root cause. Distinguish
this from workflow-permission errors and product CI failures. If one targeted
correction still fails, inspect the template/parser evidence and report the
remaining blocker rather than repeatedly editing metadata or rerunning unrelated
CI.
