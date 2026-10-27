# v2: declared publication dates and TED ordering

2026-10-27. Applies to v2 only; the graded release remains unchanged.

## Source fields

TED Search requests `procedure-identifier`, not both it and its alias.
The official [search field list](https://docs.ted.europa.eu/ODS/latest/reuse/field-list.html)
maps this field to `BT-04-notice`. The Publications Office also explicitly
[documents the search alias](https://op.europa.eu/en/web/ted-reusers-workshops/questions_and_answers_2024_04_17).
A live Search API v3 probe with `fields=[ND, PD, procedure-identifier]`
succeeded on 2026-10-27. Procedure identifiers are absent on some planning
notices by design; do not fabricate them from notice numbers or buyer names.

PLACSP publication date is the earliest valid `cbc:IssueDate` under
`cpe:ValidNoticeInfo[cpex:NoticeTypeCode=DOC_CN]` /
`cpe:AdditionalPublicationStatus/cpe:AdditionalPublicationDocumentReference`.
All publication media are considered; XML order does not determine the result.
Multiple dates may describe republication. Award/result notices and Atom
`updated` are distinct facts. Missing/invalid declared dates remain null.
No first-observed fallback is implemented or passed off as an official date.
The first complete corpus run covers 14,762 of 14,763 Gold opportunities
(99.9932%); only two of 83,111 PUB snapshots lack a date. Whole-history Silver
coverage is lower (298,913 / 348,230 notice snapshots), concentrated in closed
and planning states. No fallback is warranted for the opportunity product.
Using the September/October retrieval date would also misrepresent historical
publication. The complete two-run evidence records coverage by grain and status.

## Current-state ordering

For PLACSP, preserve the source revision timestamp rule and existing tombstone
semantics. Publication date does not order its revisions.

For TED, use declared publication date at day precision as the ordering key,
without setting `source_updated_at` to an invented midnight revision timestamp.
A unique latest publication day wins when every notice in the procedure has a
date. A lone notice is still resolvable without one. Missing dates in competing
groups, or two notices sharing the latest day, leave every event in that group
in `current_state_issues` (`undated_competing_events`). Older same-day ties do
not block a unique later notice. Input order, ingestion time, notice ID and
notice type do not break TED day ties. Notice type distinguishes lifecycle
forms, but does not prove correction chronology within a day.

This deliberately leaves some newly identified procedures unresolved. A future
ordering change needs authoritative revision/version evidence; UUID or form-type
priority would produce a deterministic answer without sufficient chronology.
TED status and deadline remain unmapped, so resolving identity alone does not
make a TED notice an actionable Gold opportunity.

## Engine boundary

The v2 facade adds publication dates after either guarded engine. It checks the
new field across all Bronze observations before deduplication can hide date
conflicts. The native kernel and frozen Python reference remain v1 benchmark
oracles. Engine benchmarks therefore measure their documented historical
contract, while production v2 parity includes this shared extension. Schemas
are unchanged, but values and current-state counts change deliberately.
