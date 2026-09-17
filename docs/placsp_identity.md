# OpenPLACSP notice/tombstone identity

This note resolves the notice/tombstone identity question left open by the
Gold/Spark foundation. It freezes only the identity bridge required by
current-state; it does not implement current-state selection or change
canonical Silver.

## Authoritative rule

OpenPLACSP uses the Atom Tombstones namespace
`http://purl.org/atompub/tombstones/1.0`. RFC 6721 section 3 defines
`at:deleted-entry/@ref` as the value of the `atom:id` of the entry that was
removed. The same section defines `when` as the removal instant and explicitly
describes the case where an Atom feed contains an entry and a deleted-entry for
the same identifier: processors compare `deleted-entry/@when` with
`entry/atom:updated` to decide which observation is newer.

Reference: <https://www.rfc-editor.org/rfc/rfc6721#section-3>

The repository fixture `tests/fixtures/placsp/mini-placsp.atom` uses that exact
vocabulary and the same published URI family for both values:

```text
atom:id / deleted-entry@ref
https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/<id>
```

No title, buyer, CPV, contract-folder text, retrieval timestamp or fuzzy
normalization is needed to establish identity.

## Canonical Silver consequence

Canonical Silver already preserves the two published identifiers without URL
normalization:

```text
PLACSP notice:    procedure_id = placsp:procedure:<atom:id>
PLACSP tombstone: procedure_id = placsp:procedure:<deleted-entry@ref>
```

Therefore exact `procedure_id` equality is the procedure-identity bridge for
PLACSP. Current-state Spark may partition/group notices and tombstones directly
by the canonical `procedure_id`; it must not rewrite that key.

Dated tombstones have a source-time-specific `event_id`, but their
`procedure_id` remains the published `ref`, so multiple delete instants for the
same Atom entry remain historical events of one source procedure.

## Resolution policy

`tfm_licitaciones.placsp_identity.resolve_placsp_identity` is a small
engine-neutral reference checker for this contract. It returns:

- `resolved` when the canonical event identity and procedure identity agree on
  one exact published Atom URI;
- `unresolved` when `procedure_id` is missing/malformed, a future PLACSP event
  type is not covered, or `event_id` and `procedure_id` disagree.

Unresolved cases never receive a synthetic key. They must be surfaced by the
future `current_state_issues` output rather than joined heuristically.

## What this does not decide

This identity result does **not** make every procedure temporally resolvable.
The Gold/Spark current-state implementation still has to freeze and implement
policies for genuinely undated competing events, incomplete observed histories
and the `current_state_issues` contract. Identity and temporal ordering are kept
separate deliberately.
