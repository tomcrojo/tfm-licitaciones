# PLACSP identity evidence snapshot

This small evidence note is intentionally separate from the implementation
contract so the current-state PR can distinguish source evidence from policy.

The repository fixture `tests/fixtures/placsp/mini-placsp.atom` contains
OpenPLACSP Atom entry identifiers such as:

```text
https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101
https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000102
```

and a tombstone `ref` in the same published identifier namespace:

```text
https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/99900001
```

The fixture alone establishes the concrete PLACSP identifier shape but not the
semantic meaning of `ref`. That meaning comes from RFC 6721 section 3, which
requires a deleted-entry `ref` to equal the removed entry's `atom:id`.

The implementation therefore uses exact URI equality only. It does not infer
identity from the numeric suffix or normalize host/path/query components.
