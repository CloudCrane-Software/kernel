# Mandate file format (kernel docs)

A Mandate is the root of authority: a human-signed, hash-anchored grant of
capability + budget. Everything downstream (grants, intents, reservations)
derives from it and can never exceed it.

## File layout (CNB mandates repo)

```
mandates/0001-bootstrap.md          # source of truth (human-authored)
mandates/0001-bootstrap.signed.json # JCS payload + signature (machine-checked)
```

## Fields

| Field | Type | Rule |
|---|---|---|
| mandate_id | string | ULID, stable |
| human_signer | string | `human:<identifier>` — must be a real human principal |
| scope | object | `{actions: [...], resources: [...], limits: {...}}` — the ROOT scope every grant must shrink |
| cap_amount | int | minor units, > 0; the engine-level budget ceiling (TigerBeetle account credits) |
| ledger_id | string | engine cluster/account namespace |
| valid_from / expires_at | RFC3339 | grants derived from this mandate may never expire later |
| payload_jcs | string | canonical JSON (RFC 8785-flavored: sorted keys, no whitespace) of all above fields |
| mandate_sha256 | hex | sha256(payload_jcs) — registered UNIQUE in the mandates table |
| signature | string | OpenBao transit `human-signer` (ed25519) signature over the base64 of payload_jcs |

## Signing procedure (human)

```bash
# on srv-1, with the mandate file's payload_jcs already canonicalized:
base64 -w0 mandates/0001-bootstrap.jcs > /tmp/in.b64
BAO_TOKEN=<root-or-signed-token> bao write transit/sign/human-signer \
    input="$(cat /tmp/in.b64)"   # output: vault:v1:<sig>
# paste the signature value into the .signed.json and the .md
```

## Registration (admin, internal only)

```bash
curl -X POST https://api.<DOMAIN>/v1/admin/mandates -d @mandates/0001-bootstrap.signed.json
```

The kernel verifies: digest match (mandate_sha256 == sha256(payload_jcs)),
signature via `bao transit verify/human-signer`, expiry window — and only
then sets `signature_verified = true`, which the OPA policy consumes
(`mandate.signature_unverified` → DENY). The policy never re-derives the
verdict: it consumes the Mandate Verifier's conclusion.

## Invariants

- One human, one signature: no self-issuance anywhere down the chain
  (grant.issuer == grant.subject → DENY).
- Children shrink: actions/resources subsets, limits per-key ≤, expiry ≤,
  remaining_depth strictly decreasing (enforced by OPA + DB triggers).
- The cap lives in the ENGINE (debits_must_not_exceed_credits); application
  checks are advisory only.
