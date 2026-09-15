# kernel.grant — authorization policy for action intents (WO-02).
#
# Four-valued decision, fail closed:
#   DENY           a hard invariant is violated (mandate/grant/scope)
#   DEFER          the policy registry is not fresh (cannot decide now)
#   NEED_EVIDENCE  evidence requirements are not satisfied
#   ALLOW          everything passed
# Priority: DENY > DEFER > NEED_EVIDENCE > ALLOW.
#
# Signature verification happens OUTSIDE this policy (Mandate Verifier);
# the policy only consumes its boolean conclusions — it never trusts agent
# memory or request payloads for authority.
#
# No hardcoded subjects/resources/actions appear here by design.

package kernel.grant

now_ns := time.parse_rfc3339_ns(input.now)

chain_head := input.grant_chain[0]

# ---------------------------------------------------------------- deny rules
deny_reasons contains "mandate.signer_not_human" if {
	input.mandate.signer_kind != "human"
}

deny_reasons contains "mandate.signature_unverified" if {
	not input.mandate.signature_verified
}

deny_reasons contains "mandate.digest_mismatch" if {
	input.mandate.digest != input.mandate.registered_digest
}

deny_reasons contains "mandate.not_active" if {
	input.mandate.status != "ACTIVE"
}

deny_reasons contains "mandate.expired" if {
	now_ns >= time.parse_rfc3339_ns(input.mandate.expires_at)
}

deny_reasons contains "grant.not_active" if {
	input.grant.status != "ACTIVE"
}

deny_reasons contains "grant.expired" if {
	now_ns >= time.parse_rfc3339_ns(input.grant.expiry)
}

deny_reasons contains "grant.call_limit_exceeded" if {
	input.grant.used_calls >= input.grant.call_limit
}

deny_reasons contains "grant.budget_limit_exceeded" if {
	input.grant.used_budget >= input.grant.budget_limit
}

# self-issuance is forbidden: an issuer cannot grant to itself
deny_reasons contains "grant.self_issued" if {
	input.grant.issuer == input.grant.subject
}

# any revoked/expired ancestor revokes the whole subtree
deny_reasons contains "grant.parent_revoked" if {
	some p in input.grant_chain
	p.status == "REVOKED"
}

deny_reasons contains "grant.parent_inactive" if {
	some p in input.grant_chain
	p.status != "ACTIVE"
}

deny_reasons contains "grant.parent_expired" if {
	some p in input.grant_chain
	now_ns >= time.parse_rfc3339_ns(p.expiry)
}

# ------------------------------------------------- scope non-increasing rules
# child scope must be a subset of parent scope (WO-02 item 4)

deny_reasons contains "scope.action_expanded" if {
	count(input.grant_chain) > 0
	some a in input.grant.scope.actions
	not a in chain_head.scope.actions
}

deny_reasons contains "scope.resource_expanded" if {
	count(input.grant_chain) > 0
	some r in input.grant.scope.resources
	not r in chain_head.scope.resources
}

deny_reasons contains "scope.limit_increased" if {
	count(input.grant_chain) > 0
	some k, v in input.grant.scope.limits
	v > object.get(chain_head.scope.limits, k, 0)
}

deny_reasons contains "scope.expiry_extended" if {
	count(input.grant_chain) > 0
	time.parse_rfc3339_ns(input.grant.expiry) > time.parse_rfc3339_ns(chain_head.expiry)
}

deny_reasons contains "scope.depth_not_decreasing" if {
	count(input.grant_chain) > 0
	input.grant.remaining_depth >= chain_head.remaining_depth
}

# ------------------------------------------------------------- defer rules
defer_reasons contains "registry.not_fresh" if {
	not input.registry.fresh
}

defer_reasons contains "registry.unavailable" if {
	not input.registry.reachable
}

# -------------------------------------------------------- evidence rules
need_evidence_reasons contains sprintf("evidence.missing.%s", [k]) if {
	some k in input.evidence.required
	not k in input.evidence.provided
}

# ---------------------------------------------------------------- decisions
decision := {
	"outcome": "DENY",
	"reasons": sort([r | some r in deny_reasons]),
	"decision_id": input.decision_id,
	"registry_revision": object.get(input.registry, "revision", 0),
} if {
	count(deny_reasons) > 0
}

decision := {
	"outcome": "DEFER",
	"reasons": sort([r | some r in defer_reasons]),
	"decision_id": input.decision_id,
	"registry_revision": object.get(input.registry, "revision", 0),
} if {
	count(deny_reasons) == 0
	count(defer_reasons) > 0
}

decision := {
	"outcome": "NEED_EVIDENCE",
	"reasons": sort([r | some r in need_evidence_reasons]),
	"decision_id": input.decision_id,
	"registry_revision": object.get(input.registry, "revision", 0),
} if {
	count(deny_reasons) == 0
	count(defer_reasons) == 0
	count(need_evidence_reasons) > 0
}

decision := {
	"outcome": "ALLOW",
	"reasons": [],
	"decision_id": input.decision_id,
	"registry_revision": object.get(input.registry, "revision", 0),
} if {
	count(deny_reasons) == 0
	count(defer_reasons) == 0
	count(need_evidence_reasons) == 0
}
