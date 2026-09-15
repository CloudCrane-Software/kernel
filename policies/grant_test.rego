# Unit tests for kernel.grant (run: `opa test policies/`).
# Coverage per WO-02: child-scope expansion (action/resource/limit/expiry) →
# DENY; self-issuance → DENY; parent revoked → subtree DENY; evidence missing
# → NEED_EVIDENCE; registry unavailable → DEFER; fully valid → ALLOW.

package kernel.grant_test

import data.kernel.grant

valid_input := {
	"decision_id": "dec-0001",
	"now": "2026-09-16T00:00:00Z",
	"mandate": {
		"signer_kind": "human",
		"signature_verified": true,
		"digest": "abc123",
		"registered_digest": "abc123",
		"status": "ACTIVE",
		"expires_at": "2099-01-01T00:00:00Z",
	},
	"grant": {
		"status": "ACTIVE",
		"expiry": "2098-01-01T00:00:00Z",
		"used_calls": 0,
		"call_limit": 100,
		"used_budget": 0,
		"budget_limit": 1000,
		"issuer": "mandate:0001",
		"subject": "episode:0009",
		"remaining_depth": 2,
		"scope": {
			"actions": ["compute.run"],
			"resources": ["repo:*"],
			"limits": {"max_cost": 100},
		},
	},
	"grant_chain": [
		{
			"status": "ACTIVE",
			"expiry": "2098-06-01T00:00:00Z",
			"remaining_depth": 3,
			"scope": {
				"actions": ["compute.run", "compute.cancel"],
				"resources": ["repo:*", "queue:*"],
				"limits": {"max_cost": 500},
			},
		},
	],
	"registry": {"fresh": true, "reachable": true, "revision": 42},
	"evidence": {"required": [], "provided": []},
}

# ------------------------------------------------------------------ ALLOW
test_all_valid_allows if {
	result := grant.decision with input as valid_input
	result.outcome == "ALLOW"
	result.registry_revision == 42
}

# ------------------------------------------------------- DENY: scope growth
test_child_action_expansion_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/scope/actions",
		"value": ["compute.run", "ssh.exec"],
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"scope.action_expanded" in result.reasons
}

test_child_resource_expansion_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/scope/resources",
		"value": ["repo:*", "secret:*"],
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"scope.resource_expanded" in result.reasons
}

test_child_limit_increase_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/scope/limits/max_cost",
		"value": 501,
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"scope.limit_increased" in result.reasons
}

test_child_expiry_extension_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/expiry",
		"value": "2099-06-01T00:00:00Z",
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"scope.expiry_extended" in result.reasons
}

test_child_depth_not_decreasing_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/remaining_depth",
		"value": 3,
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"scope.depth_not_decreasing" in result.reasons
}

# ---------------------------------------------------- DENY: self-issuance
test_self_issuance_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/issuer",
		"value": "episode:0009",
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"grant.self_issued" in result.reasons
}

# ------------------------------------------------- DENY: parent revoked
test_parent_revoked_denies_subtree if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant_chain/0/status",
		"value": "REVOKED",
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"grant.parent_revoked" in result.reasons
}

# --------------------------------------------- DENY: mandate/budget basics
test_signature_unverified_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/mandate/signature_verified",
		"value": false,
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"mandate.signature_unverified" in result.reasons
}

test_digest_mismatch_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/mandate/digest",
		"value": "tampered",
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"mandate.digest_mismatch" in result.reasons
}

test_budget_exhausted_denied if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/grant/used_budget",
		"value": 1000,
	}])
	result := grant.decision with input as inp
	result.outcome == "DENY"
	"grant.budget_limit_exceeded" in result.reasons
}

# ------------------------------------------------ NEED_EVIDENCE / DEFER
test_missing_evidence_needs_evidence if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/evidence/required",
		"value": ["receipt.external"],
	}])
	result := grant.decision with input as inp
	result.outcome == "NEED_EVIDENCE"
	count([r | some r in result.reasons; startswith(r, "evidence.missing.")]) > 0
}

test_registry_not_fresh_defers if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/registry/fresh",
		"value": false,
	}])
	result := grant.decision with input as inp
	result.outcome == "DEFER"
	"registry.not_fresh" in result.reasons
}

test_registry_unreachable_defers if {
	inp := json.patch(valid_input, [{
		"op": "add",
		"path": "/registry/reachable",
		"value": false,
	}])
	result := grant.decision with input as inp
	result.outcome == "DEFER"
	"registry.unavailable" in result.reasons
}

# DENY wins over DEFER when both apply (fail closed first)
test_deny_beats_defer if {
	inp := json.patch(valid_input, [
		{"op": "add", "path": "/registry/fresh", "value": false},
		{"op": "add", "path": "/grant/status", "value": "REVOKED"},
	])
	result := grant.decision with input as inp
	result.outcome == "DENY"
}
