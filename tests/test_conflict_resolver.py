"""
tests/test_conflict_resolver.py

Tests for ConflictResolver — strategy application and change detection.
No external dependencies — pure logic testing.
"""

import pytest
from sync.conflict_resolver import (
    ConflictResolver,
    ConflictStrategy,
    ConflictAction,
    compute_record_hash,
)


@pytest.fixture
def valid_record():
    """A complete transformed record for testing."""
    return {
        "_jde_an8": 1002,
        "_jde_at1": "C",
        "name": "Gaisano Grand Mall",
        "phone": "+63328889999",
        "street": "Ouano Avenue",
        "street2": None,
        "city": "Mandaue City",
        "zip": "6014",
        "state_code": "07",
        "country_code": "PHL",
        "vat": "987654321000",
        "customer_rank": 1,
        "is_company": True,
        "parent_an8": None,
        "comment": "Migrated from JDE F0101 | AN8=1002",
    }


class TestNoConflict:
    def test_no_existing_odoo_record_returns_none_action(self, valid_record):
        """Record not in Odoo yet — action must be NONE, no strategy applied."""
        resolver = ConflictResolver(ConflictStrategy.JDE_WINS)
        result = resolver.resolve(valid_record, existing_odoo_id=None)
        assert result.action == ConflictAction.NONE

    def test_no_conflict_strategy_is_not_set(self, valid_record):
        """No conflict means no strategy was applied."""
        resolver = ConflictResolver(ConflictStrategy.JDE_WINS)
        result = resolver.resolve(valid_record, existing_odoo_id=None)
        assert result.strategy is None


class TestChangeDetection:
    def test_same_hash_returns_skip(self, valid_record):
        """No change detected — must skip regardless of strategy."""
        resolver = ConflictResolver(ConflictStrategy.JDE_WINS)
        current_hash = compute_record_hash(valid_record)
        result = resolver.resolve(
            valid_record,
            existing_odoo_id=8,
            last_known_hash=current_hash,
        )
        assert result.action == ConflictAction.SKIP

    def test_different_hash_triggers_strategy(self, valid_record):
        """Changed record — strategy must be applied."""
        resolver = ConflictResolver(ConflictStrategy.JDE_WINS)
        result = resolver.resolve(
            valid_record,
            existing_odoo_id=8,
            last_known_hash="old_hash_that_does_not_match",
        )
        assert result.action == ConflictAction.UPDATE

    def test_no_previous_hash_triggers_strategy(self, valid_record):
        """First time seeing this record — no stored hash — strategy applied."""
        resolver = ConflictResolver(ConflictStrategy.JDE_WINS)
        result = resolver.resolve(
            valid_record,
            existing_odoo_id=8,
            last_known_hash=None,
        )
        assert result.action == ConflictAction.UPDATE


class TestJdeWinsStrategy:
    def test_conflict_returns_update(self, valid_record):
        """JDE_WINS — changed record must trigger UPDATE action."""
        resolver = ConflictResolver(ConflictStrategy.JDE_WINS)
        result = resolver.resolve(
            valid_record,
            existing_odoo_id=8,
            last_known_hash="old_hash",
        )
        assert result.action == ConflictAction.UPDATE
        assert result.strategy == ConflictStrategy.JDE_WINS


class TestOdooWinsStrategy:
    def test_conflict_returns_skip(self, valid_record):
        """ODOO_WINS — changed record must be skipped, Odoo data preserved."""
        resolver = ConflictResolver(ConflictStrategy.ODOO_WINS)
        result = resolver.resolve(
            valid_record,
            existing_odoo_id=8,
            last_known_hash="old_hash",
        )
        assert result.action == ConflictAction.SKIP
        assert result.strategy == ConflictStrategy.ODOO_WINS


class TestFlagStrategy:
    def test_conflict_returns_flag(self, valid_record):
        """FLAG — changed record must be flagged for human review."""
        resolver = ConflictResolver(ConflictStrategy.FLAG)
        result = resolver.resolve(
            valid_record,
            existing_odoo_id=8,
            last_known_hash="old_hash",
        )
        assert result.action == ConflictAction.FLAG
        assert result.strategy == ConflictStrategy.FLAG


class TestComputeRecordHash:
    def test_same_record_produces_same_hash(self, valid_record):
        """Hash must be deterministic — same input always produces same output."""
        hash1 = compute_record_hash(valid_record)
        hash2 = compute_record_hash(valid_record)
        assert hash1 == hash2

    def test_changed_field_produces_different_hash(self, valid_record):
        """Changing any business field must change the hash."""
        original_hash = compute_record_hash(valid_record)
        valid_record["phone"] = "09999999999"
        new_hash = compute_record_hash(valid_record)
        assert original_hash != new_hash

    def test_comment_change_does_not_change_hash(self, valid_record):
        """comment is excluded from hash — changing it must not affect hash."""
        original_hash = compute_record_hash(valid_record)
        valid_record["comment"] = "Different timestamp on every run"
        new_hash = compute_record_hash(valid_record)
        assert original_hash == new_hash

    def test_hash_is_32_character_hex_string(self, valid_record):
        """MD5 hash must be exactly 32 hex characters."""
        hash_value = compute_record_hash(valid_record)
        assert len(hash_value) == 32
        assert all(c in "0123456789abcdef" for c in hash_value)