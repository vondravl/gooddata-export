"""Tests for users / user-group processing from the usersAndUserGroups layout."""

from gooddata_export.process.layout import (
    process_user_group_members,
    process_users,
)


def make_data(users):
    return {"users": users, "userGroups": []}


class TestProcessUserGroupMembers:
    """Tests for the process_user_group_members function."""

    def test_basic_memberships(self):
        data = make_data(
            [
                {"id": "u1", "userGroups": [{"id": "g1"}, {"id": "g2"}]},
                {"id": "u2", "userGroups": [{"id": "g1"}]},
            ]
        )
        result = process_user_group_members(data)
        assert result == [
            {"user_id": "u1", "user_group_id": "g1"},
            {"user_id": "u1", "user_group_id": "g2"},
            {"user_id": "u2", "user_group_id": "g1"},
        ]

    def test_duplicate_group_entries_deduped(self):
        """The API tolerates the same group listed twice in a user's
        userGroups; duplicates must not reach the PK-constrained table."""
        data = make_data(
            [{"id": "u1", "userGroups": [{"id": "g1"}, {"id": "g1"}, {"id": "g2"}]}]
        )
        result = process_user_group_members(data)
        assert result == [
            {"user_id": "u1", "user_group_id": "g1"},
            {"user_id": "u1", "user_group_id": "g2"},
        ]

    def test_missing_ids_skipped(self):
        data = make_data(
            [
                {"id": "", "userGroups": [{"id": "g1"}]},
                {"id": "u1", "userGroups": [{"id": ""}, {}]},
            ]
        )
        assert process_user_group_members(data) == []

    def test_no_users(self):
        assert process_user_group_members({"users": [], "userGroups": []}) == []


class TestProcessUsers:
    """Tests for the process_users function."""

    def test_duplicate_group_entries_deduped(self):
        """user_group_ids/user_group_count must match the deduped junction
        table, preserving first-seen order."""
        data = make_data(
            [{"id": "u1", "userGroups": [{"id": "g2"}, {"id": "g1"}, {"id": "g2"}]}]
        )
        (user,) = process_users(data)
        assert user["user_group_ids"] == str(["g2", "g1"])
        assert user["user_group_count"] == 2

    def test_no_groups(self):
        data = make_data([{"id": "u1", "userGroups": []}])
        (user,) = process_users(data)
        assert user["user_group_ids"] == ""
        assert user["user_group_count"] == 0
