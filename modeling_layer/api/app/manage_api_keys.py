from __future__ import annotations

import argparse

from .db import fetch_all, fetch_one
from .security import api_key_prefix, generate_api_key, hash_api_key


def create_key(name: str) -> int:
    api_key = generate_api_key()
    query = """
        INSERT INTO api_access.api_keys (name, key_prefix, key_hash)
        VALUES (%(name)s, %(key_prefix)s, %(key_hash)s)
        RETURNING id, name, key_prefix, created_at
    """
    row = fetch_one(
        query,
        {
            "name": name,
            "key_prefix": api_key_prefix(api_key),
            "key_hash": hash_api_key(api_key),
        },
    )
    if not row:
        print("Failed to create API key.")
        return 1

    print("Created API key")
    print(f"id: {row['id']}")
    print(f"name: {row['name']}")
    print(f"prefix: {row['key_prefix']}")
    print(f"created_at: {row['created_at']}")
    print("")
    print("Plaintext key (shown once):")
    print(api_key)
    return 0


def list_keys() -> int:
    query = """
        SELECT
            id,
            name,
            key_prefix,
            is_active,
            created_at,
            last_used_at,
            revoked_at
        FROM api_access.api_keys
        ORDER BY created_at DESC, id DESC
    """
    rows = fetch_all(query)
    if not rows:
        print("No API keys found.")
        return 0

    for row in rows:
        print(
            f"{row['id']:>4} | {row['name']:<24} | {row['key_prefix']:<16} | "
            f"active={row['is_active']} | created={row['created_at']} | "
            f"last_used={row['last_used_at']} | revoked={row['revoked_at']}"
        )
    return 0


def revoke_key(key_id: int) -> int:
    query = """
        UPDATE api_access.api_keys
        SET is_active = FALSE,
            revoked_at = now()
        WHERE id = %(id)s
          AND is_active = TRUE
        RETURNING id, name, key_prefix, revoked_at
    """
    row = fetch_one(query, {"id": key_id})
    if not row:
        print(f"No active API key found for id={key_id}.")
        return 1

    print(
        f"Revoked API key id={row['id']} name={row['name']} "
        f"prefix={row['key_prefix']} at {row['revoked_at']}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage API keys for the Strategy Data API.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create a new API key")
    create_parser.add_argument("--name", required=True, help="Friendly name for the key owner")

    subparsers.add_parser("list", help="List API keys")

    revoke_parser = subparsers.add_parser("revoke", help="Revoke an existing API key")
    revoke_parser.add_argument("--id", type=int, required=True, help="Database id of the key to revoke")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "create":
        return create_key(args.name)
    if args.command == "list":
        return list_keys()
    if args.command == "revoke":
        return revoke_key(args.id)
    print(f"Unknown command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
