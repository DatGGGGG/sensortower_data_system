from __future__ import annotations

import argparse

import psycopg
from psycopg.rows import dict_row

from .server import DATABASE_URL, generate_mcp_token, hash_token, token_prefix


def _fetch_one(query: str, params: dict[str, object] | None = None) -> dict[str, object] | None:
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or {})
            return cur.fetchone()


def _fetch_all(query: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or {})
            return list(cur.fetchall())


def create_token(name: str) -> int:
    token = generate_mcp_token()
    row = _fetch_one(
        """
        INSERT INTO api_access.mcp_tokens (name, token_prefix, token_hash)
        VALUES (%(name)s, %(token_prefix)s, %(token_hash)s)
        RETURNING id, name, token_prefix, created_at
        """,
        {
            "name": name,
            "token_prefix": token_prefix(token),
            "token_hash": hash_token(token),
        },
    )
    if not row:
        print("Failed to create MCP token.")
        return 1

    print("Created MCP token")
    print(f"id: {row['id']}")
    print(f"name: {row['name']}")
    print(f"prefix: {row['token_prefix']}")
    print(f"created_at: {row['created_at']}")
    print("")
    print("Plaintext token (shown once):")
    print(token)
    return 0


def list_tokens() -> int:
    rows = _fetch_all(
        """
        SELECT
            id,
            name,
            token_prefix,
            is_active,
            created_at,
            last_used_at,
            revoked_at
        FROM api_access.mcp_tokens
        ORDER BY created_at DESC, id DESC
        """
    )
    if not rows:
        print("No MCP tokens found.")
        return 0

    for row in rows:
        print(
            f"{row['id']:>4} | {row['name']:<24} | {row['token_prefix']:<16} | "
            f"active={row['is_active']} | created={row['created_at']} | "
            f"last_used={row['last_used_at']} | revoked={row['revoked_at']}"
        )
    return 0


def revoke_token(token_id: int) -> int:
    row = _fetch_one(
        """
        UPDATE api_access.mcp_tokens
        SET is_active = FALSE,
            revoked_at = now()
        WHERE id = %(id)s
          AND is_active = TRUE
        RETURNING id, name, token_prefix, revoked_at
        """,
        {"id": token_id},
    )
    if not row:
        print(f"No active MCP token found for id={token_id}.")
        return 1

    print(
        f"Revoked MCP token id={row['id']} name={row['name']} "
        f"prefix={row['token_prefix']} at {row['revoked_at']}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage remote MCP tokens for the VN Strategy Data Warehouse.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create a new MCP token")
    create_parser.add_argument("--name", required=True, help="Friendly name for the token owner")

    subparsers.add_parser("list", help="List MCP tokens")

    revoke_parser = subparsers.add_parser("revoke", help="Revoke an existing MCP token")
    revoke_parser.add_argument("--id", type=int, required=True, help="Database id of the token to revoke")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "create":
        return create_token(args.name)
    if args.command == "list":
        return list_tokens()
    if args.command == "revoke":
        return revoke_token(args.id)
    print(f"Unknown command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
