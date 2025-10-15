# DSIS Python Client

Minimal SDK enabling dual-token (Azure AD + DSIS) access to DSIS data via Equinor API Management.

This page provides a high-level overview. For installation & a code snippet, see Getting Started.

## Documentation Map

- Getting Started: `guides/getting-started.md`
- API Summary (includes configuration): `api/index.md`

## Core Concepts

- Dual token flow handled internally.
- DEV / QA / PROD isolation (never reuse secrets across envs).
- Required request headers assembled by client.

## Security Highlights

All credentials supplied via environment or secret manager. Rotate and never commit.

## Next Steps

Start with `guides/getting-started.md`, then explore `api/index.md`.
