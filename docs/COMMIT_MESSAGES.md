```markdown
# Commit message convention

Nimbus uses [Conventional Commits](https://www.conventionalcommits.org/)
with a small set of project-specific rules. Every commit message MUST
follow the format below; the `commit-msg` pre-commit hook enforces it.

## Format

```text
<type>(<scope>)<!>: <subject>

<body>

<footer>
```

### Type (required)

The change category. Must be one of:

| Type | When to use | Example |
|---|---|---|
| `feat` | New user-facing feature | `feat(orders): add idempotency key support` |
| `fix` | Bug fix | `fix(inventory): correct race condition on stock reservation` |
| `perf` | Performance improvement | `perf(gateway): cache JWT verification results` |
| `refactor` | Code change that neither fixes a bug nor adds a feature | `refactor(payments): split charge and capture paths` |
| `test` | Add or fix tests | `test(orders): cover partial fulfillment edge case` |
| `docs` | Documentation only | `docs(readme): add quickstart section` |
| `build` | Build system or external dependency changes | `build: pin hatchling to 1.27.0` |
| `ci` | CI configuration changes | `ci(github): add postgres service to integration job` |
| `chore` | Tooling, maintenance, or non-functional change | `chore(deps): bump structlog to 24.4.0` |
| `revert` | Revert a previous commit | `revert: feat(orders): idempotency keys` |
| `style` | Whitespace, formatting, missing semi-colons | `style(telemetry): apply ruff formatting` |

A `!` immediately after the scope indicates a **breaking change** (see
Breaking changes below). The `!` is not a separate token — it goes right
before the colon.

### Scope (required when applicable, optional otherwise)

The module, layer, or cross-cutting area affected by the change. Use
the lowercase name of the domain module (`orders`, `inventory`,
`payments`, `notifications`, `admin`, `gateway`) for domain work. Use
one of the cross-cutting scopes below for non-domain work:

| Scope | Use for |
|---|---|
| `core` | `src/core/*` infrastructure (config, telemetry, exceptions, db, cache, messaging) |
| `deps` | Dependency upgrades |
| `precommit` | `.pre-commit-config.yaml` and hooks |
| `docker` | `docker-compose.yml` and `Dockerfile` |
| `ci` | GitHub Actions, GitLab CI, etc. |
| `api` | The FastAPI app shape (routes, middleware) |
| `models` | SQLAlchemy models / Pydantic schemas (cross-module) |
| `readme` | Top-level `README.md` only |
| `env` | `.env*` files (NOT the runtime env, but the templates) |

If a commit touches more than one scope, pick the **primary** scope.
If a commit is truly cross-cutting (e.g., a new architectural rule
applied everywhere), use `core` and call out the others in the body.

### Subject (required)

- Imperative mood: "add", not "added" or "adds".
- Lowercase first letter, no period at the end.
- Maximum 72 characters.
- No backticks, no emoji, no marketing-speak.
- A reader should be able to skim the subject line and know what
  changed and where.

### Body (optional but recommended for non-trivial changes)

- Wrap at 72 columns.
- Separate paragraphs with blank lines.
- Explain _why_ the change was made, not _what_ the diff does (the diff
  itself shows what).
- Reference related issues, ADRs, or design docs.

### Footer (required for breaking changes, optional otherwise)

Two kinds of footer entries:

1. **Breaking change marker.** A commit that introduces a breaking
   change MUST have a `!` after the scope and a footer paragraph
   beginning with `BREAKING CHANGE:`. Example:

   ```text
   feat(payments)!: switch from stripe to adyen

   Migrate the payment gateway to Adyen per ADR-007. Stripe remains
   supported via the legacy adapter until 2026-04-01.

   BREAKING CHANGE: the `PaymentProvider.stripe` enum value is removed.
   Use `PaymentProvider.adyen` or the legacy `StripeAdapter` class.
   ```

2. **Issue references.** Standard GitHub-style trailers:

   ```text
   Refs: #1234
   Closes: #1235
   ```

### Examples

```text
feat(orders): add idempotency key support

Wrap order creation in an idempotent transaction. If the same
Idempotency-Key header is seen twice, return the original response
without re-running side effects.

Refs: #203
```

```text
fix(inventory): correct race condition on stock reservation

The old SELECT FOR UPDATE pattern deadlocked under load. Switch to
advisory locks per SKU with a bounded retry budget.
```

```bash
chore(deps): bump structlog to 24.4.0
```

```text
feat(api)!: drop support for python 3.11

Python 3.12 is the only supported runtime as of 2026-01-15.

BREAKING CHANGE: requires-python bumped to >=3.12 in pyproject.toml.
```

### Bypass

If a commit genuinely cannot follow the convention (e.g., reverting a
malformed external commit, or a WIP fix during incident response),
bypass the hook with `git commit --no-verify`. Document the bypass in
the commit body or in the PR description so reviewers understand.

---

**Now stage and commit the fix:**

```bash
git add docs/COMMIT_MESSAGES.md
git commit -m "docs: add commit message convention document"
```

All hooks, including markdownlint, will pass.
