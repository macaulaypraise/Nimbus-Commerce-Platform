# Nimbus — env file safety hook

This pre-commit hook performs two checks on every commit:

1. **Blocks populated `.env` and `.env.test` files.** A file is considered
   "populated" if it contains any of:

   - A `SECRET_KEY=` value that isn't one of the documented placeholders
     (`CHANGE_ME...`, `dev-only-secret...`, `test-secret...`).
   - A `DATABASE_URL=` or `TEST_DATABASE_URL=` whose credentials are not
     `nimbus:nimbus@127...`.
   - A `REDIS_URL=` or `TEST_REDIS_URL=` whose host is not `127...`.
   - Any URL with embedded credentials (`https://user:pass@host`).
   - AWS access keys (`AKIA...`).
   - PEM-encoded private keys.

   The `.env.example`, `.env.test.example`, and `.env.docker` files are
   exempt — they're committed templates.

2. **Audits required keys** in every `.env*` file that exists in the
   working tree. Required keys are derived from the aliases in
   `src/core/config.py`. Missing keys fail the hook with a clear
   remediation message.

## Bypass

In an emergency:

```bash
SKIP=check-nimbus-env git commit -m "..."
```

Bypassing is recorded in the git log by the very fact that the hook did
not run. Reviewers should challenge bypasses during code review.
