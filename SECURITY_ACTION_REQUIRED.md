# Security action required before any trading

Credentials previously stored in this project must be treated as compromised.
Sanitizing the local files does not revoke them.

Before enabling integrations or MT5 AutoTrading:

1. Revoke the exposed Telegram bot token with BotFather and create a new token.
2. Rotate the exposed Supabase service-role key in the Supabase dashboard. Review
   authentication, database, storage, and API logs for unexpected use.
3. Invalidate any other value that was copied from the same credential note.
4. Store replacements only in a local `0600` environment file or an operating-
   system secret manager. Do not commit or archive that file.
5. Purge the old values from the real Git history, backups, chat transcripts,
   deployment images, and shared copies.
6. The legacy project ZIP was locally sanitized on 2026-07-15 and now contains
   only an environment placeholder in its archived `QUICK_START.sh`. Treat any
   older or externally shared copy as compromised and destroy or sanitize it.
7. Run the repository secret scanner and inspect the output before deployment.

Do not paste old or replacement secret values into an issue, commit message,
terminal command argument, test output, or support conversation.
