#!/usr/bin/env python3
"""Retired direct-notification sender.

The former script sent a dozen fabricated production-looking alerts at import
time and accepted secrets on the command line.  Notification verification now
belongs to the durable outbox integration tests and an explicitly configured
operator diagnostic in the canonical runtime.
"""


def main() -> int:
    raise SystemExit(
        "Direct Telegram test sending is disabled. Run the integration/outbox "
        "test suite; never pass bot credentials as command-line arguments."
    )


if __name__ == "__main__":
    main()
