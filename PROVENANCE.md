# Source and release provenance

QUANT-SWARM-XAU 4.0.0-rc2 is a remediation release. Its local Git history
starts at sanitized import commit `1c91f8d`; the predecessor repository history
was not available when that import was created.

The import is anchored to the historical archive
`QUANT_SWARM_XAU_EA-v4.0.0-rc1-area6-hotfix5-20260717-195554.tar.gz`, whose
SHA-256 digest is:

```text
70695105ca5ab1597237a6347b59294d02ccfa71f831f15e6c1004609dc3293b
```

That digest records continuity with the supplied material. It is not proof of
authorship, an independently authenticated chain of custody, or approval of
the archive. The historical archive contains files outside the canonical
runtime boundary and must not be deployed or repackaged as RC2.

Every RC2 release manifest must bind:

- the exact Git commit and commit-derived source epoch;
- the exact runtime input allowlist;
- every packaged file and its SHA-256 digest;
- the native MetaEditor compiler evidence;
- the exact MQ5 source and newly compiled EX5 digests;
- the Python 3.12 runtime dependency lock;
- `real_capital_enabled=false`.

Only a dedicated release key stored outside this repository may sign release
manifests and archives. A signed RC2 artifact authenticates the stated RC2
inputs; it does not retroactively authenticate the imported history.
