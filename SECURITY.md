# Security Policy

## Supported Versions

gogo is currently in the `v0.x` public preview. Security fixes are targeted at
the latest released `v0.x` minor line unless a release note says otherwise.

| Version | Security support |
| --- | --- |
| `v0.1.x` | Supported |
| `< v0.1.0` | Unsupported |

When `v1.0.0` is released, this policy should be updated to describe the
stable release line and any maintained preview branches.

## Reporting a Vulnerability

Please do not report vulnerabilities through public issues, discussions, or
pull requests until a fix or disclosure plan is agreed.

Preferred channel: use GitHub private vulnerability reporting for this
repository, if available:

[GitHub private vulnerability report](https://github.com/Snocko-main/gogo/security/advisories/new)

If private reporting is not available, open a public issue that only asks for
a private security contact. Do not include exploit details, proofs of concept,
crash triggers, credentials, logs with secrets, or vulnerable deployment
details in that issue.

A useful private report includes:

- affected version or commit,
- build tags and platform,
- impact and likely affected API or middleware,
- minimal reproduction steps or proof of concept,
- whether the issue is already public or under active exploitation,
- any suggested fix or mitigation.

This project does not yet publish a formal response SLA. Maintainers should
acknowledge private reports, coordinate a fix, and publish a release note or
advisory when appropriate. Security fixes should avoid broad API changes
unless the release policy for the current `v0.x` line explicitly calls them
out.
