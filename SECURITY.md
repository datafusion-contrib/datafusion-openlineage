# Security policy

## Reporting a vulnerability

Please **don't** report security vulnerabilities in public issues, discussions
or pull requests.

Report them privately through GitHub instead:
[**Report a vulnerability**](https://github.com/datafusion-contrib/datafusion-openlineage/security/advisories/new)
(Security tab → Advisories → Report a vulnerability).

Include the affected crate and version, a description of the issue and its
impact, and steps or code to reproduce it. We aim to acknowledge reports within
a week and will keep you updated on the fix and the disclosure timeline. We're
happy to credit you in the advisory.

## Supported versions

Fixes go into the latest release of each crate. The crates are pre-1.0, so we
don't backport to older versions.

| Crate | Supported |
|---|---|
| `datafusion-openlineage` | latest release |
| `openlineage-client` | latest release |

## Scope

In scope: the code in this repository and the crates published from it. If
the vulnerability is in a dependency (for example DataFusion or Arrow), please
report it to that project. We'll pick up the fixed release.
