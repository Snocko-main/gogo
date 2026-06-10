# v1 Release Candidate Runbook

Use this runbook with `ROAD_TO_V1.md` and `docs/release-checklist.md` when
preparing `v1.0.0-rc.1`. It does not create the tag.

## When to Tag rc.1

Tag `v1.0.0-rc.1` from `origin/main` only after the v0.9 PRs for the public API
freeze, exported-symbol audit, docs/package audit, stress and race validation,
downstream smoke testing, changelog, and release notes have merged or been
explicitly marked non-blocking by a maintainer.

Before tagging, confirm:

- `origin/main` contains all accepted v0.9 release-candidate work.
- `CHANGELOG.md` has a dated `v1.0.0-rc.1` entry with any breaking or
  compatibility-sensitive changes called out.
- The required checks in `docs/release-checklist.md` have passed locally or in
  the matching CI run.
- The downstream smoke test has passed from a clean temporary module.

## Freeze After rc.1

After `v1.0.0-rc.1`, accept only:

- Blocker fixes for security issues, data races, panics, deadlocks, build or
  install failures, broken release checks, or documented v1 API behavior
  regressions.
- Explicitly approved v1-readiness changes that are narrow, reviewed by a
  maintainer, and needed before `v1.0.0`.
- Release-note or documentation corrections that prevent unsafe, misleading, or
  incorrect release guidance.

Do not accept new public APIs, broad refactors, cosmetic documentation churn,
new dependencies, or performance-only changes unless a maintainer explicitly
marks them as v1 blockers.

## Blocker Fix Process

Each blocker PR should state the impacted release line, why the issue blocks
`v1.0.0`, the owning lane, the smallest safe fix, and the validation run. Keep
the diff focused on the blocker and update `CHANGELOG.md` when the fix changes
user-visible behavior, compatibility, or security posture.

If a blocker fix lands after `rc.1`, cut a new release candidate instead of
moving directly to `v1.0.0` unless maintainers explicitly decide the change is
documentation-only and does not affect released behavior.
