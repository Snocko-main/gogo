# v1 Release Candidate Notes

Use these notes with `ROAD_TO_V1.md` and `docs/release-checklist.md` when
tracking the `v1.0.0-rc.1` state and deciding whether a later RC is needed.

## Tagged rc.1

`v0.9.0` and `v1.0.0-rc.1` were cut as annotated tags on 2026-06-10 from
`origin/main` commit `8c36f0b21ac22569879072200184839c1bb30adf` after the v0.9
release-candidate work merged.

This release-note update records:

- Public API freeze, exported-symbol audit, and docs/package audit before the
  tags.
- Stress and race validation plus downstream smoke testing before the tags.
- Dated `CHANGELOG.md` entries for `v0.9.0` and `v1.0.0-rc.1` after the tags
  were created.
- Release checklist coverage for required checks and post-tag verification.

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
