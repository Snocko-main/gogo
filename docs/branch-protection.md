# Branch Protection Expectations

These settings are expected before the `v1.0.0` release candidate.

## Protected Branch

Protect `main`.

Recommended rules:

- Require pull requests before merging.
- Require at least one approving review.
- Dismiss stale approvals when new commits are pushed.
- Require conversations to be resolved before merge.
- Require the branch to be up to date before merge, or require a merge queue.
- Block force pushes.
- Block branch deletion.

## Required Status Checks

Require the `Release Hygiene` workflow checks:

- `Go tests (ubuntu-latest, Go 1.24.x)`
- `Go tests (ubuntu-latest, Go 1.26.x)`
- `Go tests (macos-latest, Go 1.24.x)`
- `Go tests (macos-latest, Go 1.26.x)`
- `Native tests (ubuntu-latest, Go 1.24.x)`
- `Native tests (ubuntu-latest, Go 1.26.x)`
- `Native tests (macos-latest, Go 1.24.x)`
- `Native tests (macos-latest, Go 1.26.x)`
- `Release checks`

The current stable Go version is configured in
`.github/workflows/release-hygiene.yml` through `GO_STABLE_VERSION`.

## Merge Discipline

- Use branch prefixes `feat/`, `fix/`, `chore/`, or `doc/`.
- Keep release-engineering, native, API, middleware, WebSocket, and docs work
  in separate PRs unless an integrator PR is explicitly planned.
- Do not merge broad cross-lane changes without a clear checklist in the PR
  description.
- Keep security-gate review output finding-first, with tests and residual risk
  after the findings.
