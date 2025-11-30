# Project Governance

## Branch Protection Rules
To ensure code stability and quality, the `main` branch is protected.

### Policy
1.  **Require Pull Requests**: Direct pushes to `main` are prohibited. All changes must be merged via a Pull Request (PR).
2.  **Code Review**: At least one approval is required before merging.
3.  **Status Checks**: CI/CD builds (tests) must pass before merging.
4.  **Exceptions**: The service account/admin user `schemaforge` is exempt from these rules to perform automated releases or critical hotfixes.

### Setup Instructions (GitHub Admin)
1.  Go to **Settings** > **Branches**.
2.  Click **Add branch protection rule**.
3.  **Branch name pattern**: `main`.
4.  Check **Require a pull request before merging**.
5.  Check **Require status checks to pass before merging** (Select `Build Linux`, `Build Windows`, `Build MacOS`).
6.  Check **Do not allow bypassing the above settings**.
7.  **Exemption**: Add `schemaforge` to the "Allow specified actors to bypass required pull requests" list (if available in your plan) or "Allow force pushes" / "Allow deletions" if strictly necessary for release tags (though generally discouraged).

## Release Process
Releases are automated via GitHub Actions.
1.  Update `VERSION` file.
2.  Push a tag `vX.Y.Z`.
3.  The workflow will build binaries and create a GitHub Release.
