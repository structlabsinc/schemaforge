# Contributing to SchemaForge

We welcome contributions to SchemaForge. To maintain the stability and security required for enterprise environments, please adhere to the following guidelines.

## Development Standards

### 1. Code Style & Quality
*   **Python Version:** All code must be compatible with Python 3.9+.
*   **Linting:** We enforce strict linting rules. Ensure your code passes all checks before submitting.
    *   `flake8` for style compliance.
    *   `bandit` for security vulnerability scanning.
*   **Type Hinting:** Use Python type hints for all function signatures to ensure code clarity and maintainability.

### 2. Testing Requirements
*   **Coverage:** New features must include unit tests. We maintain a strict focus on high code coverage (target >85%).
*   **Regression:** Run the full test suite to ensure no regressions are introduced.
    ```bash
    pytest tests/
    ```
*   **Dependencies:** Ensure `sqlglot` is installed (`pip install sqlglot`).
*   **Edge Cases:** Tests must cover edge cases, including null values, maximum precision limits, and special character handling.

## specific Workflow

### Reporting Issues
When reporting a bug, please include:
1.  **Version Information:** output of `sf --version`.
2.  **Reproduction Steps:** Minimal SQL files (`v1.sql`, `v2.sql`) that demonstrate the issue.
3.  **Expected vs. Actual Output:** Clear description of the discrepancy.

### Submitting Pull Requests
1.  **Fork & Branch:** Create a feature branch from `main`.
2.  **Implementation:** changes should be atomic and strictly scoped.
3.  **Verification:**
    *   Run unit tests: `pytest`
    *   Run security scan: `bandit -r schemaforge/`
4.  **Description:** Provide a detailed description of the changes and the reasoning behind them.
5.  **DCO (Developer Certificate of Origin):** All commits must be signed off to certify that you wrote the code or have the right to contribute it.

## Security Policy

For security vulnerability reports, please do **not** open a public issue. Contact the maintenance team directly via the designated security email address.

## License

By contributing to this repository, you agree that your contributions will be licensed under the Apache 2.0 License.
