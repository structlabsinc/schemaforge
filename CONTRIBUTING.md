# Contributing to SchemaForge

Thank you for your interest in contributing to SchemaForge! We welcome contributions from the community.

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally.
    ```bash
    git clone https://github.com/YOUR_USERNAME/schemaforge.git
    cd schemaforge
    ```
3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Development Workflow

1.  **Create a Branch**: Always work on a feature branch (`feature/my-feature`) or fix branch (`fix/issue-123`).
2.  **Make Changes**: Write clean, documented code.
3.  **Run Tests**: Ensure all tests pass before submitting.
    ```bash
    # Run unit tests
    python3 -m unittest discover tests
    
    # Run specific blackbox tests (optional)
    python3 tests/blackbox/massive_runner.py --dialect sqlite --count 10
    ```
4.  **Commit**: Use clear, descriptive commit messages.

## Pull Request Process

1.  Push your branch to your fork.
2.  Open a **Pull Request** (PR) against the `main` branch.
3.  Ensure CI/CD checks pass.
4.  Wait for review from the maintainers.

## License
By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
