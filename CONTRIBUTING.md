# Contributing to LiteGoDB

Thank you for considering contributing to LiteGoDB! We welcome contributions from the community to make this project better.

## How to Contribute

1. **Fork the Repository**: Start by forking the repository to your GitHub account.

2. **Clone the Repository**: Clone your forked repository to your local machine.

   ```bash
   git clone https://github.com/<your-username>/litegodb.git
   cd litegodb
   ```

3. **Create a Branch**: Create a new branch for your feature or bug fix.

   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make Changes**: Implement your changes in the codebase. Ensure your code adheres to the project's coding standards.

5. **Write Tests**: Add or update tests to cover your changes.

6. **Run Tests**: Ensure all tests pass before submitting your changes.

   ```bash
   go test ./...
   ```

7. **Commit Changes**: Commit your changes with a descriptive commit message.

   ```bash
   git add .
   git commit -m "Add feature: your-feature-name"
   ```

8. **Push Changes**: Push your branch to your forked repository.

   ```bash
   git push origin feature/your-feature-name
   ```

9. **Submit a Pull Request**: Open a pull request to the `main` branch of the original repository. Provide a clear description of your changes and the problem they solve.

## Code Style

- Follow Go's standard formatting guidelines. Use `gofmt` to format your code.
- Write clear and concise comments where necessary.
- Use descriptive variable and function names.

## Reporting Issues

If you encounter a bug or have a feature request, please open an issue on GitHub. Provide as much detail as possible, including steps to reproduce the issue if applicable.

## Community Guidelines

Please adhere to the [Code of Conduct](CODE_OF_CONDUCT.md) when interacting with the community.