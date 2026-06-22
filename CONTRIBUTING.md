# Contributing to go-paxos

Thanks for your interest in contributing! This project aims to be a clean,
educational, and correct implementation of Multi-Paxos. Contributions of all
kinds are welcome — bug reports, documentation improvements, tests, and code.

## Getting started

1. Fork the repository and clone your fork.
2. Make sure you have Go 1.21 or newer installed.
3. Create a branch for your change:

   ```bash
   git checkout -b my-change
   ```

## Development workflow

Before opening a pull request, please make sure the following all pass:

```bash
# Format code (no output means everything is formatted)
gofmt -l .

# Build everything
go build ./...

# Vet for common mistakes
go vet ./...

# Run the full test suite, including the race detector
go test -race ./...
```

New behavior should come with tests. Because this is a consensus library,
correctness matters more than features — small, well-tested changes are
preferred over large ones.

## Pull requests

- Keep each pull request focused on a single change.
- Write a clear description of *what* the change does and *why*.
- Reference any related issues.
- Make sure CI (build, vet, tests) is green.

## Reporting bugs

When filing an issue, please include:

- A clear description of the problem and expected behavior.
- Steps to reproduce (a minimal example or test is ideal).
- Your Go version and operating system.

## Code of conduct

Please be respectful and constructive in all interactions. We want this to be a
welcoming project for people learning about distributed consensus.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE) that covers this project.
