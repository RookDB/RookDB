Please follow the guidelines below when contributing to this repository.

## Branching Strategy
* The main working branch is `develop` branch.
* Submit all Pull Requests targeting `develop` branch.
* `main` is reserved for stable releases and will be periodically updated by merging changes from the `develop` branch.
* Each project already has a dedicated branch. Push your changes to that branch and open a Pull Request with clear title and summary of changes to the `develop` branch.

## Documentation Contribution
Please refer to `docs/README.md` for documentation contribution guidelines.

### Test case Contribution
* For every new function, write a corresponding test case in the tests/ directory.
* Name the test file using the following format: `<test_function_name>.rs`
* Refer to existing test cases for guidance on structure and style.