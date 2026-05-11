Please follow the guidelines below when contributing to this repository.

## Code Contribution
1. Create a branch using the naming convention: *feature-name*.
2. Make changes with meaningful commit messages.
3. Before pushing your changes, run
```
cargo test
```
to verify that all test cases pass successfully. A pre-push hook is configured to automatically run this command before every push. If the build fails, the push will be blocked.
4. Push your changes.
5. Add tests (if applicable) and update documentation in `https://github.com/RookDB/docs` repo.
6. Submit a Pull Request to the main branch with a clear title and summary.


### Test case Contribution
* For every new function, write a corresponding test case in the tests/ directory.
* Name the test file using the following format: `<test_function_name>.rs`
* Refer to existing test cases for guidance on structure and style.
