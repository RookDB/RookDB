Please follow the guidelines below when contributing to this repository.

## Code Contribution

1. Fork this repository to your GitHub account.

2. Create a new branch following the naming convention: **feature-name**

3. Make your changes and commit them using clear and meaningful commit messages.

4. Push the branch to your forked repository.

5. If you introduce any new functions:
   - Add corresponding test case, see [Test Case Writing Guidelines](#test-case).
   - Document the function following the style used in `API-Doc.md`, see [API Doc Contribution Guidlines](#api-doc-contribution)

6. Open a Pull Request targeting the develop branch of this repository.
   - Use a clear PR title.
   - Provide a brief summary of the changes made.

## Documentation Contribution

- Currently three documents are available:
  - **Design Doc** (Overall RookDB Architecture)
  - **API Doc** (Code-specific functions documentation)
  - **Database Doc** (Database architecture documentation)

- All documents are located in the `docs/` folder.

- For every document:
  - A corresponding `<document-name>.md` file exists
  - A generated `<document-name>.pdf` file is also present

- Markdown files are converted to PDF using **Pandoc** and **markdown-pdf** tools.
The following tools are used for PDF generation:
- Pandoc: https://pandoc.org  
- Markdown-PDF: https://pypi.org/project/markdown-pdf/

---

#### Database Doc Contribution
If there are any changes to the database documentation layout, update the `Database-Doc.md` file and generate the corresponding PDF using markdown-pdf:

```bash
markdown-pdf Database-Doc.md -o Database-Doc.pdf
```


#### API Doc Contribution
* Add the function name to the index section of `API-Doc.md`.
* In the API descriptions section, add a complete entry for the function that includes:
   - API description
   - Inputs
   - Outputs
   - Implementation steps
* Follow the format used in the existing API documentation.
* After updating API-Doc.md, regenerate the corresponding PDF using markdown-pdf:

```bash
markdown-pdf API-Doc.md -o API-Doc.pdf
```

### Design Doc Contribution
* If there are any changes to the design or architecture of RookDB, reflect those changes in `Design-Doc.md`.
* After updating the Markdown file, generate the corresponding PDF using Pandoc:
```bash
pandoc -f markdown Design-Doc.md -o Design-Doc.pdf
```
> Note: Pandoc is used because markdown-pdf does not correctly render images during PDF generation.


### Test case
* For every new function, write a corresponding test case in the tests/ directory.
* Name the test file using the following format: `<test_function_name>.rs`
* Refer to existing test cases for guidance on structure and style.