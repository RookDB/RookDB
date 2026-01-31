Please follow the guidelines below when contributing to this repository.

## Code Contribution

1. Fork this repository to your GitHub account.

2. Create a new branch using the following naming convention:  
   `<rollnumber>-<your-name>`

3. Commit your changes with meaningful commit messages.

4. Push the branch to your forked repository.

5. Create a Pull Request to the main branch of this repository.
   - Use a clear and descriptive title.
   - Briefly describe the changes made.


## Documentation Contribution

- Currently three documents are available:
  - **Design Doc** (Overall RookDB Architecture)
  - **API Doc** (Code-specific functions documentation)
  - **Database Doc** (Database architecture documentation)

- All documents are located in the `docs/` folder.

- For every document:
  - A corresponding `<document-name>.md` file exists
  - A generated `<document-name>.pdf` file is also present

- Markdown files are converted to PDF using **Pandoc** and **markdown-pdf**.

### Tools

- Pandoc: https://pandoc.org  
- Markdown-PDF: https://pypi.org/project/markdown-pdf/

---

### PDF Generation Instructions

#### API Doc and Database Doc

For `API-Doc.md` and `Database-Doc.md`, use **markdown-pdf**:

```bash
markdown-pdf API-Doc.md -o API-Doc.pdf
markdown-pdf Database-Doc.md -o Database-Doc.pdf
```

#### Design Doc
For `Design-Doc.md`, use Pandoc:
```
pandoc -f markdown Design-Doc.md -o Design-Doc.pdf
```
Pandoc is used here because markdown-pdf does not correctly render images while generating the PDF.


### Contribution Steps
1. Update the required Markdown file (.md).
2. Generate the corresponding PDF file.
3. Create a fork of the repository.
4. Create a branch with the naming format:
   `docs-<document-name>`
5. Commit your changes and push the branch.
6. Submit a Pull Request.