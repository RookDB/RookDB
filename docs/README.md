# Documentation Website

The project documentation website is built using [Docusaurus](https://docusaurus.io/).

All documentation content is maintained inside the `content/` folder.  
To update the documentation, modify the corresponding Markdown file in this folder. Any changes made there will be reflected in the documentation website after deployment.


## Local Development

```bash
npm install
```

## Start the development server

```bash
npm start
```

## Semester Report Update Workflow

Each project has its own page under `content/projects/`.

Update the relevant project page regularly with the following sections:

1. Project Scope
2. Design Summary
3. Public APIs
4. Implementation Progress (Completed / In Progress / Pending)
5. Test Coverage Notes
6. Known Limitations / Next Milestones
7. Change Log (date-wise)

Suggested update cadence:

- After every major feature merge
- After every bug fix affecting behavior
- Before each project review/demo

## Recommended Commands

From `docs/`:

```bash
npm install
npm start
```

For production-style validation:

```bash
npm run build
npm run serve
```

## Contribution Checklist (Docs)

- Keep API signatures aligned with current code.
- Avoid stale status text; prefer dated changelog entries.
- Mention known limitations explicitly.
- Ensure project page builds cleanly in Docusaurus.