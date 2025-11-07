# Project Rules for Claude

## Meta: Maintaining This File
- **This file is your persistent memory across sessions**
- When important context emerges during work, ADD IT HERE and commit immediately - no questions asked
- This includes: architecture decisions, user preferences, critical bugs to avoid, workflow patterns, etc.
- Keep it concise but comprehensive - future you depends on this

## Git & Release Management
- **NEVER merge PRs to main without explicit user approval**
- **Main branch is protected - cannot push directly or use gh CLI to modify it**
- Create PRs and STOP - let the user review and merge them
- Only commit to feature branches (must use `claude/*` prefix with session ID suffix)
- **Create separate branch/PR for each logical fix** - one issue = one branch/PR
- This makes reviews easier and allows independent merging

## Code Review Process (Codex)
- Codex automatically reviews every PR on initial push
- Subsequent pushes to the same PR require a comment: `@codex review`
- Review status emojis:
  - 👀 = Codex is reviewing
  - 👍 = Passed review
  - Comments = Issues found, needs fixes
- Check PR comments yourself to see Codex feedback before asking user

## GitHub Access
- If you need to check PRs/issues/comments, you need a GitHub personal access token
- **DO NOT store tokens in files** - they are session-specific
- If you don't have a token (e.g., after context reset), ask the user for it
- Use curl with GitHub API for PR operations (gh CLI doesn't work with protected main)

---

**Note:** This is a living document. Important project-specific rules and context will be added here as they emerge.
