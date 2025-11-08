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

## Future Features (Backlog)

### Incremental Episode Updates with Caching
**Problem**: Currently only refreshes series with 0 episodes. New episodes released after initial generation are never detected.

**Example scenario**:
- Week 1: Series has 10 episodes → generates 10 .strm files
- Week 2: Provider releases episode 11 → plugin sees 10 in DB, skips (thinks tree_complete)
- Result: Episode 11 never gets generated ❌

**Solution components** (reference implementations: vodstrmpg, strmvod):

1. **Manifest file tracking** (`.vod2strm_manifest.json`):
   ```json
   {
     "series": {
       "series_id_123": {
         "last_refresh": "2025-11-07T12:00:00Z",
         "episode_count": 10,
         "provider_episode_ids": ["ep1", "ep2", ...]
       }
     }
   }
   ```

2. **Time-based refresh check**:
   - If `last_refresh > 24 hours` → fetch provider episode list
   - Compare provider count vs manifest count
   - If different → refresh needed

3. **Incremental file writes** (from strmvod/vodstrmpg):
   - `write_text_if_changed()` only writes if content differs
   - Avoids unnecessary disk writes for existing .strm files
   - Returns: `(changed: bool, reason: str)` → "unchanged", "written", "dry_run"

4. **Key insight**:
   - Both reference plugins still fetch FULL episode list from provider API
   - "Incremental" means not re-writing unchanged .strm files
   - NOT incremental API calls (provider API returns full list anyway)
   - Our plugin just needs to periodically check provider for new episodes

**Bonus optimization from vodstrmpg**:
- Batched TMDB season fetching: `_tmdb_fetch_season_map(series_id, season_num)`
- Fetches entire season once instead of per-episode HTTP calls
- Cached with key: `season:{series}:{season}` in manifest

---

**Note:** This is a living document. Important project-specific rules and context will be added here as they emerge.
