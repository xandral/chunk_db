# ChunkDB Agent Ecosystem

This document describes the Claude Code agent ecosystem configured for ChunkDB development. Agents are specialized AI assistants that can be invoked to handle specific tasks autonomously.

## Quick Reference

| Agent | Model | Purpose | Updates Docs? | Invocation |
|-------|-------|---------|---------------|------------|
| `codebase-analyzer` | opus | Deep codebase audit | CLAUDE.md (issues, architecture, types) | Automatic on audit requests |
| `test-runner` | haiku | Run tests, report results | CLAUDE.md (test counts) | After code changes |
| `unit-test-writer` | sonnet | Write unit tests | CLAUDE.md (test counts, architecture) | When adding features |
| `benchmark-runner` | sonnet | Run & analyze benchmarks | CLAUDE.md (perf issues), benchmarks/README.md | Performance checks |
| `doc-reviewer` | sonnet | Fix documentation drift | ALL docs (CLAUDE.md, README, agents.md, examples) | Before releases |

## Agent Files

All agents live in `.claude/agents/` as Markdown files with YAML frontmatter:

```
.claude/agents/
├── codebase-analyzer.md    # Full codebase audit (opus, most thorough)
├── test-runner.md          # Run cargo test + verify examples (haiku, fast)
├── unit-test-writer.md     # Write idiomatic Rust tests (sonnet)
├── benchmark-runner.md     # Run benchmarks, analyze results (sonnet)
└── doc-reviewer.md         # Cross-reference docs vs code (sonnet)
```

## Agent Details

### codebase-analyzer

**Model**: opus (most capable, for deep analysis)
**Memory**: project-scoped (`.claude/agent-memory/codebase-analyzer/`)
**Color**: green

Performs comprehensive codebase audits following a structured methodology:
1. **Context Gathering** - reads specs, README, architecture docs
2. **Systematic Code Review** - analyzes each module for bugs, efficiency, spec drift
3. **Cross-Cutting Concerns** - consistency, test coverage, documentation accuracy

Outputs a structured report with severity-ranked issues, spec compliance matrix, and prioritized action items. Persists findings in its agent memory for future reference.

**When to use**: Before releases, after major refactors, periodic code health checks, when suspecting spec drift.

**Example prompt to trigger it**:
```
"Check the codebase for any issues before the release"
"Verify the implementation matches the README specs"
```

### test-runner

**Model**: haiku (fast, cheap - just runs commands)
**Memory**: project-scoped
**Color**: blue

Runs `cargo test` and `cargo build --examples --release`, then reports:
- Pass/fail status with test counts
- Failing test details with file:line and likely causes
- Compiler warnings
- Example compilation status

**When to use**: After every code change. This agent is intentionally lightweight for frequent use.

**Example prompt to trigger it**:
```
"Run all tests"
"Make sure nothing is broken"
```

### unit-test-writer

**Model**: sonnet (balanced - needs code generation ability)
**Memory**: project-scoped
**Color**: yellow

Writes idiomatic Rust tests following ChunkDB conventions:
- Unit tests (`#[cfg(test)] mod tests {}`) in source files
- Integration tests in `tests/integration_test.rs`
- Uses `tempfile::tempdir()` for test databases
- Follows Arrange-Act-Assert pattern
- Covers happy path, edge cases, error cases, boundary conditions

**When to use**: After implementing new features, fixing bugs (regression tests), improving coverage.

**Example prompt to trigger it**:
```
"Write tests for the new between filter"
"Add unit tests for hash_registry"
"We need regression tests for the avg() fix"
```

### benchmark-runner

**Model**: sonnet (needs analytical capability)
**Memory**: project-scoped
**Color**: cyan

Runs one or more of the 4 available benchmarks and provides performance analysis:
- `query_benchmark` - quick single-config check
- `chunkdb_vs_duckdb_benchmark` - ChunkDB vs DuckDB comparison
- `parametric_benchmark` - 1000-config parameter sweep
- `column_scaling_benchmark` - column count scaling

Reports absolute latency, throughput, pruning effectiveness, and comparison ratios.

**When to use**: After optimizations, before releases, investigating performance regressions.

**Example prompt to trigger it**:
```
"Run benchmarks to see if the optimization helped"
"Compare our performance against DuckDB"
```

### doc-reviewer

**Model**: sonnet (needs reading comprehension)
**Memory**: project-scoped
**Color**: magenta

Cross-references all documentation against the codebase:
- Verifies API examples compile (types, signatures)
- Checks file/example references exist
- Validates test counts and benchmark scripts
- Detects terminology inconsistencies
- Flags residual non-English text

**When to use**: Before presentations, after API changes, periodic doc health checks.

**Example prompt to trigger it**:
```
"Make sure the docs still match the code"
"Review the README for accuracy"
```

## Agent Memory

Each agent has persistent memory in `.claude/agent-memory/<agent-name>/`:

```
.claude/agent-memory/
└── codebase-analyzer/
    ├── MEMORY.md         # Key findings (loaded every session, max 200 lines)
    ├── architecture.md   # Component reference
    └── known-issues.md   # Bug/issue tracking
```

The first 200 lines of `MEMORY.md` are injected into the agent's system prompt every time it runs, giving it institutional memory across sessions.

**Memory scopes**:
- `project` - stored in `.claude/agent-memory/` (shared via git)
- `user` - stored in `~/.claude/agent-memory/` (personal)
- `local` - stored in `.claude/agent-memory-local/` (not committed)

## Agent Frontmatter Reference

```yaml
---
name: agent-name              # Unique identifier (lowercase, hyphens, max 64 chars)
description: "When to use..." # Triggers automatic delegation
model: sonnet                 # sonnet | opus | haiku | inherit
color: blue                   # Terminal color for output
memory: project               # project | user | local
tools: Read, Grep, Bash       # Restrict available tools (omit = all)
disallowedTools: Write, Edit  # Deny specific tools
maxTurns: 50                  # Max API round-trips
permissionMode: default       # default | acceptEdits | plan | dontAsk
hooks:                        # Agent-scoped hooks
  PreToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "./scripts/validate.sh"
---

System prompt content here...
```

## Orchestration Patterns

### Sequential: Fix then verify

Claude will naturally chain agents when appropriate:
```
User: "Fix the avg() bug and make sure everything still works"
→ Claude fixes the code
→ Claude spawns test-runner to verify
→ Claude reports results
```

### Parallel: Multi-concern analysis

```
User: "Full pre-release check"
→ Claude spawns codebase-analyzer (background)
→ Claude spawns doc-reviewer (background)
→ Claude spawns test-runner (foreground)
→ Claude aggregates all results
```

### Pipeline: Write → Test → Benchmark

```
User: "Add caching to all_chunks and verify it helps"
→ Claude implements the cache
→ Claude spawns unit-test-writer for cache tests
→ Claude spawns test-runner to verify
→ Claude spawns benchmark-runner to measure improvement
```

## Documentation Maintenance Policy

Every agent has a **mandatory** Documentation Maintenance section. After completing its primary task, each agent must update project docs if it discovers significant changes.

### What each agent updates:

| Agent | CLAUDE.md | docs/agents.md | README.md | Agent Memory |
|-------|-----------|----------------|-----------|--------------|
| `codebase-analyzer` | Architecture, Key Types, Known Issues | Agent responsibilities | - | Full update |
| `test-runner` | Test counts | - | - | - |
| `unit-test-writer` | Test counts, Architecture (new files) | - | - | Coverage gaps |
| `benchmark-runner` | Known Performance Issues | Benchmark table | - | Baseline numbers |
| `doc-reviewer` | Everything | Everything | Everything | - |

### What counts as "significant":
- API signature changes → always
- New/removed source files → always
- Test count changes → always
- New PERF/BUG annotations → always
- Style/formatting only → agent memory only

### The doc-reviewer difference:
The `doc-reviewer` is unique: it **fixes** discrepancies directly rather than just reporting them. All other agents report their findings and update docs as a side effect of their primary task.

## Adding New Agents

1. Create `.claude/agents/<name>.md` with YAML frontmatter
2. Write a clear description (used for automatic delegation)
3. Include examples in the description showing when to delegate
4. Write the system prompt with specific instructions
5. Choose the right model:
   - **haiku**: Fast tasks (running commands, simple checks)
   - **sonnet**: Balanced tasks (code generation, analysis)
   - **opus**: Complex tasks (deep reasoning, architecture review)

## Configuration Files

| File | Purpose |
|------|---------|
| `.claude/agents/*.md` | Agent definitions |
| `.claude/agent-memory/*/MEMORY.md` | Agent persistent memory |
| `.claude/settings.json` | Shared project settings (hooks, permissions) |
| `.claude/settings.local.json` | Personal settings (not committed) |
| `CLAUDE.md` | Project-wide instructions for Claude |
