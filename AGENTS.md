# AGENTS.md

## Pipeline Architecture

Refer to `pipeline.md` for the pipeline architecture, data flow, layer responsibilities, and transformation requirements. All pipeline-related implementations and modifications should follow the design documented in `docs/pipeline.md`.

## General Guidelines

### Code Quality
- Write clean, readable, maintainable code with descriptive variable and function names.
- Prefer simple, straightforward solutions over unnecessary abstractions or clever implementations.
- Follow existing project conventions and maintain a consistent coding style.
- Keep functions focused on a single responsibility.
- Avoid duplicated logic when a reusable solution is appropriate.
- Do not introduce unnecessary dependencies, frameworks, or configuration.

### Comments and Documentation
- Write self-explanatory code whenever possible.
- Use inline comments to explain non-obvious logic or important design decisions, not to restate what the code does.
- Keep docstrings concise and focused on behavior, parameters, and return values when clarification is needed.
- Keep documentation reasonably short, structured, and relevant.
- Avoid excessive explanations, redundant examples, and unnecessary boilerplate.
- Update existing documentation when implementation changes affect documented behavior.

### Architecture and Organization
- Respect existing architectural decisions and project structure.
- Refer to `pipeline.md` before implementing or modifying pipeline components.
- Maintain clear separation of concerns between modules and pipeline layers.
- Avoid premature optimization and overengineering.
- Prefer modifying existing components over introducing new ones when appropriate.

### Reliability and Testing
- Handle errors explicitly and provide actionable error messages.
- Avoid silently swallowing exceptions or failures.
- Write focused tests for meaningful behavior and edge cases.
- Do not add tests solely to increase test counts or coverage metrics.
- Ensure changes do not introduce regressions in existing functionality.
- Never hardcode secrets, credentials, or environment-specific paths.

### Scope and Changes
- Make the smallest reasonable change that fully addresses the task.
- Avoid unrelated refactoring or modifications.
- Do not introduce speculative features or abstractions for hypothetical future requirements.
- Remove obsolete code when replacing functionality, provided it is no longer used.
- Favor consistency with the existing codebase over personal stylistic preferences.
