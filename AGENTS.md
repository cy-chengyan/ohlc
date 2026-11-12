# Project instructions

- Run project commands from the repository root.
- docs/design.md is the sole product and technical design document. Keep requirements, formats, algorithms, capacity calculations, and validation criteria in that file; do not create separate design, research, or historical design documents.
- Keep benchmark environments, execution records, measured results, comparisons, limitations, and evidence locations in dated reports under docs/benchmarks/. Link reports from docs/design.md and the READMEs; reports must not redefine product contracts or validation criteria.
- The first-release baseline is two-dimensional tiling, computed addressing, fixed-length row storage, and no compression. The document defines the implementation defaults and distinguishes calculations from measured performance.
- All user-created tables share the same eight-field, nullable 64-byte row format defined in docs/design.md section 3.1. Reads and writes must identify a table; table IDs and volume ownership must be included in isolation and cache keys.
- Public APIs and the shell accept dates or date-times. Internal time codes are assigned by the engine, remain stable during historical backfill, and must not be required from users. Queries are ordered by actual time.
- Table period metadata describes caller-supplied bars; do not add automatic aggregation, trading-calendar inference, or timestamp rounding. Preserve the date and time-zone contract in docs/design.md.
- Follow the completion criteria in docs/design.md and record implemented capabilities and outstanding work accurately.
- Clarify consequential unresolved requirements before implementation. Keep code clear, layered, efficient, and easy for humans to maintain.
- Follow the accepted coding standards in docs/design.md section 18. High-quality, readable, understandable, and maintainable code is mandatory; do not sacrifice clear control flow or ownership for compressed expressions.
- Write all project code comments in English, with no Chinese. This includes documentation comments, Python docstrings, Java Javadoc, build scripts, tests, and code examples. Design prose and user-facing text may remain Chinese.
- Attach pointer stars to the type: `char* p`, `const char* p`, and `char** p`. Use one variable per declaration. Apply the same alignment to pointer parameters and return types while preserving valid C declarator syntax.
- Use 4-space indentation, a 100-column target, attached opening braces, and braces for all C control-flow bodies. When implementation begins, set `PointerAlignment: Left` and `DerivePointerAlignment: false` explicitly in the versioned clang-format configuration.
- Use meaningful snake_case names in C, `ohlc_` for public symbols, `OHLC_` for macros and enum constants, and `static` for file-private symbols. Document ownership, lifetime, errors, and thread safety at public interfaces.
- Reduce unnecessary testing. Use validation that checks meaningful invariants and actual failure boundaries.
- Do not claim implementation, Linux validation, durability validation, or measured performance that has not actually been completed.
- Keep LICENSE as project metadata. AGENTS.md contains project operation rules, not a second product design.
