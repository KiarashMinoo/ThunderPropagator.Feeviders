---
mode: agent
description: "Deep recursion docs under /docs (no src segments). Generate rich README.md for EVERY folder with real API details from *.cs; never leave a README empty. Parents only link to children. Exclude tests. Strip leading 'RapidStreamer' from segment names. Wipe /docs first. Merge duplicates only within the same source folder, then remove originals. Include RapidStreamer* NuGet metadata (GitHub Packages). Create /docs/README.md landing and update root README. Add per-folder retry (up to 3 passes) + empty-prevention rules + coverage audit so a single run converges."
---

# Docs-Only Mirror (deep recursion; no src; no tests; **no empty READMEs**; auto-retry)

Act autonomously and **do not ask for confirmations** unless Copilot requires it. Use safe defaults.

## Structural Rules (kept)
- **Deep recursion**: create `/docs/<canonical path>/README.md` for **every** non-excluded folder (any depth).
- **Only README.md files** (plus `/docs/README.md` and root `/README.md` update).
- **Parents link to direct children only** (no child content merged into parent).
- **Drop all `src` segments** from docs paths (case-insensitive).
- **Strip leading `RapidStreamer`** from each path segment (case-insensitive; optional `.`/`-`/`_`), but **do not split on `.`** beyond removing `RapidStreamer.`.
- **Exclude tests** entirely.
- **Wipe `/docs`** before rebuilding.
- **RapidStreamer NuGet** hosted at **GitHub Packages**: `https://nuget.pkg.github.com/KiarashMinoo/index.json`.
- **Create `/docs/README.md`** landing (link Application & Infrastructure if present); **update root `/README.md`** to link to `docs/README.md`.
- **No “Source Location” / file system paths** in docs. Use **cross-doc links + anchors** only.

## Empty-Prevention Rules (required)
A folder README **must not** be empty or skeletal. If initial extraction yields too little, escalate **in the same run**:

**Pass 1 — Public surface (normal)**
- Parse `*.cs` in this folder for **public** types/members, XML docs, attributes (serialization/validation).
- Produce all required sections (see “Folder README — Required Sections”).

**If README is still sparse (e.g., < 10 meaningful lines OR no Types table when code exists), do Pass 2.**

**Pass 2 — Broader scan (internal)**
- Include **internal** types/members for summaries.
- Use symbol names, attributes, and usage patterns to infer one-liners.
- Build a **Files** table for every file in the folder (ignore tests), with a short inferred responsibility.

**If sparse again, do Pass 3.**

**Pass 3 — Heuristics & fallbacks**
- Derive concepts from file/namespace names (e.g., “Pipelines”, “Adapters”, “Models”).
- Generate at least:
  - Overview (2–5 sentences),
  - Files table (all files),
  - A minimal **API Reference (Summary)** listing key types (public+internal) with one-line descriptions,
  - One **Usage Recipe** relevant to the folder (even if generic, but realistic for the domain).
- If absolutely no `.cs` files and no children: create a **Leaf README** with “Files: _None_” and a TODO line prompting future description.
- Mark the README with a gentle banner if content relied on heuristics:
  > _Note: Some details inferred due to limited XML docs. Consider adding summaries/remarks to source._

**Never leave a README with only a title and one small paragraph.**

## Canonicalization (paths & titles)
1) Split repo path → segments.  
2) Remove every `src` segment (case-insensitive).  
3) For each remaining segment, strip leading `RapidStreamer` (regex `^(rapidstreamer)([.\-_])?`, case-insensitive).  
4) Normalize: collapse duplicate separators/dots/hyphens/underscores; trim; drop empties.  
5) Collision safety: if two sources canonicalize to the same docs path, keep the first; suffix later (e.g., `-rs` or short hash). Record in audit.  
6) Destination: /docs/<canonical>/README.md

## Exclusions (strict)
- Skip paths whose any segment matches (case-insensitive): `test`, `tests`, `testing`, `.tests`, `*unit*test*`, `*integration*test*`.
- Skip: `.git`, `.github` (except `.github/prompts`), `.vs`, `.idea`, `node_modules`, `bin`, `obj`, caches/build artifacts.
- Do not traverse `/docs` during generation.

## Cross-Document Linking & Bookmarks (required)
- Every README has `## Contents` with anchor links to all major sections.
- Use relative links: `./Child/README.md`, `../Sibling/README.md#api-reference-summary`.
- Stable anchors: default Markdown slugs; for types use `### TypeName` → `#typename`.
- End long sections with `[↑ Back to top](#contents)`.

## Folder README — Required Sections
(omit only if truly N/A and write **_Not applicable_**)

1) **Title & TOC**  
2) **Overview** (2–5 sentences)  
3) **Files** (table; all files in this folder)  
   | File | Primary type(s) | LOC (approx) | Responsibility |
4) **Types & Members** (when any types exist in this folder)  
   - Types table:
     | Type | Kind | Summary | Inherits/Implements | Key Members |
   - Per-type details:
     - `### <TypeName>`
       - Kind, Namespace
       - Inherits/Implements
       - Attributes (serialization/validation)
       - Key Properties (name : type — one-liner, nullability)
       - Key Methods (signature — one-liner; notable params)
       - Events (if any)
       - Constructors/Factories (notable)
       - Thread-safety / immutability
       - Serialization notes
       - Validation notes
       - **Usage Recipe** (realistic, **no test examples**)
5) **Serialization & Contracts** (if applicable)  
6) **Validation & Constraints** (if applicable)  
7) **Performance Notes** (if applicable)  
8) **RapidStreamer Dependencies** (if used here)  
   - Table (**Package | Version | Description | Links**)  
   - Links: GitHub Packages feed, Repo URL (if any), internal anchors
9) **Benchmarks / Architecture / Diagrams** (if present)  
10) **Examples** (short; no tests)  
11) **See Also** (siblings/parent)

### Leaf README (no child folders)
- Title & TOC
- Overview
- Files (or “_None_”)
- API Reference (Summary) if any types exist
- Optional sections as applicable
- Back-to-top

## RapidStreamer Dependencies (GitHub Packages)
- Treat all `RapidStreamer*` packages as hosted at `https://nuget.pkg.github.com/KiarashMinoo/index.json`.
- Detect from `*.csproj`, `Directory.Packages.props`, `packages.lock.json`, optionally `dotnet list package`.
- Record: `Id`, resolved `Version`, `Description`, `Project URL`, `Repository URL` (if any), `License`, `Authors`.
- Per-folder README: add table + internal deep links to usage anchors.
- Root `/README.md`: roll-up table of unique RapidStreamer packages with deep links to folders’ `#rapidstreamer-dependencies`.

## Docs Landing + Root README
- Create `/docs/README.md` (landing): title, short intro, links to `./Application/README.md` & `./Infrastructure/README.md` (if present), plus other top-level areas.
- **Coverage Audit** appended (see below).
- Update root `/README.md`: Documentation → `docs/README.md` (relative); include NuGet sources (parse `nuget.config`, show add-source for GitHub feed if missing) and:
  ```bash
  dotnet restore
  dotnet build -c Release
