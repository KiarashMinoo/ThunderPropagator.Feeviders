---
mode: agent
description: "Docs generator with optional inputs. Deep recursion under /docs (no src/source/custom-root segments in paths). Per-folder README; parents link to children; exclude tests; no Source Location paths; RapidStreamer on GitHub Packages. IMPORTANT: roots= is DROP-ONLY for path canonicalization (NEVER limits traversal). Inputs optional: mode=full|incremental, modules=<comma paths>, roots=<comma names>, dryRun=true|false."
---

# Repo Docs Generator
*(roots are **drop-only**; full-tree traversal; deep recursion; incremental supported)*

> ### Inputs (all optional)
> - `mode`: `${input:mode:full}` — `full` (rebuild all; wipes /docs) or `incremental` (only selected modules; no global wipe)
> - `modules`: `${input:modules:}` — comma/space-separated repo-relative folders to update (used only when `mode=incremental`)
> - `roots`: `${input:roots:}` — comma/space-separated **segment names** to drop from docs paths (e.g., `src, source, app, packages`). **Drop-only; never limits traversal.**
> - `dryRun`: `${input:dryRun:false}` — `true` = plan/report only (no file writes)

---

## High-level Behavior
- If `mode=full` **or** inputs omitted → **full rebuild**:
  - Wipe `/docs`, traverse **entire repo**, generate docs.
- If `mode=incremental` **and** `modules` provided → **partial update**:
  - Do **not** wipe `/docs`.
  - Regenerate docs only for those **modules and all their descendants**; refresh affected links/roll-ups.
- If `mode=incremental` but `modules` empty → treat as **full**.

---

## Traversal Scope (critical)
- **Always traverse the entire repository directory tree** (filesystem enumeration; no depth limit), except:
  - Skip `/docs` during generation (it’s being rebuilt/updated).
  - Skip standard exclusions (tests, system/build dirs; see below).
- **NEVER** use `roots` to limit traversal. `roots` are **only** used to **drop those names from docs paths** during canonicalization.

---

## Exclusions (strict)
- **Exclude tests entirely** (no docs, no merges) if **any segment** (case-insensitive) matches:
  `test`, `tests`, `testing`, `.tests`, `*unit*test*`, `*integration*test*`.
- Skip system/build dirs: `.git`, `.github` (keep `.github/prompts`), `.vs`, `.idea`, `node_modules`, `bin`, `obj`, caches/artifacts.
- Do not traverse `/docs` while generating.

---

## Canonicalization (paths & titles)
When mapping any repository folder `<F>` to a docs folder:

1) Split `<F>` into segments.  
2) Build a **drop-set** (lowercased):
   - Always includes: `src`, `source`.
   - Plus any names from `${input:roots}` (if provided).  
   *Example:* `roots=app,packages` → also drop `app`, `packages`.
3) **Drop any segment** whose lowercased value is in the drop-set.  
4) For each remaining segment `S`, strip a **leading `RapidStreamer`** token with optional separator (`.`, `-`, `_`):
   - Regex (case-insensitive): `^(rapidstreamer)([.\-_])?` → `""`
   - **Do not split on `.`** beyond removing `RapidStreamer.`.
5) Normalize: collapse duplicate separators/dots/hyphens/underscores; trim; drop empties.  
6) **Collision safety**: if two sources canonicalize to the same docs path, keep the first; suffix later (e.g., `-rs` or short hash). Record in Diagnostics.  
7) Destination README path: /docs/<canonical>/README.md

---

## Core Guarantees
- **Deep recursion**: create a `README.md` for **every** non-excluded directory at **any depth**.
- **Per-folder README only**; parents **link to direct children** (no child content merged into parent).
- **Only `README.md` files** are produced/updated (plus `/docs/README.md` landing and root `/README.md` update).
- **No “Source Location”/filesystem paths** in docs — use **relative links + anchors** only.
- **RapidStreamer packages** are documented from **GitHub Packages** feed:  
  `https://nuget.pkg.github.com/KiarashMinoo/index.json`.

---

## Empty-Prevention & Auto-Retry (per folder)
Prevent blank/skeletal docs in one run:

- **Pass 1 (public)**: parse `*.cs` in this folder for **public** types/members, XML docs, serialization/validation attributes; generate standard sections.
- If sparse → **Pass 2 (internal)**: include **internal** types/members; add **Files** table (all files; inferred responsibilities).
- If still sparse → **Pass 3 (heuristics)**: infer from names/patterns; produce Overview, Files, minimal API Summary, and at least one realistic **Usage Recipe**.
- If truly no `.cs` and no children: create a Leaf README with “Files: _None_” and a TODO note.
- Never leave a README with only a title and a tiny paragraph.

---

## Folder README — Required Sections
*(omit only if truly N/A and state **_Not applicable_**; never include code paths)*

1) **Title & TOC**  
   - `# <Canonical Name>`  
   - `## Contents` — anchors for all sections
2) **Overview** (2–5 sentences: role in architecture, when to use; non-goals if relevant)
3) **Files** (table; all files in this folder; ignore tests)
   | File | Primary type(s) | LOC (approx) | Responsibility |
4) **Types & Members** (if any types here)
   - Types table:
     | Type | Kind | Summary | Inherits/Implements | Key Members |
   - Per-type details:
     - `### <TypeName>`
       - Kind, Namespace
       - Inherits / Implements
       - Attributes (serialization/validation)
       - Key Properties (name : type — one-liner, nullability)
       - Key Methods (signature — one-liner; notable params)
       - Events (if any)
       - Constructors / Factories
       - Thread-safety / immutability
       - Serialization notes
       - Validation notes
       - **Usage Recipe** (realistic snippet; **no test examples**)
5) **Serialization & Contracts** (if applicable)
6) **Validation & Constraints** (if applicable)
7) **Performance Notes** (if applicable)
8) **RapidStreamer Dependencies** (if used here)
   - Table: **Package | Version | Description | Links**  
   - Links: **GitHub Packages feed**, Repository URL (if any), **internal anchors** to usage
9) **Benchmarks / Architecture / Diagrams** (if present)
10) **Examples** (short; no tests)
11) **See Also** (siblings/parent; **relative links + anchors**)

---

## Cross-Document Links & Anchors
- Parents link to **direct children** via `./Child/README.md` and anchors as needed.
- Use stable anchors (Markdown slugs; for types: `### TypeName` → `#typename`).
- Add `[↑ Back to top](#contents)` on long sections.

---

## Docs Landing + Root README
- Create `/docs/README.md` (landing):
  - `# Documentation`, a short intro
  - Prominent links to `./Application/README.md` and `./Infrastructure/README.md` if they exist
  - Other top-level areas (tools/components/etc.)
  - **Diagnostics** section (below)
- Update root `/README.md`:
  - Documentation → link to `docs/README.md`
  - NuGet sources (parse `nuget.config`; if GitHub feed missing, show):
    ```bash
    dotnet nuget add source "https://nuget.pkg.github.com/KiarashMinoo/index.json" --name "github-KiarashMinoo"
    ```
  - Build:
    ```bash
    dotnet restore
    dotnet build -c Release
    ```

---

## RapidStreamer Dependencies (GitHub Packages)
- Detect from non-excluded projects: `*.csproj`, `Directory.Packages.props`, `packages.lock.json`, optionally `dotnet list package`.
- Record: `Id`, resolved `Version`, `Description`, `Project URL`, `Repository URL` (if any), `License`, `Authors`.
- Per-folder README: add table + internal deep links to usage anchors.
- Root `/README.md`: roll-up of unique RapidStreamer packages with deep links to each folder’s `#rapidstreamer-dependencies`.

---

## Diagnostics & Sanity Checks
Append to `/docs/README.md`:

### Diagnostics
- **Inputs** used: `mode`, `modules`, `roots`, `dryRun`
- **Drop-set** (lowercased): union of `['src','source']` + parsed `roots`
- **Discovered directories (count)** (non-excluded)
- **Generated READMEs (count)** (excluding `/docs/README.md`)
- **Collisions resolved**: list of original → final canonicalized
- **Auto-retry notes** (per-folder)

**Sanity fallback**
- If **Discovered ≥ 10** and **Generated ≤ 1**, automatically:
  - Re-run generation **ignoring custom `roots`** (drop only `src`/`source`),
  - Append a note:  
    *“Sanity fallback triggered: roots were treated drop-only but yielded too few outputs; a second pass ignoring custom roots produced N READMEs.”*

---

## Process (by mode)

**Full mode (or inputs omitted):**
1) If `${input:dryRun}` == `true`: simulate traversal & generation; print planned changes + Diagnostics; **do not write**.
2) Else:
   - Wipe `/docs`.
   - Traverse entire repo (skip exclusions); discover all directories.
   - Canonicalize using drop-set (roots drop-only).
   - Generate per-folder READMEs with Empty-Prevention (3 passes).
   - Create landing + update root README.
   - Write Diagnostics & Sanity fallback block (and re-run if triggered).

**Incremental mode:**
1) If `${input:modules}` empty → treat as **full**.
2) If `${input:dryRun}` == `true`: simulate changes limited to those modules; output Diagnostics for touched area.
3) Else:
   - Traverse entire repo, but **generate/refresh** only for the specified modules and all their descendants (**no global wipe**).
   - Update affected links/roll-ups and landing page.
   - Diagnostics limited to touched area.

---

## Safety
- Never delete outside `/docs/**` (except removing code-side READMEs that were merged **inside targeted modules**).
- Validate internal links you modify; fix broken anchors you introduce.

