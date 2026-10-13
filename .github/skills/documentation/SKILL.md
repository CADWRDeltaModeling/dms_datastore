---
name: documentation
description: "Use when contributing to, building, or troubleshooting dms_datastore Sphinx documentation: environment setup using Conda, combining environment.yml and environment-doc.yml with recent or older Conda, documentation-support dependencies, HTML builds, and missing autodoc or Click CLI output."
---

# Documentation

Documentation sources live in `docsrc/`; HTML output goes to `docs/html/`.
Run the commands below from the repository root in a Conda-initialized terminal.
Let the user create or remove environments unless they explicitly request execution.

## Environment setup using Conda

Use both environment files:

- `environment.yml` provides the runtime dependencies and Python version needed to import the package during autodocumentation.
- `environment-doc.yml` adds documentation-support tools: Sphinx and extensions, notebook tools, Pandoc, Node.js, and publishing utilities. It is an overlay, not a standalone runtime environment.

The overlay covers this project's and BayDeltaSCHISM's documentation-support collections. Do not add downstream runtime packages such as `schimpy` or SCHISM-specific dependencies to it.
Normal environment setup is unchanged unless the overlay is explicitly included.
Use a separate `dms_datastore_doc` environment; do not augment `base` with project dependencies.

### Recent Conda: combine the files

Check the actual Conda executable version:

```shell
conda --version
```

`conda create` gained YAML input support in Conda 26.3.0. With a recent Conda that supports `--format`, create the combined environment in one command:

```shell
conda create -n dms_datastore_doc --format cep-24 --file environment.yml --file environment-doc.yml
```

Specify the format explicitly because the custom overlay filename may not be autodetected.
For Conda 26.3 releases that offer `--env-spec` rather than `--format`, use `--env-spec cep-24` instead. Newer releases deprecate that spelling; prefer `--format` when available.

Conda normally lives in `base`. Updating Python in another environment does not update Conda. Conda's own Python requirement is distinct from the Python version selected by `environment.yml`.
Do not require a Conda upgrade just to build docs; the two-stage path below works with older Conda.

### Older Conda: create, then augment

```shell
conda env create -n dms_datastore_doc -f environment.yml
conda env update -n dms_datastore_doc -f environment-doc.yml
```

Never add `--prune` to the overlay update: the overlay does not describe the full environment, so pruning can remove needed runtime dependencies.
For an existing compatible docs environment, only the update step is needed. Complete Conda installation steps before installing the editable package with pip.

### Install the checkout and build

After either setup path:

```shell
conda activate dms_datastore_doc
python -m pip install --no-deps -e .
sphinx-build -b html docsrc docs/html
```

Use `--no-deps` for the editable install because the two environment files supply the dependencies. Adding `[doc]` to a pip install with `--no-deps` does not install the extra's dependencies.
For a fresh validation build, use `sphinx-build -E -a -b html docsrc docs/html`. Check the build output for import errors and missing CLI sections, not just whether HTML files were produced.

### Optional standalone Mermaid rendering

The overlay includes Node.js but does not install Mermaid CLI globally. To obtain and invoke it when needed:

```shell
npm exec --package=@mermaid-js/mermaid-cli -- mmdc --help
```

This may download the CLI and browser dependencies on first use. It is separate from Sphinx's Mermaid extension. Do not disable TLS verification or change the global npm registry as part of docs setup.

## Troubleshooting

- `could not parse 'name: ...'`: Conda is treating YAML as a plain package list. Use the older-Conda two-stage path or a recent Conda with explicit YAML format selection.
- `EnvironmentSpecPluginNotDetected`: select `--format cep-24` and check YAML contents. All entries directly under `dependencies` must have the same indentation; extra indentation can concatenate package names into invalid specifications.
- A downloader's CLI section is absent: `sphinx-click` must import its module. Check the import exception in the docs environment. Install the required runtime dependency through the base environment definition rather than changing a correct RST directive or mocking Click.
- An environment name is already in use: ask whether to update it, choose another name, or remove it. Do not assume it is disposable. If the user explicitly chooses removal, deactivate it first and run `conda env remove -n dms_datastore_doc`; this deletes the environment, not the checkout.
- In Windows CMD, quote constraints such as `"conda>=26.3"`; an unquoted `>` redirects output. If an upgrade is requested, review the solve before confirming and do not silently change channels or pins.

## Dependency maintenance

Compare `environment-doc.yml` with the `doc` extra in `pyproject.toml`, `docsrc/requirements.txt`, and `.github/workflows/build_sphinx.yml` when documentation dependencies change. CI currently has its own installation path; editing the overlay alone does not change CI.
Preserve the distinction between the Pandoc executable provided by Conda and the Python `pandoc` wrapper installed through the overlay's pip section.