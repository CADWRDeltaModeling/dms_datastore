# CLI Guide

## Scope

These conventions apply to command-line tools across the DMS Python ecosystem.

Use **Click** for new CLI development. `argparse` is primarily a legacy interface and should generally be preserved when maintaining an existing tool unless conversion is part of the task.

Consistency with neighboring tools is usually more valuable than introducing a locally "better" convention that makes the suite inconsistent.

## Program structure

A command-line module should normally separate command-line concerns from the programmatic operation.

A useful three-layer structure is:

1. **CLI entry point** — parses and validates command-line arguments and performs CLI-specific setup.
2. **Work function** — performs the main operation using explicit Python arguments.
3. **Optional lower-level function** — performs the most reusable operation on Python objects rather than filenames or CLI concepts.

Typical structure:

```python
def work_function(input_file, output_file, start, end):
    ...


@click.command()
@click.help_option("-h", "--help")
...
def main(input_file, output_file, start, end):
    work_function(
        input_file=input_file,
        output_file=output_file,
        start=start,
        end=end,
    )


if __name__ == "__main__":
    main()
```

### Structural rules

- Standalone scripts should include an `if __name__ == "__main__":` block that normally does nothing except call the CLI entry point.
- Name the CLI entry point `main()` or, when useful for clarity or composition, `<tool>_cli()`.
- The CLI entry point should primarily gather concrete arguments, perform CLI-level validation/setup, and call the work function with named arguments.
- Pass explicit arguments to work functions. Eight or more explicit arguments are acceptable when this makes the programmatic API clearer.
- Do not pass Click contexts, argparse namespaces, or generic dictionaries into core work functions merely for convenience. They hide the API and make documentation, testing, and reuse harder.
- Passing a configuration object is appropriate when the configuration itself is genuinely part of the API.
- The central work function should normally have a name closely related to the module's purpose without necessarily duplicating the filename exactly. For example, `download_cdec.py` might expose `cdec_download()`.
- Important functions and helpers should use NumPy-style docstrings.

## Lower-level Python interfaces

When useful, provide a lower-level function that operates on Python objects rather than external representations.

For example, a CLI might accept `--station-list stations.txt`. The work function may read that file into a list of station identifiers, while a lower-level function performs the operation on a `list[str]`.

This makes functionality easier to compose from other Python programs.

Do not force callers through filenames or CLI parsing when the underlying operation naturally accepts Python objects.

## Testability

Think about testability when designing the interface, not after implementation.

The core operation should normally be testable without invoking the command line.

Separate command-line parsing, file opening and external representation, validation, and programmatic work where doing so produces a clearer interface.

Do not make inference from surrounding files the only way to supply important inputs. Inference may be a convenience, but explicit use must remain possible where practical.

## Click

Use Click for new CLI development.

Provide both long and short help options:

```python
@click.help_option("-h", "--help")
```

### Help behavior

`--help` and `-h` must:

- exit immediately;
- exit successfully;
- not require other arguments;
- ignore otherwise-missing required arguments;
- display help rather than fail argument validation.

Never use `-h` for anything except help.

## Legacy argparse

Do not convert a working argparse interface casually unless conversion is part of the task.

When maintaining an argparse tool, preserve interfaces relied upon by existing documentation and tooling.

Where the established documentation system expects a parser factory, name it `create_argparse()`.

New tools should normally use Click rather than introducing additional argparse interfaces.

## Option naming

Prefer long options in **kebab-case**:

```text
--dry-run
--output-dir
--station-list
```

Click maps these naturally to Python identifiers using underscores.

Before introducing a compound option name, consider whether one of its words is unnecessary.

Prefer `--crs` over `--pyproj-crs` unless the implementation distinction is genuinely meaningful to the user.

Avoid exposing implementation details in option names. An option name should describe the user's concept rather than the library currently used to implement it.

## Canonical option meanings

Use conventional option names and short aliases where they fit the command.

| Long option | Short alias | Expected meaning |
| --- | --- | --- |
| `--help` | `-h` | Show help and exit |
| `--version` | `-V` when useful | Print version and exit |
| `--verbose` | `-v` | Increase output |
| `--quiet` | `-q` | Suppress non-essential output |
| `--debug` | usually none or `-d` | Debug-level output |
| `--input FILE` | `-i FILE` | Input file |
| `--output FILE` | `-o FILE` | Output file |
| `--force` | `-f` | Overwrite or bypass confirmation |
| `--dry-run` | `-n` when unambiguous | Show intended actions without side effects |
| `--yes` | `-y` | Automatically confirm prompts |
| `--all` | `-a` | Include everything |
| `--recursive` | `-r` | Recurse through directories |
| `--start DATE` | `-s` when unambiguous | Start time |
| `--end DATE` | `-e` | End time |

Additional established names include:

- `--runstart DATE` when model run start must be distinguished from another start time;
- `--reftime DATE` for an elapsed-datetime reference when that concept is actually required;
- `--iter NUM` for a model iteration or time-step number;
- `--crs EPSG:26910` for a coordinate reference system;
- `--limit N` for a maximum number of items;
- `--sort FIELD` for a sort key;
- `--reverse` for reverse ordering.

Use ISO-style datetime inputs such as `2009-01-01` or `2009-01-01T00:00` unless an existing interface has a deliberate alternative.

Do not use `--sdate`.

## Short-option conflicts

Short options are scarce and sometimes ambiguous.

In particular:

- `-v` means verbose; do not use it for version.
- `-h` means help and must not be repurposed.
- `-n` may mean dry-run or limit/count in existing conventions.
- `-r` may mean recursive or reverse.

Do not assign a short alias merely for completeness. Omit it when the command's vocabulary would make the alias ambiguous.

Do not change the meaning of a short option between closely related subcommands without a compelling compatibility reason.

## Logging and verbosity

Configure logging at the CLI entry point rather than inside reusable work functions.

Use the package's established logging configuration machinery.

Common options include `--logdir`, `--debug`, `--verbose`, and `--quiet`.

`--quiet` and `--verbose` should normally be treated as conflicting modes.

Multiple `-v` flags may increase verbosity when that convention is deliberately supported.

Do not invent a separate logging framework for an individual command when shared machinery already exists.

## Input and output behavior

Use conventional input/output names when they accurately describe the interface:

```text
-i, --input
-o, --output
```

Users expect `--force` / `-f` to permit potentially destructive behavior such as overwrite or bypassing confirmation.

Users expect `--dry-run` to have **no side effects**.

Do not use these names for weaker or substantially different semantics.

## Packaging and registration

Once a standalone command is stable, expose it as an entry point in `pyproject.toml` where appropriate.

Adding a Python file alone does not make a command installed, discoverable, or part of a larger command namespace.

Toolchain-specific registration requirements belong in the relevant package or ecosystem guidance. For example, the BayDeltaSCHISM command hierarchy may require additional registration beyond the local `pyproject.toml`.

## Documentation

CLI work is not complete when the code runs.

When adding or materially changing a command:

- document important Python functions using NumPy-style docstrings;
- describe the steps performed by the work function when that helps users understand its behavior;
- update the user guide;
- expose generated CLI help in Sphinx where the documentation system supports it;
- provide a pointer to relevant examples;
- ensure related configuration or YAML examples point to the appropriate user documentation.

Examples should address realistic production work rather than merely demonstrate valid syntax.

Documentation generation details that are specific to a particular toolchain should remain in that toolchain's guidance rather than this common guide.

## Compatibility and consistency

Do not rename options casually.

Changing an established option can break scripts, documentation, automation, and user habits.

When normalizing an older interface:

1. determine who or what may depend on the existing spelling;
2. weigh consistency against compatibility;
3. use an alias when compatibility is more important than immediate cleanup;
4. avoid churn that produces little practical benefit.

Consistency across the tool suite is generally preferable to isolated interface improvement.
