Contributing To The Docs
========================

Edit the reStructuredText (``.rst``) sources in ``docsrc/``. Run the commands
below from the repository root in a Conda-initialized terminal.

Environment Setup Using Conda
-----------------------------

Use ``environment.yml`` for the package's runtime dependencies and
``environment-doc.yml`` for documentation tools. Create a separate documentation
environment so your normal working environment remains unchanged.

The convenience helper creates the environment, adds the documentation tools and
installs the checkout editable. On Windows PowerShell:

.. code-block:: console

   .\build_doc_env.bat

On Linux/macOS, with Conda available in the terminal:

.. code-block:: console

   bash build_doc_env.sh

The helper does not delete or replace an existing ``dms_datastore_doc``
environment, does not build the HTML, and does not leave your calling terminal
activated. Use ``--help`` to see what it does. The explicit steps follow.

With recent Conda supporting ``--format``, combine both files:

.. code-block:: console

   conda create -n dms_datastore_doc --format cep-24 --file environment.yml --file environment-doc.yml

For older Conda, create the environment first, then add the documentation tools:

.. code-block:: console

   conda env create -n dms_datastore_doc -f environment.yml
   conda env update -n dms_datastore_doc -f environment-doc.yml

Do not add ``--prune`` to the overlay update: it does not describe the full
environment and pruning can remove required runtime dependencies.

Build And Preview
-----------------

After either setup path, activate the environment, install the checkout, and
build the HTML documentation:

.. code-block:: console

   conda activate dms_datastore_doc
   python -m pip install --no-deps -e .
   cd docsrc
   make html

On Windows PowerShell, use ``.\make.bat html``. For a clean rebuild, run
``make clean`` followed by ``make html``.

Open ``docs/html/index.html`` in your browser. Check the build output for warnings
and import errors, and inspect the pages you changed. Add new pages to the
``toctree`` in ``docsrc/index.rst`` so they appear in the table of contents.

Submit changes to the documentation sources through a pull request. Generated
HTML is not committed; it is built and published by the documentation workflow.

GitHub Pages
------------

The **Sphinx: Render docs** workflow uses the same ``environment.yml`` plus
``environment-doc.yml`` environment and ``make html``, then publishes
``docs/html`` through the GitHub Pages actions rather than a branch. In
repository **Settings -> Pages**, the source must be **GitHub Actions**, and the
``github-pages`` environment must allow the publishing branch or tag. The build
and deploy jobs are separate, so check both in the **Actions** tab: a successful
build does not establish that deployment succeeded.