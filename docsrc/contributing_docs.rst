Contributing To The Docs
========================

Edit the reStructuredText (``.rst``) sources in ``docsrc/``. Run the commands
below from the repository root in a Conda-initialized terminal.

Environment Setup Using Conda
-----------------------------

Use ``environment.yml`` for the package's runtime dependencies and
``environment-doc.yml`` for documentation tools. Create a separate documentation
environment so your normal working environment remains unchanged.

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
   sphinx-build -b html docsrc docs/html

Open ``docs/html/index.html`` in your browser. Check the build output for warnings
and import errors, and inspect the pages you changed. Add new pages to the
``toctree`` in ``docsrc/index.rst`` so they appear in the table of contents.

Submit changes to the documentation sources through a pull request. Generated
HTML is built and published by the documentation workflow.