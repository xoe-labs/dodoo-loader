click-odoo-loader
==================

.. image:: https://img.shields.io/badge/license-LGPL--3-blue.svg
   :target: http://www.gnu.org/licenses/lgpl-3.0-standalone.html
   :alt: License: LGPL-3
.. image:: https://badge.fury.io/py/click-odoo-.svg
    :target: http://badge.fury.io/py/click-odoo-

``click-odoo-loader`` is a set of useful Odoo maintenance functions.
They are available as CLI scripts (based on click-odoo_), as well
as composable python functions.

.. contents::

Script [EXAMPLE - Put output of `--help` here]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code:: bash

    Usage: loader.py [OPTIONS]

      Loads data into an Odoo Database.

      Supply data by file or stream in a supported format and load it into a
      local or remote Odoo database.

      Highlights:

      • Detects model-level dependency on related fields and record-level
      dependencies in tree-like tables (hierarchies). Cares to load everything
      in the correct order*.

      • Supported formats: JSON, CSV, XLS & XLSX

      • Logs success to --out. Next runs deduplicate based on those logs.

      • [TBD] Can trigger onchange as if data was entered through forms.

      Note: record-level dependency detection only works with parent columns
      ending in /.id (db ID) or /id (ext ID). Either one must match the
      principal id or .id column (to which it refers).

      Note: For UX and security reasons, nested semantics (as in Odoo) are not
      supported as they usually are undeterministic (lack of identifier on the
      nested levels). That's too dangerous for ETL.

    Options:
      -c, --config FILE           Specify the Odoo configuration file. Other ways
                                  to provide it are with the ODOO_RC or
                                  OPENERP_SERVER environment variables, or
                                  ~/.odoorc (Odoo >= 10) or ~/.openerp_serverrc.
      -d, --database TEXT         Specify the database name. If present, this
                                  parameter takes precedence over the database
                                  provided in the Odoo configuration file.
      --log-level TEXT            Specify the logging level. Accepted values
                                  depend on the Odoo version, and include debug,
                                  info, warn, error.  [default: warn]
      --logfile FILE              Specify the log file.
      -f, --file FILENAME         Path to the file, that you want to load. You can
                                  specify this option multiple times for more than
                                  one file to load.
      -s, --stream TEXT...        [stream type model] Stream, you want to load.
                                  `type` can be csv or json. `model` can be any
                                  odoo model availabe in env. You can specify this
                                  option multiple times for more than one stream
                                  to load.
      --onchange / --no-onchange  [TBD] Trigger onchange methods as if data was
                                  entered through normal form views.  [default:
                                  True]
      --batch INTEGER             The batch size. Records are cut-off for
                                  iteration after so many records.  [default: 50]
      --out FILENAME              Log success into a json file.  [default:
                                  ./log.json]
      --help                      Show this message and exit.


Useful links
~~~~~~~~~~~~

- pypi page: https://pypi.org/project/click-odoo-
- code repository: https://github.com//click-odoo-
- report issues at: https://github.com//click-odoo-/issues

.. _click-odoo: https://pypi.python.org/pypi/click-odoo

Credits
~~~~~~~

Contributors:

- David Arnold (XOE_)

.. _XOE: https://xoe.solutions

Maintainer
~~~~~~~~~~

.. image:: https://erp.xoe.solutions/logo.png
   :alt: XOE Corps. SAS
   :target: https://xoe.solutions
