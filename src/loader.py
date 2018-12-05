#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# This file is part of the dodoo-loader (R) project.
# Copyright (c) 2018 XOE Corp. SAS
# Authors: David Arnold, et al.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, see <http://www.gnu.org/licenses/>.
#

from __future__ import absolute_import, division, print_function, unicode_literals

import gc
import json
import logging
import os
from builtins import bytes, open, str

import click
import click_odoo
import networkx as nx
import numpy as np
import pandas as pd
from click_odoo import odoo
from future import standard_library
from future.utils import viewitems

# etc., as needed


standard_library.install_aliases()


# from utils import manifest, gitutils

_logger = logging.getLogger(__name__)


SUPPORTED_FORMATS = ["csv", "json"]
SUPPORTED_FORMATS_EXCEL = ["xlsx", "xls"]


def load(env, model, chunk):
    """ Loads a chunk into model.
    Public method. Can be scheduled into threads. Interface method. """
    res = env[model].load(
        [chunk.index.name] + chunk.columns.tolist(),  # fields
        chunk.fillna("").astype(str).reset_index().values.tolist(),  # data
    )

    # Make current return API more explicit
    if not res["ids"]:
        return "failure", res["ids"], res["messages"]
    return "success", res["ids"], res["messages"]


def log_load_json(state, ids, extids, msgs, batch, model):
    """ Logs load result into json chunk. Interface method. """
    return bytes(
        json.dumps(
            {
                "batch": batch,
                "candidates": extids,
                "loaded": ids,
                "model": model,
                "state": state,
                "x_msgs": msgs,
            },
            sort_keys=True,
            indent=4,
        )
        + ",",
        "utf-8",
    )


class DataSetGraph(nx.DiGraph):
    """ Holds DataFrames as nodes plus their metadata.
    Class-level functions (ordered) describe the processing stages."""

    def __init__(self, *args, **kwargs):
        self.env = kwargs.get("env", False)
        super(DataSetGraph, self).__init__(*args, **kwargs)

    def load_metadata(self):
        """ Loads all required metadata from the odoo enviornment
        for all nodes in the graph and normalizes column names"""
        for _node, data in self.nodes(data=True):
            # Normalize column names
            data["cols"] = []
            for col in data["df"].columns:
                fixed = odoo.models.fix_import_export_id_paths(col)
                subfield = fixed[1] if len(fixed) == 2 else ""
                data["cols"].append({"name": fixed[0], "subfield": subfield})

                if subfield and subfield not in ["id", ".id"]:
                    raise click.UsageError(
                        "*2many subfield notation is not supported by this "
                        "loader:\nThe semantics of this notation can be "
                        "indeterministic.",
                        ctx=click.get_current_context(),
                    )

            klass = self.env[data["model"]]

            data["fields"] = {"stored": [], "relational": []}
            # spec: {'relational': [{'name':'', 'model':''}]}
            data["parent"] = klass._parent_name  # pylint: disable=W0212
            data["repr"] = klass._description  # pylint: disable=W0212

            for _name, field in klass._fields.items():
                if field.store:
                    data["fields"]["stored"].append(field)
                if field.relational:
                    data["fields"]["relational"].append(
                        {"name": field.name, "model": field.comodel_name}
                    )

            # Enrich cols with data from odoo env (convenience)
            for col in data["cols"]:
                for rel in data["fields"]["relational"]:
                    if col["name"] == rel["name"]:
                        col["model"] = rel["model"]

    def seed_edges(self):
        """ Seeds the edges based on the df columns relations
        and existing models in the graph """
        for node_u, data in self.nodes(data=True):
            for col in [col for col in data["cols"] if col.get("model")]:
                for node_v, model in self.nodes(data="model"):
                    if col["model"] == model and col["name"] != data["parent"]:
                        self.add_edge(node_u, node_v, column=col["name"])

    def order_to_parent(self):
        """ Reorganizes dataframes for parent fields so they are in
        suitable loading order.
        TODO: Does not work with nested rows. Flatten everything first? """
        for _node, data in self.nodes(data=True):
            parent_col = [col for col in data["cols"] if col["name"] == data["parent"]]
            if not parent_col:
                continue
            parent_col = parent_col[0]
            idx = data["df"].index.name

            # We can only infer parent dependency if id and parent_id column
            # are in the same format (eg .id & /.id or id & /id)
            if parent_col["subfield"] != idx:
                continue

            parent = parent_col["name"] + "/" + parent_col["subfield"]
            record_graph = nx.DiGraph()

            record_graph.add_nodes_from(data["df"].index.tolist())
            record_graph.add_edges_from(
                viewitems(data["df"][parent][data["df"][parent].notnull()])
            )
            data["df"] = data["df"].reindex(
                nx.topological_sort(record_graph.reverse(True))
            )

    def chunk_dataframes(self, batch):
        """ Chunks dataframes as per provided batch size.
        Resulting DFs are stored back as []DataFrame on the node.

        Note:
            Don't attempt to schedule Hierarchy tables across threads: we
            deliberately refrain from implementing a federated data chunk
            dependency lock. This is usually not a problem, as hierarchy tables
            tend to be relatively small in size and simple in datastructure.
        """
        for _node, data in self.nodes(data=True):
            # https://stackoverflow.com/a/25703030
            # returns an iterable over (key, group)
            data["chunked_iterable"] = data["df"].groupby(
                np.arange(len(data["df"])) // batch
            )
            del data["df"]
            # force gc collection as allocated memory
            # chunks might non-negligable.
            gc.collect()

    def flush_all(self, log_stream=None):
        """ Synchronously flushes all DataSetGraph's chunks in topo-sorted
        order into their respective model. Writes return state as json into
        the log_buf reciever """
        for node in nx.topological_sort(self.reverse(False)):
            batchlen = len(self.nodes[node]["chunked_iterable"])
            for batch, df in self.nodes[node]["chunked_iterable"]:
                _logger.info(
                    "Synchronously loading %s (%s), batch %s/%s.",
                    self.nodes[node]["repr"],
                    self.nodes[node]["model"],
                    batch + 1,
                    batchlen,
                )
                state, ids, msgs = load(self.env, self.nodes[node]["model"], df)
                if log_stream:
                    log_stream.write(
                        log_load_json(
                            state,
                            ids,
                            df.index.tolist(),
                            msgs,
                            batch,
                            self.nodes[node]["model"],
                        )
                    )


def _infer_valid_model(filename):
    """ Returns a valid model name from filename or False
    Filenames are expected to convey the model just as Odoo
    does when loading csv files. """

    # if filename not in ENV:  # does raise in old odoo versions
    try:
        ENV[filename]  # noqa
        return filename
    except KeyError:
        return False


def _log_retrieve_loaded_indices(out, model):
    out.seek(0)
    data = json.loads(out.read().decode("utf-8"))[:-1]
    data[:] = (x for x in data if x["model"] == model and x["loaded"])
    return [c for batch in data for c in batch["candidates"]]


def _load_dataframes(buf, input_type, model, out):
    """ Loads dataframes into the GRAPH global receiver """
    # out = None

    def _load_into_graph(df, mod):
        # Drop lines with empty or NaN first column
        df = df[df.iloc[:, 0] != ""][  # Filter out empty strings
            ~df.iloc[:, 0].isnull()  # Filter out none-set values (eg. in json)
        ]
        if "id" in df.columns:
            idx = "id"
        if ".id" in df.columns:
            idx = ".id"
        if not idx:
            raise click.UsageError(
                "You need to provide an index column:" "\t'id' or '.id' are supported"
            )

        df.set_index(idx, inplace=True)
        if out and out.read(1):
            df = df[~df.index.isin(_log_retrieve_loaded_indices(out, mod))]
        GRAPH.add_node(id(df), model=model, df=df)

    # Special case: Excel file with sheets
    if input_type == "xls":
        xlf = pd.ExcelFile(buf)
        for name in xlf.sheet_names:
            model = _infer_valid_model(name)
            if not model:
                continue
            df = _read_excel(xlf, name)
            _load_into_graph(df, model)
        return

    if not model:
        return
    if input_type == "csv":
        df = _read_csv(buf)
    if input_type == "json":
        df = _read_json(buf)
    _load_into_graph(df, model)


def _read_csv(filepath_or_buffer):
    """ Reads a CSV file through pandas from a buffer.
    Returns a DataFrame. """
    return pd.read_csv(filepath_or_buffer)


def _read_json(filepath_or_buffer):
    """ Reads a JSON file through pandas from a buffer.
    Returns a DataFrame. """
    return pd.read_json(filepath_or_buffer)


def _read_excel(excelfile, sheetname):
    """ Reads a XLS/XLSX file through pandas from an ExcelFile object.
    Returns a DataFrame. """
    return pd.read_excel(excelfile, sheetname)


@click.command(
    cls=click_odoo.CommandWithOdooEnv,
    env_options={"with_rollback": False, "with_addons_path": True},
    default_overrides={"log_level": "warn"},
)
@click.option(
    "--file",
    "-f",
    type=click.File("rb", lazy=True),
    multiple=True,
    required=False,
    help="Path to the file, that you want to load. "
    "You can specify this option multiple times "
    "for more than one file to load.",
)
@click.option(
    "--stream",
    "-s",
    nargs=3,
    multiple=True,
    required=False,
    help="[stream type model] Stream, you want to load. "
    "`type` can be csv or json. "
    "`model` can be any odoo model availabe in env. "
    "You can specify this option multiple times "
    "for more than one stream to load.",
)
@click.option(
    "--onchange/--no-onchange",
    default=True,
    show_default=True,
    help="[TBD] Trigger onchange methods as if data was entered "
    "through normal form views.",
)
@click.option(
    "--batch",
    default=50,
    show_default=True,
    help="The batch size. Records are cut-off for iteration " "after so many records.",
)
@click.option(
    "--out",
    type=click.File("r+b", lazy=True),
    default="./log.json",
    show_default=True,
    help="Log success into a json file.",
)
def main(env, file, stream, onchange, batch, out):
    """ Loads data into an Odoo Database.

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
    ending in /.id (db ID) or /id (ext ID). Either one must match the principal
    id or .id column (to which it refers).

    Note: For UX and security reasons, nested semantics (as in Odoo) are not
    supported as they usually are undeterministic (lack of identifier on the
    nested levels). That's too dangerous for ETL.
    """

    global ENV  # pylint: disable=W0601
    global GRAPH  # pylint: disable=W0601
    ENV = env
    # Non-private Class API, therfore pass env as arg
    GRAPH = DataSetGraph(env=env)

    # Check either file or stream input is set.
    if not file and not stream:
        raise click.BadParameter(
            "No stream or file input defined. "
            "Define either a --file and/or a --stream input.",
            ctx=click.get_current_context(),
        )

    for f in file:
        if not hasattr(f, "name"):
            raise click.BadParameter(
                "{} doesn't seem to be a file.".format(f),
                ctx=click.get_current_context(),
            )
        name = os.path.basename(f.name).lower()
        name = os.path.splitext(name)[0]
        type_ = os.path.splitext(f.name)[-1].lower().lstrip(".")
        if type_ not in SUPPORTED_FORMATS + SUPPORTED_FORMATS_EXCEL:
            formats = ", ".join(SUPPORTED_FORMATS + SUPPORTED_FORMATS_EXCEL)
            raise click.BadParameter(
                "Supported formats: {formats}.\n"
                "Found {type_}".format(formats=formats, type_=type_),
                ctx=click.get_current_context(),
                param_hint=f.name,
            )
        if type_ == "xlsx":
            type_ = "xls"

        excel = type_ == "xls"
        model = _infer_valid_model(name)

        if not excel and not model:
            raise click.BadParameter(
                "Filename is no valid odoo model. For non-excel files, "
                "the filename (before the extension) must encode the model.",
                ctx=click.get_current_context(),
                param_hint=name,
            )
        _load_dataframes(f, type_, model, out)

    for (s, type_, model) in stream:
        type_, model = type_.lower(), _infer_valid_model(model.lower())
        if hasattr(s, "name"):
            raise click.BadParameter(
                "{s} doesn't seem to be a stream.".format(locals()),
                ctx=click.get_current_context(),
            )
        if type_ not in SUPPORTED_FORMATS:
            formats = ", ".join(SUPPORTED_FORMATS)
            raise click.BadParameter(
                "Supported formats for type argument: {formats}.\n"
                "Found {type_}".format(formats=formats, type_=type_),
                ctx=click.get_current_context(),
            )

        if not model:
            raise click.BadParameter(
                "Model argument is no valid odoo model.",
                ctx=click.get_current_context(),
                param_hint=model,
            )
        with open(s, "rb") as stream:
            _load_dataframes(stream, type_, model, out)

    GRAPH.load_metadata()
    GRAPH.seed_edges()
    GRAPH.order_to_parent()
    GRAPH.chunk_dataframes(batch)

    out.seek(0)
    if not out.read(1):
        out.write(bytes("[", "utf-8"))  # Hack to produce valid json
    else:
        out.seek(-3, 2)
    GRAPH.flush_all(out)  # Sychronous loading
    out.write(bytes("{}]", "utf-8"))  # Hack to produce valid json


if __name__ == "__main__":  # pragma: no cover
    main()
