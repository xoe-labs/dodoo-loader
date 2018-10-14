#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# This file is part of the click-odoo-loader (R) project.
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

import logging

import gc
import os
import json
import pandas as pd
import numpy as np
import networkx as nx

import click
import click_odoo

from click_odoo import odoo

# from utils import manifest, gitutils

_logger = logging.getLogger(__name__)


SUPPORTED_FORMATS = ['csv', 'json']
SUPPORTED_FORMATS_EXCEL = ['xlsx', 'xls']


def load(env, model, chunk):
    """ Loads a chunk into model.
    Public method. Can be scheduled into threads. Interface method. """
    res = env[model].load(
        [chunk.index.name] + chunk.columns.tolist(),  # fields
        chunk.fillna('').astype(str).reset_index().values.tolist()  # data
    )

    # Make current return API more explicit
    if not res['ids']:
        return 'failure', res['ids'], res['messages']
    return 'success', res['ids'], res['messages']


def log_load_json(state, ids, extids, msgs, batch, model):
    """ Logs load result into json chunk. Interface method. """
    return bytes(json.dumps({
        'batch': batch,
        'candidates': extids,
        'loaded': ids,
        'model': model,
        'state': state,
        'x_msgs': msgs
        }, sort_keys=True, indent=4), 'utf-8')


class DataSetGraph(nx.DiGraph):
    """ Holds DataFrames as nodes plus their metadata.
    Class-level functions (ordered) describe the processing stages."""

    def __init__(self, *args, **kwargs):
        self.env = kwargs.get('env', False)
        super(DataSetGraph, self).__init__(*args, **kwargs)

    def load_metadata(self):
        """ Loads all required metadata from the odoo enviornment
        for all nodes in the graph and normalizes column names"""
        for node, data in self.nodes(data=True):
            # Normalize column names
            data['cols'] = []
            for col in data['df'].columns:
                fixed = odoo.models.fix_import_export_id_paths(col)
                data['cols'].append({'name': fixed[0],
                    'subfield': fixed[1] if len(fixed) == 2 else ''})

            klass = self.env[data['model']]

            data['fields'] = {'stored': [], 'relational': []}
            # spec: {'relational': [{'name':'', 'model':''}]}
            data['parent'] = klass._parent_name  # pylint: disable=W0212
            data['repr'] = klass._description  # pylint: disable=W0212

            for name, field in klass._fields.items():
                if field.store:
                    data['fields']['stored'].append(field)
                if field.relational:
                    data['fields']['relational'].append(
                        {'name': field.name, 'model': field.comodel_name})

            # Enrich cols with data from odoo env (convenience)
            for col in data['cols']:
                for rel in data['fields']['relational']:
                    if col['name'] == rel['name']:
                        col['model'] = rel['model']

    def seed_edges(self):
        """ Seeds the edges based on the df columns relations
        and existing models in the graph """
        for node_u, data in self.nodes(data=True):
            for col in [col for col in data['cols'] if col.get('model')]:
                for node_v, model in self.nodes(data='model'):
                    if col['model'] == model and col['name'] != data['parent']:
                        self.add_edge(node_u, node_v, column=col['name'])

    def order_to_parent(self):
        """ Reorganizes dataframes for parent fields so they are in
        suitable loading order.
        TODO: Does not work with nested rows. Flatten everything first? """
        for node, data in self.nodes(data=True):
            parent_col = [col for col in data['cols']
                if col['name'] == data['parent']]
            if not parent_col:
                continue
            parent_col = parent_col[0]
            idx = data['df'].index.name

            # We can only infer parent dependency if id and parent_id column
            # are in the same format (eg .id & /.id or id & /id)
            if parent_col['subfield'] != idx:
                continue

            parent = parent_col['name'] + '/' + parent_col['subfield']
            record_graph = nx.DiGraph()

            record_graph.add_nodes_from(data['df'].index.tolist())
            record_graph.add_edges_from(
                data['df'][parent][data['df'][parent].notnull()].iteritems()
            )
            data['df'] = data['df'].reindex(
                nx.topological_sort(record_graph.reverse(True)))

    def chunk_dataframes(self, batch):
        """ Chunks dataframes as per provided batch size.
        Resulting DFs are stored back as []DataFrame on the node.

        Note:
            Don't attempt to schedule Hierarchy tables across threads: we
            deliberately refrain from implementing a federated data chunk
            dependency lock. This is usually not a problem, as hierarchy tables
            tend to be relatively small in size and simple in datastructure.
        """
        for node, data in self.nodes(data=True):
            # https://stackoverflow.com/a/25703030
            # returns an iterable over (key, group)
            data['chunked_iterable'] = data['df'].groupby(
                np.arange(len(data['df']))//batch)
            del data['df']
            # force gc collection as allocated memory
            # chunks might non-negligable.
            gc.collect()

    def flush_all(self, log_stream=None):
        """ Synchronously flushes all DataSetGraph's chunks in topo-sorted
        order into their respective model. Writes return state as json into
        the log_buf reciever """
        for node in nx.topological_sort(self.reverse(False)):
            batchlen = len(self.nodes[node]['chunked_iterable'])
            for batch, df in self.nodes[node]['chunked_iterable']:
                _logger.info("Synchronously loading %s (%s), batch %s/%s.",
                             self.nodes[node]['repr'],
                             self.nodes[node]['model'], batch, batchlen)
                state, ids, msgs = load(
                    self.env, self.nodes[node]['model'], df)
                if log_stream:
                    log_stream.write(
                        log_load_json(state, ids, df.index.tolist(), msgs, batch,
                            self.nodes[node]['model']))


def _infer_valid_model(filename):
    """ Returns a valid model name from filename or False
    Filenames are expected to convey the model just as Odoo
    does when loading csv files. """

    if filename not in ENV:
        return False
    return filename


def _load_dataframes(buf, input_type, model):
    """ Loads dataframes into the GRAPH global receiver """

    # Special case: Excel file with sheets
    if input_type == 'xls':
        xlf = pd.ExcelFile(buf)
        for name in xlf.sheet_names:
            model = _infer_valid_model(name)
            if not model:
                continue
            df = _read_excel(xlf, name)
            df.set_index(df.columns[0], inplace=True)
            GRAPH.add_node(id(df), model=model, df=df)
        return

    if not model:
        return
    if input_type == 'csv':
        df = _read_csv(buf)
        df.set_index(df.columns[0], inplace=True)
    if input_type == 'json':
        df = _read_json(buf)
        df.set_index(df.columns[0], inplace=True)
    GRAPH.add_node(id(df), model=model, df=df)


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


@click.command()
@click_odoo.env_options(default_log_level='warn', with_rollback=False)
@click.option('--file', '-f', type=click.File('rb', lazy=True),
              multiple=True, required=False,
              help="Path to the file, that you want to load.\n"
                   "You can specify this option multiple times "
                   "for more than one file to load.")
@click.option('--stream', '-s', nargs=3,
              multiple=True, required=False,
              help="Stream, that you want to load.\n"
                   "\tFormat: -s stream type model\n"
                   "\t\t`type` can be csv or json.\n"
                   "\t\t`model` can be any odoo model availabe in env.\n"
                   "You can specify this option multiple times "
                   "for more than one stream to load.")
@click.option('--onchange/--no-onchange', default=True, show_default=True,
              help="Trigger onchange methods as if data was entered "
                   "through normal form views.")
@click.option('--batch', default=50, show_default=True,
              help="The batch size. Records are cut-off for iteration "
                   "after so many records. Nested lines do not count "
                   "towards that value. In *very* complex loading "
                   "scenarios: take some care with nested records.")
@click.option('--out', type=click.File('wb', lazy=True),
              default="./log.json", show_default=True,
              help="Persist the server's output into a JSON database "
                   "alongside each source file. On subsequent runs, "
                   "sucessfull loads are deduplicated.")
def main(env, file, stream, onchange, batch, out):
    """ Load data into an Odoo Database.

    Loads data supplied in a supported format by file or stream
    into a local or remote Odoo database.

    Highlights:

    - Detects model-level graph dependency on related fields and
      record level graph dependencies in tree-like tables (hierarchies)
      and loads everything in the correct order*.

    - Supported import formats are governed by the excellent pandas library.
      Most useful: JSON, CSV, XLS & XLSX

    - Through `output` persistence flag: can be run idempotently.

    - Can trigger onchange as if data was entered through forms.

    * Only */.id or /id are supported for parent fields. This is due to the
      fact that we cannot pipe through name_seach previous of the creation of
      the relevant reocrds. Furthermore, it must be the same format as the id
      column. Either 'id' and '*/id' or '.id' and '*/.id'.

    Writes sucess status of batches in JSON format into --out.
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
            ctx=click.get_current_context())

    for f in file:
        if not hasattr(f, 'name'):
            raise click.BadParameter(
                "{} doesn't seem to be a file.".format(f),
                ctx=click.get_current_context())
        name = os.path.basename(f.name).lower()
        name = os.path.splitext(name)[0]
        type_ = os.path.splitext(f.name)[-1].lower().lstrip('.')
        if type_ not in SUPPORTED_FORMATS + SUPPORTED_FORMATS_EXCEL:
            formats = ', '.join(SUPPORTED_FORMATS + SUPPORTED_FORMATS_EXCEL)
            raise click.BadParameter(
                "Supported formats: {formats}.\n"
                "Found {type_}".format(formats=formats, type_=type_),
                ctx=click.get_current_context(), param_hint=f.name)
        if type_ == 'xlsx':
            type_ = 'xls'

        excel = type_ == 'xls'
        model = _infer_valid_model(name)

        if not excel and not model:
            raise click.BadParameter(
                "Filename is no valid odoo model. For non-excel files, "
                "the filename (before the extension) must encode the model.",
                ctx=click.get_current_context(), param_hint=name)
        _load_dataframes(f, type_, model)

    for (s, type_, model) in stream:
        type_, model = type_.lower(), _infer_valid_model(model.lower())
        if hasattr(s, 'name'):
            raise click.BadParameter(
                "{s} doesn't seem to be a stream.".format(locals()),
                ctx=click.get_current_context())
        if type_ not in SUPPORTED_FORMATS:
            formats = ', '.join(SUPPORTED_FORMATS)
            raise click.BadParameter(
                "Supported formats for type argument: {formats}.\n"
                "Found {type_}".format(formats=formats, type_=type_),
                ctx=click.get_current_context())

        if not model:
            raise click.BadParameter(
                "Model argument is no valid odoo model.",
                ctx=click.get_current_context(), param_hint=model)
        with open(s, 'rb') as stream:
            _load_dataframes(stream, type_, model)

    GRAPH.load_metadata()
    GRAPH.seed_edges()
    GRAPH.order_to_parent()
    GRAPH.chunk_dataframes(batch)
    GRAPH.flush_all(out)  # Sychronous loading


if __name__ == '__main__':  # pragma: no cover
    main()
