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

import os

# import click
from click.testing import CliRunner
# import mock

from src.loader import main

HERE = os.path.dirname(__file__)
DATADIR = os.path.join(HERE, 'data/test_loader/')


def test_bad_parameter(odoodb, odoocfg):
    """ Test if XLSX, XLS, CSV & JSON files load into DataSetGraph """

    # Neither --stream nor --file defined.
    result = CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
    ])
    assert "No stream or file input defined. " in result.output

    # --stream needs 3 arguments
    result = CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--stream', 'a', 'b',
    ])
    assert "--stream option requires 3 arguments" in result.output

    # Not supported file format
    result = CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--file', DATADIR + 'not_supported.abc',
    ])
    assert "Supported formats:" in result.output

    # Not supported stream type
    result = CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--stream', '/dev/stdin', 'xls', 'res.partner'
    ])
    assert "Supported formats for type argument:" in result.output

    # No valid odoo model file
    result = CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--file', DATADIR + 'res.no.partner.csv'
    ])
    assert "Filename is no valid odoo model. For non-excel files, " \
        in result.output

    # No valid odoo model stream
    result = CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--stream', 'stream', 'json', 'res.no.partner'
    ])
    assert "Model argument is no valid odoo model." in result.output


# def test_read_files(odoodb, odoocfg):
#     """ Test if XLSX, XLS, CSV & JSON files load into DataSetGraph """

#     # With two --src, ther must be two --model, not one.
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--file', DATADIR + "noname1",
#         '--stream', DATADIR + "noname2",
#     ])
#     assert "If we cannot infer a model from filename, " in result.output

#     # Cannot specify --model with xls
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "res_partner.xls",
#         '--type', "xls",
#         '--model', 'res.partner',
#     ])
#     assert "You cannot specify --model for 'xls' imports." in result.output

#     # Serious loading

#     # Test xlsx
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "res_partner.xlsx",
#         '--type', "xls",
#     ])
#     assert '' in result.output
#     assert result.exit_code == 0

#     # Test xls
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "res_partner.xls",
#         '--type', "xls",
#     ])
#     assert result.exit_code == 0

#     # Test csv
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "res.partner.csv",
#         # default: '--type', "csv",
#     ])
#     assert result.exit_code == 0

#     # Test json
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "res.partner.json",
#         '--type', "json",
#     ])
#     assert result.exit_code == 0

#     # Test 2 csv
#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "noname1",
#         '--src', DATADIR + "noname2",
#         # default: '--type', "csv",
#         '--model', 'res.partner',
#         '--model', 'res.partner',
#     ])
#     assert result.exit_code == 0


# def test_file_dependency(odoodb, odoocfg):
#     """ Test if two dependend files will be loaded in the correct order """

#     result = CliRunner().invoke(main, [
#         '-d', odoodb,
#         '-c', str(odoocfg),
#         '--src', DATADIR + "res.country.state.json",  # Should load second
#         '--src', DATADIR + "res.country.json",  # Should load first
#         '--type', "json",
#     ])
#     assert result.exit_code == 0
