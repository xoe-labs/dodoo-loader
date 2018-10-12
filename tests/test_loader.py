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

import click
from click.testing import CliRunner
# import mock

from src.loader import main

HERE = os.path.dirname(__file__)
DATADIR = os.path.join(HERE, 'data/test_loader/')


def test_read_files(odoodb, odoocfg):
    """ Test if XLSX, XLS, CSV & JSON files load into DataSetGraph """

    # First try if script bails out correctly for config errors
    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "noname1",
        '--src', DATADIR + "noname1",
        # default: '--type', "csv",
        'res.partner'
    ])

    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "res_partner.xls",
        '--type', "xls",
        'res.partner'
    ])

    # Serious loadnig
    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "res_partner.xlsx",
        '--type', "xls",
    ])

    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "res_partner.xls",
        '--type', "xls",
    ])

    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "res.partner.csv",
        # default: '--type', "csv",
    ])

    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "res.partner.json",
        '--type', "json",
    ])

    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "noname1",
        '--src', DATADIR + "noname1",
        # default: '--type', "csv",
        'res.partner res.partner'
    ])


def test_file_dependency(odoodb, odoocfg):
    """ Test if two dependend files will be loaded in the correct order """

    CliRunner().invoke(main, [
        '-d', odoodb,
        '-c', str(odoocfg),
        '--src', DATADIR + "res.country.state.json",  # Should load second
        '--src', DATADIR + "res.country.json",  # Should load first
        '--type', "json",
    ])
