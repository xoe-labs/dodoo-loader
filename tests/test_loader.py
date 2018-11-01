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

import os

from click.testing import CliRunner
from click_odoo import OdooEnvironment

from src.loader import main

# import mock


HERE = os.path.dirname(__file__)
DATADIR = os.path.join(HERE, "data/test_loader/")


def test_bad_parameter(odoodb, jsonlog, odoocfg):
    """ Test if XLSX, XLS, CSV & JSON files load into DataSetGraph """

    # Neither --stream nor --file defined.
    result = CliRunner().invoke(
        main, ["-d", odoodb, "-c", str(odoocfg), "--out", str(jsonlog)]
    )
    assert "No stream or file input defined. " in result.output

    # --stream needs 3 arguments
    result = CliRunner().invoke(
        main,
        ["-d", odoodb, "-c", str(odoocfg), "--out", str(jsonlog), "--stream", "a b"],
    )
    assert "--stream option requires 3 arguments" in result.output

    # Not supported file format
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "not_supported.abc",
            "--out",
            str(jsonlog),
        ],
    )
    assert "Supported formats:" in result.output

    # Not supported stream type
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--stream",
            "/dev/stdin",
            "xls",
            "res.partner",
            "--out",
            str(jsonlog),
        ],
    )
    assert "Supported formats for type argument:" in result.output

    # No valid odoo model file
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "res.no.partner.csv",
            "--out",
            str(jsonlog),
        ],
    )
    assert "Filename is no valid odoo model. For non-excel files, " in result.output

    # No valid odoo model stream
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--stream",
            "stream",
            "json",
            "res.no.partner",
            "--out",
            str(jsonlog),
        ],
    )
    assert "Model argument is no valid odoo model." in result.output


def test_read_basic_files(odoodb, jsonlog, odoocfg, mocker):
    """ Test if XLSX, XLS, CSV & JSON files load into DataSetGraph """

    # Test xlsx
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "res_partner.xlsx",
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0

    # Test xls
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "res_partner.xls",
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0

    # Test json
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "res.partner.json",
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0
    self = mocker.patch("click_odoo.CommandWithOdooEnv")
    self.database = odoodb
    with OdooEnvironment(self) as env:
        assert env.ref("__import__.res_partner_5")  # XLSX
        assert env.ref("__import__.res_partner_10")  # XLS
        assert env.ref("__import__.res_partner_24")  # JSON


def test_file_dependency(odoodb, jsonlog, odoocfg, mocker):
    """ Test dependency either between files or within a file (hierarchy) """

    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "res.country.state.json",  # Should load second
            "--file",
            DATADIR + "res.country.json",  # Should load first
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0

    # Test csv & parent field reorganization
    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "res.partner.csv",  # Records are in wrong order
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0
    self = mocker.patch("click_odoo.CommandWithOdooEnv")
    self.database = odoodb
    with OdooEnvironment(self) as env:
        assert env.ref("__import__.res_country_state_1")  # Dependencies
        assert env.ref("__import__.res_partner_18")  # CSV with parent field


def test_subfield_fails_gracefully(odoodb, jsonlog, odoocfg):
    """ Test unsupported subfield and nested notation give correct errors """

    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "2many_fail/res.country.json",
            "--out",
            str(jsonlog),
        ],
    )
    assert "subfield notation is not supported" in result.output


def test_log_deduplication_1(odoodb, jsonlog, odoocfg):
    """ Test if log is correctly read to avoid duplicated loading """

    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "log_deduplication/res.country.json",
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0

    result = CliRunner().invoke(
        main,
        [
            "-d",
            odoodb,
            "-c",
            str(odoocfg),
            "--file",
            DATADIR + "log_deduplication/res.country.json",
            "--out",
            str(jsonlog),
        ],
    )
    assert result.exit_code == 0
    with open(str(jsonlog), "r") as logs:
        assert not logs.read().endswith(
            """
},{
    "batch": 0,
    "candidates": [
        "__import__.res_country_test_1",
        "__import__.res_country_test_2",
        "__import__.res_country_test_3"
    ],
    "loaded": [
        252,
        253,
        254
    ],
    "model": "res.country",
    "state": "success",
    "x_msgs": []
},{
    "batch": 0,
    "candidates": [
        "__import__.res_country_test_1",
        "__import__.res_country_test_2",
        "__import__.res_country_test_3"
    ],
    "loaded": [
        252,
        253,
        254
    ],
    "model": "res.country",
    "state": "success",
    "x_msgs": []
},{}]"""
        )  # Esure second load did not do (and log) anything
