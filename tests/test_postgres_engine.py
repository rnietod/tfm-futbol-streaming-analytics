"""
Tests del cliente Postgres — Parte 1 del fix de replay.

Verifican que `get_db_engine()` crea el engine (y su pool) UNA sola vez y lo
reutiliza, en lugar de recrearlo en cada request (causa principal de la
lentitud del replay). No requieren una base de datos real: se mockea
`create_engine` y la lectura de config.
"""
from unittest.mock import mock_open

import src.data.postgres_client as pc


def _patch_config(mocker):
    """Evita depender de configs/dev.json real durante el test."""
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("builtins.open", mock_open(read_data='{"cloudsql": {}}'))


def test_engine_is_singleton(mocker):
    """Llamadas repetidas devuelven el MISMO engine y create_engine corre 1 vez."""
    pc._ENGINE = None  # aislar el test
    sentinel = object()
    create = mocker.patch("sqlalchemy.create_engine", return_value=sentinel)
    _patch_config(mocker)

    e1 = pc.get_db_engine()
    e2 = pc.get_db_engine()

    assert e1 is e2 is sentinel
    assert create.call_count == 1  # no se recrea por llamada

    pc._ENGINE = None  # limpieza para no contaminar otros tests


def test_engine_uses_persistent_pool(mocker):
    """El engine se crea con pre-ping y pool persistente (reutiliza conexiones)."""
    pc._ENGINE = None
    create = mocker.patch("sqlalchemy.create_engine", return_value=object())
    _patch_config(mocker)

    pc.get_db_engine()

    kwargs = create.call_args.kwargs
    assert kwargs.get("pool_pre_ping") is True
    assert kwargs.get("pool_size", 0) >= 1

    pc._ENGINE = None
