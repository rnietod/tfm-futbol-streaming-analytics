"""
Test del endpoint frame_at (salto del replay a una jugada).

Llama a la función directamente con el engine mockeado: no usa TestClient/httpx
ni una base de datos real, por lo que corre en CI sin Postgres/Redis.
"""
from unittest.mock import MagicMock


def _mock_engine(mocker, first_row):
    """Mockea get_db_engine -> connect() (context manager) -> execute().first()."""
    conn = MagicMock()
    conn.execute.return_value.first.return_value = first_row
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = cm
    import src.api.main as main
    mocker.patch.object(main, "get_db_engine", return_value=engine)
    return main, conn


def test_frame_at_devuelve_frame_y_pasa_params(mocker):
    row = MagicMock()
    row.frame_idx = 9220
    main, conn = _mock_engine(mocker, row)

    result = main.get_frame_at("test_match", period=1.0, seconds=788.0)

    assert result == {"frame_idx": 9220}
    # la query recibió el periodo y el segundo objetivo
    params = conn.execute.call_args[0][1]
    assert params["seconds"] == 788.0
    assert params["period"] == 1.0
    assert params["mid"] == "test_match"


def test_frame_at_sin_frame_devuelve_none(mocker):
    main, _ = _mock_engine(mocker, None)
    result = main.get_frame_at("test_match", period=1.0, seconds=10.0)
    assert result["frame_idx"] is None
