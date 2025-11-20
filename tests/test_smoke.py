def test_environment_is_ready():
    """
    Test de Humo: Solo verifica que pytest puede correr.
    Este test se eliminará cuando llegue el código del simulador.
    """
    assert True is True


def test_python_version():
    """Verifica que estamos corriendo en Python 3"""
    import sys
    assert sys.version_info.major == 3