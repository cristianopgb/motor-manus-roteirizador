import pandas as pd

from app.pipeline.m5_common import detectar_limbo_entre_perfis


def test_detector_limbo_entre_34_e_toco():
    df = pd.DataFrame([
        {"perfil": "3/4", "capacidade_peso_kg": 4500, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100},
        {"perfil": "TOCO", "capacidade_peso_kg": 10000, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100},
    ])
    out = detectar_limbo_entre_perfis(5000, df)
    assert out["em_limbo"] is True
    assert out["perfil_menor"] == "3/4"
    assert out["perfil_maior"] == "TOCO"


def test_detector_nao_limbo_para_8000():
    df = pd.DataFrame([
        {"perfil": "3/4", "capacidade_peso_kg": 4500, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100},
        {"perfil": "TOCO", "capacidade_peso_kg": 10000, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100},
    ])
    out = detectar_limbo_entre_perfis(8000, df)
    assert out["em_limbo"] is False
