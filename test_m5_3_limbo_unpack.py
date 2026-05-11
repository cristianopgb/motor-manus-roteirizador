import pandas as pd

from app.pipeline import m5_3_composicao_subregioes as m53


def test_extracao_segura_no_fallback_limbo_nao_desempacota_fixamente():
    row = pd.Series({"tipo": "TRUCK", "perfil": "P", "capacidade_peso_kg": 1000, "capacidade_vol_m3": 10, "max_entregas": 10, "max_km_distancia": 1000, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100})
    df = pd.DataFrame(
        [{
            "peso_calculado": 100,
            "peso_kg": 100,
            "vol_m3": 1,
            "distancia_rodoviaria_est_km": 10,
            "destinatario": "A",
            "id_linha_pipeline": 1,
            "corredor_30g_idx": 1,
        }]
    )

    resultado = m53._validar_fechamento(df, row, suffix="m53", corredor_ancora=1, tolerancia_corredor=1)
    ok, motivo = m53._extrair_ok_motivo_validacao(resultado)

    assert isinstance(resultado, tuple)
    assert len(resultado) >= 3
    assert isinstance(ok, bool)
    assert motivo is None or isinstance(motivo, str)
