import pandas as pd

from app.pipeline.m4_manifestos_fechados import executar_m4_manifestos_fechados


def _base_rows(pesos):
    rows = []
    for i, p in enumerate(pesos, start=1):
        rows.append(
            {
                "id_linha_pipeline": i,
                "cte": f"CTE{i}",
                "nro_documento": f"DOC{i}",
                "destinatario": "AMERICANAS",
                "cidade": "UBERLANDIA",
                "uf": "MG",
                "subregiao": "UBERLANDIA",
                "mesorregiao": "TRIANGULO MINEIRO/ALTO PARANAIBA",
                "peso_calculado": p,
                "peso_kg": p,
                "vol_m3": 1.0,
                "distancia_rodoviaria_est_km": 200.0,
                "restricao_veiculo": "",
                "veiculo_exclusivo_flag": False,
                "flag_agendada_roteirizavel": False,
                "status_triagem": "roteirizavel",
                "tipo_operacao": "entrega",
            }
        )
    return pd.DataFrame(rows)


def _veiculos():
    return pd.DataFrame(
        [
            {"tipo": "TRUCK", "perfil": "TRUCK", "capacidade_peso_kg": 14000, "capacidade_vol_m3": 100, "max_entregas": 100, "max_km_distancia": 1000, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100, "quantidade": 999},
            {"tipo": "CARRETA", "perfil": "CARRETA", "capacidade_peso_kg": 27000, "capacidade_vol_m3": 200, "max_entregas": 100, "max_km_distancia": 1000, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100, "quantidade": 999},
        ]
    )


def test_m4_limbo_cliente_americanas_fluxo():
    pesos = [5493.60, 1094.40, 232.20, 518.40, 1717.80, 75.60, 5142.60, 601.80]
    df, veics = _base_rows(pesos), _veiculos()
    outputs, _ = executar_m4_manifestos_fechados(df, veics, "R1", pd.Timestamp("2026-01-01"))

    itens = outputs["df_itens_manifestos_fechados_bloco_4"]
    rem = outputs["df_remanescente_roteirizavel_bloco_4"]

    assert len(itens) > 0
    assert (itens["origem_etapa"] == "4C_limbo_cliente").any()
    assert len(itens) + len(rem) == 8
    assert itens["id_linha_pipeline"].nunique() + rem["id_linha_pipeline"].nunique() == 8


def test_m4_limbo_saldo_abaixo_minimo_vai_remanescente():
    pesos = [13000.0, 900.0]
    df, veics = _base_rows(pesos), _veiculos()
    outputs, _ = executar_m4_manifestos_fechados(df, veics, "R2", pd.Timestamp("2026-01-01"))
    itens = outputs["df_itens_manifestos_fechados_bloco_4"]
    rem = outputs["df_remanescente_roteirizavel_bloco_4"]
    assert len(itens) + len(rem) == 2
    assert len(rem) >= 1
