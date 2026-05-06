from datetime import datetime

import pandas as pd

from app.pipeline.m3_triagem import executar_m3_triagem


def _row(id_linha, peso=100.0, agenda=None, dle="2026-05-06", folga=1, **extra):
    base = {
        "id_linha_pipeline": id_linha,
        "nro_documento": f"DOC{id_linha}",
        "destinatario": "Cliente",
        "cidade": "SAO PAULO",
        "uf": "SP",
        "latitude_destinatario": -23.5,
        "longitude_destinatario": -46.6,
        "data_agenda": agenda,
        "data_leadtime": dle,
        "data_limite_considerada": agenda or dle,
        "tipo_data_limite": "dle",
        "dias_ate_data_alvo": 1,
        "transit_time_dias": 1,
        "folga_dias": folga,
        "status_folga": "ok",
        "peso_kg": peso,
        "peso_calculado": peso,
    }
    base.update(extra)
    return base


def test_m3_separa_redespacho_e_trata_excecao_codigo_ausente():
    df = pd.DataFrame([
        _row("A", agenda=None, dle="2026-05-06", folga=1, redespacho_flag=False, redespacho_codigo=None, tipo_operacao="normal"),
        _row("B", agenda="2026-05-10", folga=3, redespacho_flag=True, redespacho_codigo="01", tipo_operacao="redespacho"),
        _row("C", agenda="2026-05-01", folga=-2, redespacho_flag=True, redespacho_codigo="01", tipo_operacao="redespacho"),
        _row("D", agenda=None, dle=None, folga=None, cidade=None, uf=None, latitude_destinatario=None, longitude_destinatario=None, redespacho_flag=True, redespacho_codigo="01", tipo_operacao="redespacho"),
        _row("E", agenda=None, dle="2026-05-06", folga=1, redespacho_flag=True, redespacho_codigo=None, tipo_operacao="redespacho"),
    ])

    _, meta = executar_m3_triagem(df, datetime(2026, 5, 6))
    outputs = meta["outputs_m3"]

    df_redespacho = outputs["df_carteira_redespacho"]
    assert set(df_redespacho["id_linha_pipeline"].tolist()) == {"B", "C", "D"}

    assert "B" not in outputs["df_carteira_agendamento_futuro"]["id_linha_pipeline"].tolist()
    assert "C" not in outputs["df_carteira_agendas_vencidas"]["id_linha_pipeline"].tolist()
    assert "D" not in outputs["df_carteira_roteirizavel"]["id_linha_pipeline"].tolist()

    df_excecoes = outputs["df_carteira_triagem"]
    linha_e = df_excecoes.loc[df_excecoes["id_linha_pipeline"] == "E"].iloc[0]
    assert linha_e["status_triagem"] == "excecao_triagem"
    assert linha_e["motivo_triagem"] == "redespacho_codigo_ausente"
