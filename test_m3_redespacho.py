from datetime import datetime

import pandas as pd
import pytest

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
        "conferencia": "EM CONFERÊNCIA",
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


def test_m3_bloqueia_sem_conf_e_preserva_fluxo_conf_valida():
    df = pd.DataFrame([
        _row("A", conferencia="EM CONFERÊNCIA", redespacho_flag=False, redespacho_codigo=None, tipo_operacao="normal"),
        _row("B", conferencia="CONFERÊNCIA FINALIZADA", redespacho_flag=False, redespacho_codigo=None, tipo_operacao="normal"),
        _row("C", conferencia="", redespacho_flag=False, redespacho_codigo=None, tipo_operacao="normal"),
        _row("D", conferencia=None, redespacho_flag=True, redespacho_codigo="77", tipo_operacao="redespacho"),
        _row("E", conferencia="EM CONFERENCIA", redespacho_flag=True, redespacho_codigo="88", tipo_operacao="redespacho"),
    ])

    _, meta = executar_m3_triagem(df, datetime(2026, 5, 6))
    outputs = meta["outputs_m3"]
    resumo = meta["resumo_m3"]

    ids_roteirizavel = set(outputs["df_carteira_roteirizavel"]["id_linha_pipeline"].tolist())
    ids_redespacho = set(outputs["df_carteira_redespacho"]["id_linha_pipeline"].tolist())
    df_triagem = outputs["df_carteira_triagem"]

    assert "A" in ids_roteirizavel
    assert "B" in ids_roteirizavel
    assert "C" not in ids_roteirizavel
    assert "D" not in ids_redespacho
    assert "E" in ids_redespacho

    linha_c = df_triagem.loc[df_triagem["id_linha_pipeline"] == "C"].iloc[0]
    assert linha_c["status_triagem"] == "excecao_triagem"
    assert linha_c["motivo_triagem"] == "conf_ausente"

    linha_d = df_triagem.loc[df_triagem["id_linha_pipeline"] == "D"].iloc[0]
    assert linha_d["status_triagem"] == "excecao_triagem"
    assert linha_d["motivo_triagem"] == "conf_ausente"

    assert resumo["total_conf_ausente"] == 2
    assert resumo["total_conf_em_conferencia"] == 2
    assert resumo["total_conf_finalizada"] == 1


def test_m3_lanca_erro_tecnico_quando_coluna_conf_nao_existe():
    df = pd.DataFrame([
        _row("A", conferencia="EM CONFERÊNCIA"),
    ]).drop(columns=["conferencia"])

    with pytest.raises(Exception, match="M3 não recebeu a coluna de conferência obrigatória"):
        executar_m3_triagem(df, datetime(2026, 5, 6))
