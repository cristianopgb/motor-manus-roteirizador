from datetime import datetime
import pandas as pd

from app.pipeline.m4_manifestos_fechados import executar_m4_manifestos_fechados


def _row(id_linha, remetente, destinatario, peso, tipo=None, flag=False, redespacho=False):
    return {
        "id_linha_pipeline": id_linha,
        "cte": id_linha,
        "destinatario": destinatario,
        "remetente": remetente,
        "cidade": "SAO PAULO",
        "uf": "SP",
        "peso_kg": float(peso),
        "vol_m3": 1.0,
        "peso_calculado": float(peso),
        "distancia_rodoviaria_est_km": 10.0,
        "status_triagem": "roteirizavel",
        "grupo_saida": "df_carteira_roteirizavel",
        "veiculo_exclusivo_flag": flag,
        "carro_dedicado_tipo": tipo,
        "tipo_operacao": "redespacho" if redespacho else "normal",
        "redespacho_flag": redespacho,
        "redespacho_codigo": "RD1" if redespacho else None,
        "redespacho_transportadora_id": "T1" if redespacho else None,
        "redespacho_transportadora_nome": "TRANS" if redespacho else None,
    }


def _veiculos():
    return pd.DataFrame([
        {"tipo": "VUC", "capacidade_peso_kg": 3000, "capacidade_vol_m3": 10, "max_entregas": 10, "max_km_distancia": 100},
        {"tipo": "TOCO", "capacidade_peso_kg": 6000, "capacidade_vol_m3": 20, "max_entregas": 10, "max_km_distancia": 100},
    ])


def _run(df):
    out, _ = executar_m4_manifestos_fechados(df, _veiculos(), "r1", datetime(2026, 5, 6))
    return out


def test_cenario_normal_agrupa_por_destinatario():
    df = pd.DataFrame([
        _row("A", "R1", "D1", 1000, tipo="normal", flag=True),
        _row("B", "R2", "D1", 1200, tipo="normal", flag=True),
        _row("C", "R3", "D2", 900, tipo="normal", flag=True),
    ])
    itens = _run(df)["df_itens_manifestos_fechados_bloco_4"]
    dedicados = itens.loc[itens["veiculo_exclusivo_flag"] == True]
    assert len(dedicados) == 3
    assert dedicados.groupby("destinatario")["manifesto_id"].nunique().to_dict() == {"D1": 1, "D2": 1}
    assert (dedicados["carro_dedicado_tipo"] == "normal").all()
    for manifesto_id, grupo in dedicados.groupby("manifesto_id"):
        assert (grupo["carro_dedicado_tipo"] == "normal").all(), f"manifesto {manifesto_id} com tipo incorreto"


def test_cenario_exclusivo_agrupa_por_remetente_destinatario():
    df = pd.DataFrame([
        _row("A", "R1", "D1", 1000, tipo="exclusivo", flag=True),
        _row("B", "R1", "D1", 1200, tipo="exclusivo", flag=True),
        _row("C", "R2", "D1", 900, tipo="exclusivo", flag=True),
    ])
    itens = _run(df)["df_itens_manifestos_fechados_bloco_4"]
    dedicados = itens.loc[itens["veiculo_exclusivo_flag"] == True]
    assert dedicados.groupby(["remetente", "destinatario"])["manifesto_id"].nunique().to_dict() == {("R1", "D1"): 1, ("R2", "D1"): 1}
    assert (dedicados["carro_dedicado_tipo"] == "exclusivo").all()
    for manifesto_id, grupo in dedicados.groupby("manifesto_id"):
        assert (grupo["carro_dedicado_tipo"] == "exclusivo").all(), f"manifesto {manifesto_id} com tipo incorreto"


def test_cenario_nao_contamina_nao_marcado():
    df = pd.DataFrame([
        _row("A", "R1", "D1", 1000, tipo="normal", flag=True),
        _row("B", "R2", "D1", 800, tipo=None, flag=False),
    ])
    out = _run(df)
    itens = out["df_itens_manifestos_fechados_bloco_4"]
    rem = out["df_remanescente_roteirizavel_bloco_4"]
    ids_dedicado = set(itens.loc[itens["veiculo_exclusivo_flag"] == True, "id_linha_pipeline"].astype(str))
    assert "A" in ids_dedicado
    assert "B" not in ids_dedicado
    assert "B" in set(rem["id_linha_pipeline"].astype(str)) or "B" in set(itens["id_linha_pipeline"].astype(str))


def test_cenario_compat_true_sem_tipo_vira_normal():
    df = pd.DataFrame([_row("A", "R1", "D1", 1000, tipo=None, flag=True)])
    itens = _run(df)["df_itens_manifestos_fechados_bloco_4"]
    linha = itens.iloc[0]
    assert linha["carro_dedicado_tipo"] == "normal"


def test_cenario_redespacho_preservado_prioridade():
    df = pd.DataFrame([
        _row("A", "R1", "D1", 1000, tipo="exclusivo", flag=True, redespacho=True),
    ])
    itens = _run(df)["df_itens_manifestos_fechados_bloco_4"]
    linha = itens.iloc[0]
    assert linha["tipo_manifesto"] == "redespacho"
    assert linha["origem_etapa"] == "4A_redespacho"
