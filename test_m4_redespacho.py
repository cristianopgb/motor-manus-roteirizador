from datetime import datetime
import pandas as pd

from app.pipeline.m4_manifestos_fechados import executar_m4_manifestos_fechados


def _row(id_linha, destinatario, peso, redespacho_codigo="", redespacho_nome="", exclusivo=False):
    return {
        "id_linha_pipeline": id_linha,
        "destinatario": destinatario,
        "cidade": "SAO PAULO",
        "uf": "SP",
        "peso_kg": float(peso),
        "vol_m3": 1.0,
        "peso_calculado": float(peso),
        "distancia_rodoviaria_est_km": 10.0,
        "status_triagem": "roteirizavel",
        "grupo_saida": "df_carteira_roteirizavel",
        "veiculo_exclusivo_flag": exclusivo,
        "restricao_veiculo": None,
        "cte": id_linha,
        "redespacho_codigo": redespacho_codigo,
        "redespacho_transportadora_nome": redespacho_nome,
        "redespacho_transportadora_id": "T1" if redespacho_codigo else None,
        "tipo_operacao": "redespacho" if redespacho_codigo else "normal",
    }


def _veiculos(com_carreta=True):
    base = [
        {"tipo": "VUC", "capacidade_peso_kg": 3000, "capacidade_vol_m3": 10, "max_entregas": 10, "max_km_distancia": 100, "ocupacao_minima_perc": 70, "ocupacao_maxima_perc": 100},
        {"tipo": "TOCO", "capacidade_peso_kg": 6000, "capacidade_vol_m3": 20, "max_entregas": 10, "max_km_distancia": 100},
        {"tipo": "TRUCK", "capacidade_peso_kg": 14000, "capacidade_vol_m3": 40, "max_entregas": 10, "max_km_distancia": 100},
    ]
    if com_carreta:
        base.append({"tipo": "CARRETA", "capacidade_peso_kg": 27000, "capacidade_vol_m3": 80, "max_entregas": 10, "max_km_distancia": 100})
    return pd.DataFrame(base)


def _run(df_input, df_redespacho, veiculos):
    return executar_m4_manifestos_fechados(
        df_input_oficial_bloco_4=df_input,
        df_veiculos_tratados=veiculos,
        rodada_id="r1",
        data_base_roteirizacao=datetime(2026, 5, 6),
        df_carteira_redespacho=df_redespacho,
    )[0]


def test_redespacho_normal_menor_veiculo_viavel():
    out = _run(
        pd.DataFrame([_row("N1", "CLIENTE N", 500)]),
        pd.DataFrame([_row("R1", "CLIENTE X", 2000, "01", "TRANS001"), _row("R2", "CLIENTE X", 3000, "01", "TRANS001")]),
        _veiculos(com_carreta=True),
    )
    rd = out["df_manifestos_fechados_bloco_4"].loc[lambda d: d["tipo_manifesto"].eq("redespacho")]
    assert len(rd) == 1
    assert rd.iloc[0]["veiculo_tipo"] == "TOCO"
    assert rd.iloc[0]["redespacho_excede_capacidade"] == False


def test_redespacho_excede_maior_usa_carreta_sem_split():
    out = _run(
        pd.DataFrame([_row("N1", "CLIENTE N", 500)]),
        pd.DataFrame([_row("R1", "CLIENTE X", 20000, "01", "TRANS001"), _row("R2", "CLIENTE X", 20000, "01", "TRANS001")]),
        _veiculos(com_carreta=True),
    )
    rd = out["df_manifestos_fechados_bloco_4"].loc[lambda d: d["tipo_manifesto"].eq("redespacho")]
    assert len(rd) == 1
    assert rd.iloc[0]["veiculo_tipo"] == "CARRETA"
    assert rd.iloc[0]["ocupacao_oficial_perc"] > 100
    assert rd.iloc[0]["redespacho_excede_capacidade"] == True
    rem = out["df_remanescente_roteirizavel_bloco_4"]
    assert "R1" not in rem.get("id_linha_pipeline", pd.Series(dtype="object")).astype(str).tolist()
    assert "R2" not in rem.get("id_linha_pipeline", pd.Series(dtype="object")).astype(str).tolist()


def test_redespacho_excede_sem_carreta_usa_maior_disponivel():
    out = _run(
        pd.DataFrame([_row("N1", "CLIENTE N", 500)]),
        pd.DataFrame([_row("R1", "CLIENTE X", 20000, "01", "TRANS001"), _row("R2", "CLIENTE X", 20000, "01", "TRANS001")]),
        _veiculos(com_carreta=False),
    )
    rd = out["df_manifestos_fechados_bloco_4"].loc[lambda d: d["tipo_manifesto"].eq("redespacho")]
    assert len(rd) == 1
    assert rd.iloc[0]["veiculo_tipo"] == "TRUCK"
    assert rd.iloc[0]["redespacho_excede_capacidade"] == True


def test_redespacho_sobrepoe_exclusivo():
    out = _run(
        pd.DataFrame([_row("N1", "CLIENTE N", 1000)]),
        pd.DataFrame([_row("R1", "CLIENTE X", 2000, "01", "TRANS001", exclusivo=True)]),
        _veiculos(com_carreta=True),
    )
    itens = out["df_itens_manifestos_fechados_bloco_4"]
    linha = itens.loc[itens["id_linha_pipeline"] == "R1"].iloc[0]
    assert linha["tipo_manifesto"] == "redespacho"
    assert linha["origem_etapa"] == "4A_redespacho"


def test_sem_redespacho_preserva_fluxo():
    out = _run(
        pd.DataFrame([_row("N1", "CLIENTE N", 500), _row("N2", "CLIENTE N", 700)]),
        pd.DataFrame(),
        _veiculos(com_carreta=True),
    )
    manifestos = out["df_manifestos_fechados_bloco_4"]
    assert "redespacho" not in manifestos.get("tipo_manifesto", pd.Series(dtype="object")).astype(str).tolist()
