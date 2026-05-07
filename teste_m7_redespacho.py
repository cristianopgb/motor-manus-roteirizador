import pandas as pd

from app.pipeline.m7_sequenciamento_entregas import executar_m7_sequenciamento_entregas


def _build_inputs():
    df_manifestos = pd.DataFrame([
        {"manifesto_id": "RD_0001", "tipo_manifesto": "redespacho", "tipo_operacao_manifesto": "redespacho", "origem_etapa": "4A_redespacho", "redespacho_flag": True},
        {"manifesto_id": "MNF_0002", "tipo_manifesto": "normal"},
    ])

    itens = []
    for i in range(14):
        itens.append(
            {
                "manifesto_id": "RD_0001",
                "id_linha_pipeline": f"RD-L{i+1}",
                "nro_documento": f"DOC-RD-{i+1}",
                "destinatario": "",
                "cidade": "",
                "uf": "",
                "peso_kg": 10,
                "peso_calculado": 10,
                "agendada": False,
                "folga_dias": 2,
                "distancia_rodoviaria_est_km": None,
                "latitude_filial": -23.55,
                "longitude_filial": -46.63,
                "latitude_destinatario": None,
                "longitude_destinatario": None,
                "redespacho_codigo": "1",
                "redespacho_transportadora_nome": "CLC",
                "tipo_operacao": "redespacho",
            }
        )
    for i in range(2):
        itens.append(
            {
                "manifesto_id": "MNF_0002",
                "id_linha_pipeline": f"NM-L{i+1}",
                "nro_documento": f"DOC-NM-{i+1}",
                "destinatario": f"Cliente {i+1}",
                "cidade": "Campinas",
                "uf": "SP",
                "peso_kg": 5,
                "peso_calculado": 5,
                "agendada": False,
                "folga_dias": 1,
                "distancia_rodoviaria_est_km": 20,
                "latitude_filial": -23.55,
                "longitude_filial": -46.63,
                "latitude_destinatario": -22.90 - i * 0.01,
                "longitude_destinatario": -47.06 - i * 0.01,
            }
        )
    return df_manifestos, pd.DataFrame(itens)


def test_m7_redespacho_parada_unica():
    df_manifestos, df_itens = _build_inputs()

    outputs, meta = executar_m7_sequenciamento_entregas(
        df_manifestos_m6_2=df_manifestos,
        df_itens_manifestos_m6_2=df_itens,
        filial_contexto={"cidade": "Sao Paulo", "uf": "SP"},
    )

    itens = outputs["df_itens_manifestos_sequenciados_m7"]
    itens_rd = itens[itens["manifesto_id"] == "RD_0001"]
    assert len(itens_rd) == 14
    assert itens_rd["ordem_entrega_doc_m7"].notna().all()
    assert itens_rd["ordem_carregamento_doc_m7"].notna().all()
    assert ((itens_rd["ordem_entrega_parada_m7"] == 1) | (itens_rd["ordem_parada_m7"] == 1)).all()
    assert (itens_rd["status_sequenciamento_m7"] == "nao_aplicavel_redespacho").all()

    paradas = outputs["df_paradas_m7"]
    paradas_rd = paradas[paradas["manifesto_id"] == "RD_0001"]
    assert len(paradas_rd) == 1
    assert paradas_rd.iloc[0]["tipo_parada"] == "redespacho_coleta_filial"

    assert "RD_0001" in outputs["df_manifestos_m7"]["manifesto_id"].astype(str).tolist()
    assert "MNF_0002" in itens["manifesto_id"].astype(str).tolist()
    assert meta["resumo_m7"]["manifestos_saida_m7"] == 2
