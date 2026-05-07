import pandas as pd

from app.pipeline.m1_padronizacao import executar_m1_padronizacao


def test_m1_redespacho_flag_bool_safe_tipos_mistos():
    df_carteira = pd.DataFrame([
        {"nro_doc": "1", "cidad": "SAO PAULO", "uf": "SP", "dle": "2026-05-06", "redespacho_flag": True, "redespacho_codigo": "1", "peso": 10, "peso_calculo": 10},
        {"nro_doc": "2", "cidad": "SAO PAULO", "uf": "SP", "dle": "2026-05-06", "redespacho_flag": "true", "redespacho_codigo": "1", "peso": 10, "peso_calculo": 10},
        {"nro_doc": "3", "cidad": "SAO PAULO", "uf": "SP", "dle": "2026-05-06", "redespacho_flag": 1, "redespacho_codigo": "1", "peso": 10, "peso_calculo": 10},
        {"nro_doc": "4", "cidad": "SAO PAULO", "uf": "SP", "dle": "2026-05-06", "redespacho_flag": False, "redespacho_codigo": None, "peso": 10, "peso_calculo": 10},
        {"nro_doc": "5", "cidad": "SAO PAULO", "uf": "SP", "dle": "2026-05-06", "redespacho_flag": None, "redespacho_codigo": "1", "peso": 10, "peso_calculo": 10},
    ])

    out = executar_m1_padronizacao(
        df_carteira_raw=df_carteira,
        df_geo_raw=pd.DataFrame(),
        df_parametros_raw=pd.DataFrame([{"origem_cidade": "SAO PAULO", "origem_uf": "SP", "data_base_roteirizacao": "2026-05-06"}]),
        df_veiculos_raw=pd.DataFrame(),
    )

    tratada = out["df_carteira_tratada"]

    assert str(tratada["redespacho_flag"].dtype) == "bool"
    assert tratada.loc[tratada["nro_documento"].astype(str).eq("4"), "tipo_operacao"].iloc[0] == "normal"
    assert tratada.loc[tratada["nro_documento"].astype(str).isin(["1", "2", "3", "5"]), "redespacho_flag"].all()
    assert tratada.loc[tratada["nro_documento"].astype(str).isin(["1", "2", "3", "5"]), "tipo_operacao"].eq("redespacho").all()
    assert tratada.loc[tratada["nro_documento"].astype(str).eq("1"), "redespacho_codigo"].iloc[0] == "1"
