from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =========================================================================================
# M6.1 - CONSOLIDAÇÃO E PREPARAÇÃO DA OTIMIZAÇÃO
# =========================================================================================


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    txt = str(value).strip().lower()
    return txt in {"1", "true", "sim", "s", "yes", "y", "verdadeiro"}


def _normalizar_datetime_para_str(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
    return out


def _garantir_colunas(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in colunas:
        if col not in out.columns:
            out[col] = None
    return out


def _resolver_coluna_existente(
    df: pd.DataFrame,
    candidatos: List[str],
    nome_logico: str,
    obrigatoria: bool = True,
) -> str:
    for c in candidatos:
        if c in df.columns:
            return c

    if obrigatoria:
        raise Exception(
            f"M6.1 não encontrou a coluna obrigatória '{nome_logico}'. "
            f"Esperado um destes nomes: {candidatos}. "
            f"Corrija o contrato do módulo anterior."
        )

    return ""


def _col_manifesto_id(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["manifesto_id"], "manifesto_id", True)


def _col_veiculo_tipo(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["veiculo_tipo", "tipo"], "veiculo_tipo", True)


def _col_veiculo_perfil(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["veiculo_perfil", "perfil"], "veiculo_perfil", False)


def _col_km_manifesto(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(
        df,
        ["km_referencia", "km_total", "km_manifesto", "km_base_antes_m6"],
        "km_manifesto",
        True,
    )


def _col_peso_manifesto(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(
        df,
        ["base_carga_oficial", "peso_total_kg", "peso_total", "peso_base_antes_m6"],
        "peso_manifesto",
        True,
    )


def _col_ocupacao_manifesto(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(
        df,
        ["ocupacao_oficial_perc", "ocupacao_perc", "ocupacao_base_antes_m6"],
        "ocupacao_manifesto",
        True,
    )


def _col_capacidade_peso(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["capacidade_peso_kg_veiculo"], "capacidade_peso_kg_veiculo", True)


def _col_max_km(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["max_km_distancia_veiculo"], "max_km_distancia_veiculo", True)


def _col_qtd_itens(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["qtd_itens"], "qtd_itens", False)


def _col_qtd_ctes(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["qtd_ctes"], "qtd_ctes", False)


def _col_qtd_paradas(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(df, ["qtd_paradas"], "qtd_paradas", False)


def _col_max_paradas_manifesto(df: pd.DataFrame) -> str:
    return _resolver_coluna_existente(
        df,
        ["max_paradas_veiculo", "max_entregas_veiculo", "max_entregas", "limite_entregas"],
        "max_paradas_veiculo",
        True,
    )


def _coletar_textos_exclusividade(df: pd.DataFrame) -> pd.Series:
    colunas_texto = [
        "obs",
        "observacao",
        "observacoes",
        "observacao_manifesto",
        "observacoes_manifesto",
        "obs_manifesto",
        "justificativa",
        "justificativa_fechamento",
        "motivo_fechamento",
        "motivo",
        "regra_aplicada",
        "observacao_regra",
        "descricao_regra",
        "status_regra",
    ]

    existentes = [c for c in colunas_texto if c in df.columns]
    if not existentes:
        return pd.Series([""] * len(df), index=df.index)

    serie = pd.Series([""] * len(df), index=df.index, dtype="object")
    for col in existentes:
        texto = df[col].fillna("").astype(str).str.strip()
        serie = (serie.fillna("") + " | " + texto).str.strip(" |")
    return serie.fillna("").astype(str)


def _serie_veiculo_exclusivo(df: pd.DataFrame) -> pd.Series:
    out = pd.Series([False] * len(df), index=df.index)

    colunas_flag = [
        "veiculo_exclusivo_flag",
        "veiculo_exclusivo",
        "exclusivo",
        "carga_exclusiva",
        "manifesto_exclusivo",
        "exclusivo_m4",
    ]
    existentes_flag = [c for c in colunas_flag if c in df.columns]
    for col in existentes_flag:
        out = out | df[col].apply(_to_bool)

    textos = _coletar_textos_exclusividade(df).str.lower()
    palavras_chave = [
        "exclusivo",
        "veiculo exclusivo",
        "veículo exclusivo",
        "carga exclusiva",
        "manifesto exclusivo",
        "dedicado",
        "dedicada",
    ]

    texto_flag = pd.Series([False] * len(df), index=df.index)
    for termo in palavras_chave:
        texto_flag = texto_flag | textos.str.contains(termo, regex=False)

    out = out | texto_flag
    return out.astype(bool)


def _padronizar_manifestos(
    df_manifestos: Optional[pd.DataFrame],
    origem_modulo: str,
    tipo_manifesto_origem: str,
) -> pd.DataFrame:
    if df_manifestos is None or df_manifestos.empty:
        return pd.DataFrame()

    df = _normalizar_datetime_para_str(df_manifestos.copy())

    col_manifesto = _col_manifesto_id(df)
    col_tipo = _col_veiculo_tipo(df)
    col_perfil = _col_veiculo_perfil(df)
    col_km = _col_km_manifesto(df)
    col_peso = _col_peso_manifesto(df)
    col_ocup = _col_ocupacao_manifesto(df)
    col_cap_peso = _col_capacidade_peso(df)
    col_max_km = _col_max_km(df)
    col_max_paradas = _col_max_paradas_manifesto(df)
    col_qtd_itens = _col_qtd_itens(df)
    col_qtd_ctes = _col_qtd_ctes(df)
    col_qtd_paradas = _col_qtd_paradas(df)

    if col_qtd_itens == "":
        df["qtd_itens"] = 0
        col_qtd_itens = "qtd_itens"

    if col_qtd_ctes == "":
        df["qtd_ctes"] = 0
        col_qtd_ctes = "qtd_ctes"

    if col_qtd_paradas == "":
        df["qtd_paradas"] = 0
        col_qtd_paradas = "qtd_paradas"

    if col_perfil == "":
        serie_perfil = df[col_tipo]
    else:
        serie_perfil = df[col_perfil].replace("", pd.NA).fillna(df[col_tipo])

    serie_exclusivo = _serie_veiculo_exclusivo(df)
    texto_exclusivo = _coletar_textos_exclusividade(df)

    out = pd.DataFrame(
        {
            "manifesto_id": df[col_manifesto].astype(str),
            "origem_manifesto_modulo": origem_modulo,
            "origem_manifesto_tipo": tipo_manifesto_origem,
            "veiculo_tipo": df[col_tipo].astype(str),
            "veiculo_perfil": serie_perfil.astype(str),
            "veiculo_exclusivo_flag": serie_exclusivo.astype(bool),
            "texto_exclusivo_detectado_m6": texto_exclusivo.astype(str),
            "peso_base_antes_m6": pd.to_numeric(df[col_peso], errors="coerce").fillna(0.0),
            "km_base_antes_m6": pd.to_numeric(df[col_km], errors="coerce").fillna(0.0),
            "ocupacao_base_antes_m6": pd.to_numeric(df[col_ocup], errors="coerce").fillna(0.0),
            "capacidade_peso_kg_veiculo": pd.to_numeric(df[col_cap_peso], errors="coerce").fillna(0.0),
            "max_km_distancia_veiculo": pd.to_numeric(df[col_max_km], errors="coerce").fillna(0.0),
            "max_paradas_veiculo": pd.to_numeric(df[col_max_paradas], errors="coerce").fillna(0).astype(int),
            "qtd_itens_base_antes_m6": pd.to_numeric(df[col_qtd_itens], errors="coerce").fillna(0).astype(int),
            "qtd_ctes_base_antes_m6": pd.to_numeric(df[col_qtd_ctes], errors="coerce").fillna(0).astype(int),
            "qtd_paradas_base_antes_m6": pd.to_numeric(df[col_qtd_paradas], errors="coerce").fillna(0).astype(int),
        }
    )

    out["manifesto_id"] = out["manifesto_id"].fillna("").astype(str).str.strip()
    out["veiculo_tipo"] = out["veiculo_tipo"].fillna("").astype(str).str.strip()
    out["veiculo_perfil"] = (
        out["veiculo_perfil"].fillna("").astype(str).str.strip().replace("", pd.NA).fillna(out["veiculo_tipo"])
    )
    out["veiculo_exclusivo_flag"] = out["veiculo_exclusivo_flag"].fillna(False).astype(bool)
    out["texto_exclusivo_detectado_m6"] = out["texto_exclusivo_detectado_m6"].fillna("").astype(str)

    out = out[out["manifesto_id"] != ""].copy()
    out = out.drop_duplicates(subset=["manifesto_id"], keep="first").reset_index(drop=True)

    if (out["capacidade_peso_kg_veiculo"] <= 0).any():
        ruins = out.loc[out["capacidade_peso_kg_veiculo"] <= 0, "manifesto_id"].astype(str).tolist()[:20]
        raise Exception(f"M6.1 encontrou manifestos sem capacidade_peso_kg_veiculo válida: {ruins}")

    if (out["max_km_distancia_veiculo"] <= 0).any():
        ruins = out.loc[out["max_km_distancia_veiculo"] <= 0, "manifesto_id"].astype(str).tolist()[:20]
        raise Exception(f"M6.1 encontrou manifestos sem max_km_distancia_veiculo válido: {ruins}")

    if (out["max_paradas_veiculo"] <= 0).any():
        ruins = out.loc[out["max_paradas_veiculo"] <= 0, "manifesto_id"].astype(str).tolist()[:20]
        raise Exception(f"M6.1 encontrou manifestos sem max_paradas_veiculo válido: {ruins}")

    return out


def _padronizar_itens_manifestados(
    df_itens: Optional[pd.DataFrame],
    origem_modulo: str,
    tipo_manifesto_origem: str,
) -> pd.DataFrame:
    if df_itens is None or df_itens.empty:
        return pd.DataFrame()

    df = _normalizar_datetime_para_str(df_itens.copy())

    colunas_minimas = [
        "manifesto_id",
        "nro_documento",
        "destinatario",
        "cidade",
        "uf",
        "subregiao",
        "mesorregiao",
        "distancia_rodoviaria_est_km",
        "peso_calculado",
        "peso_kg",
        "vol_m3",
        "restricao_veiculo",
        "veiculo_exclusivo_flag",
        "origem_etapa",
        # contrato geo/origem preservado do M2 em diante
        "latitude_filial",
        "longitude_filial",
        "origem_latitude",
        "origem_longitude",
        "latitude_destinatario",
        "longitude_destinatario",
        "latitude",
        "longitude",
    ]
    df = _garantir_colunas(df, colunas_minimas)

    if "id_linha_pipeline" not in df.columns:
        df["id_linha_pipeline"] = (
            df["manifesto_id"].fillna("").astype(str).str.strip()
            + "::"
            + df["nro_documento"].fillna("").astype(str).str.strip()
            + "::"
            + df.reset_index().index.astype(str)
        )

    out = pd.DataFrame(
        {
            "manifesto_id": df["manifesto_id"].astype(str),
            "origem_manifesto_modulo": origem_modulo,
            "origem_manifesto_tipo": tipo_manifesto_origem,
            "id_linha_pipeline": df["id_linha_pipeline"].astype(str),
            "nro_documento": df["nro_documento"],
            "destinatario": df["destinatario"],
            "cidade": df["cidade"],
            "uf": df["uf"],
            "subregiao": df["subregiao"],
            "mesorregiao": df["mesorregiao"],
            "distancia_rodoviaria_est_km": pd.to_numeric(df["distancia_rodoviaria_est_km"], errors="coerce").fillna(0.0),
            "peso_calculado": pd.to_numeric(df["peso_calculado"], errors="coerce").fillna(0.0),
            "peso_kg": pd.to_numeric(df["peso_kg"], errors="coerce").fillna(0.0),
            "vol_m3": pd.to_numeric(df["vol_m3"], errors="coerce").fillna(0.0),
            "restricao_veiculo": df["restricao_veiculo"],
            "veiculo_exclusivo_flag": df["veiculo_exclusivo_flag"].apply(_to_bool),
            "origem_etapa": df["origem_etapa"].fillna("").astype(str),
            # preservação de contrato geo/origem sem alterar lógica do M6
            "latitude_filial": pd.to_numeric(df["latitude_filial"], errors="coerce"),
            "longitude_filial": pd.to_numeric(df["longitude_filial"], errors="coerce"),
            "origem_latitude": pd.to_numeric(df["origem_latitude"], errors="coerce"),
            "origem_longitude": pd.to_numeric(df["origem_longitude"], errors="coerce"),
            "latitude_destinatario": pd.to_numeric(df["latitude_destinatario"], errors="coerce"),
            "longitude_destinatario": pd.to_numeric(df["longitude_destinatario"], errors="coerce"),
            "latitude": pd.to_numeric(df["latitude"], errors="coerce"),
            "longitude": pd.to_numeric(df["longitude"], errors="coerce"),
        }
    )

    out["manifesto_id"] = out["manifesto_id"].fillna("").astype(str).str.strip()
    out["id_linha_pipeline"] = out["id_linha_pipeline"].fillna("").astype(str).str.strip()
    out["mesorregiao"] = out["mesorregiao"].fillna("").astype(str).str.strip()
    out["veiculo_exclusivo_flag"] = out["veiculo_exclusivo_flag"].fillna(False).astype(bool)
    out["origem_etapa"] = out["origem_etapa"].fillna("").astype(str).str.strip()

    out = out[(out["manifesto_id"] != "") & (out["id_linha_pipeline"] != "")].copy()
    out = out.drop_duplicates(subset=["manifesto_id", "id_linha_pipeline"], keep="first").reset_index(drop=True)

    return out


def _recompor_exclusividade_manifestos_m4(
    df_manifestos_base_m6: pd.DataFrame,
    df_itens_manifestos_base_m6: pd.DataFrame,
) -> pd.DataFrame:
    if df_manifestos_base_m6 is None or df_manifestos_base_m6.empty:
        return df_manifestos_base_m6.copy()

    out = df_manifestos_base_m6.copy()

    if df_itens_manifestos_base_m6 is None or df_itens_manifestos_base_m6.empty:
        return out

    itens_m4 = df_itens_manifestos_base_m6.loc[
        df_itens_manifestos_base_m6["origem_manifesto_modulo"].astype(str) == "M4"
    ].copy()

    if itens_m4.empty:
        return out

    itens_m4["veiculo_exclusivo_flag"] = itens_m4["veiculo_exclusivo_flag"].fillna(False).astype(bool)
    itens_m4["origem_etapa"] = itens_m4["origem_etapa"].fillna("").astype(str).str.strip()

    agreg = (
        itens_m4.groupby("manifesto_id", dropna=False)
        .agg(
            veiculo_exclusivo_flag_itens_m4=("veiculo_exclusivo_flag", "any"),
            origem_etapa_4b1_exclusivo_m4=("origem_etapa", lambda s: (s.astype(str) == "4B1_exclusivo").any()),
        )
        .reset_index()
    )

    agreg["veiculo_exclusivo_recomposto_m4"] = agreg["veiculo_exclusivo_flag_itens_m4"].fillna(False).astype(bool)

    mask_sem_flag_item = agreg["veiculo_exclusivo_flag_itens_m4"].fillna(False) == False
    agreg.loc[mask_sem_flag_item, "veiculo_exclusivo_recomposto_m4"] = agreg.loc[
        mask_sem_flag_item, "origem_etapa_4b1_exclusivo_m4"
    ].fillna(False).astype(bool)

    out = out.merge(
        agreg[
            [
                "manifesto_id",
                "veiculo_exclusivo_recomposto_m4",
                "veiculo_exclusivo_flag_itens_m4",
                "origem_etapa_4b1_exclusivo_m4",
            ]
        ],
        on="manifesto_id",
        how="left",
    )

    mask_m4 = out["origem_manifesto_modulo"].astype(str) == "M4"

    out.loc[mask_m4, "veiculo_exclusivo_flag"] = (
        out.loc[mask_m4, "veiculo_exclusivo_flag"].fillna(False).astype(bool)
        | out.loc[mask_m4, "veiculo_exclusivo_recomposto_m4"].fillna(False).astype(bool)
    )

    out["veiculo_exclusivo_flag"] = out["veiculo_exclusivo_flag"].fillna(False).astype(bool)
    return out


def _estatisticas_manifestos_antes(
    df_manifestos_base: pd.DataFrame,
    df_itens_base: pd.DataFrame,
) -> pd.DataFrame:
    if df_manifestos_base.empty:
        return pd.DataFrame()

    if df_itens_base.empty:
        out = df_manifestos_base.copy()
        out["mesorregiao_manifesto_m6"] = ""
        out["peso_itens_antes_m6"] = 0.0
        out["peso_auditoria_itens_antes_m6"] = 0.0
        out["vol_itens_antes_m6"] = 0.0
        out["km_itens_antes_m6"] = 0.0
        out["qtd_itens_recalculada_antes_m6"] = 0
        out["qtd_ctes_recalculada_antes_m6"] = 0
        out["qtd_paradas_recalculada_antes_m6"] = 0
        out["ocupacao_recalculada_antes_m6"] = 0.0
        return out

    def _mesorregiao_principal(series: pd.Series) -> str:
        vals = series.fillna("").astype(str).str.strip()
        vals = vals[vals != ""]
        if vals.empty:
            return ""
        moda = vals.mode()
        if moda.empty:
            return vals.iloc[0]
        return str(moda.iloc[0]).strip()

    agrup = (
        df_itens_base.groupby("manifesto_id", dropna=False)
        .agg(
            mesorregiao_manifesto_m6=("mesorregiao", _mesorregiao_principal),
            peso_itens_antes_m6=("peso_calculado", "sum"),
            peso_auditoria_itens_antes_m6=("peso_kg", "sum"),
            vol_itens_antes_m6=("vol_m3", "sum"),
            km_itens_antes_m6=("distancia_rodoviaria_est_km", "max"),
            qtd_itens_recalculada_antes_m6=("id_linha_pipeline", "count"),
            qtd_ctes_recalculada_antes_m6=("nro_documento", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_paradas_recalculada_antes_m6=("destinatario", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
        )
        .reset_index()
    )

    out = df_manifestos_base.merge(agrup, on="manifesto_id", how="left")

    for col in [
        "peso_itens_antes_m6",
        "peso_auditoria_itens_antes_m6",
        "vol_itens_antes_m6",
        "km_itens_antes_m6",
        "qtd_itens_recalculada_antes_m6",
        "qtd_ctes_recalculada_antes_m6",
        "qtd_paradas_recalculada_antes_m6",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    out["mesorregiao_manifesto_m6"] = out["mesorregiao_manifesto_m6"].fillna("").astype(str).str.strip()

    out["ocupacao_recalculada_antes_m6"] = 0.0
    mask_cap = pd.to_numeric(out["capacidade_peso_kg_veiculo"], errors="coerce").fillna(0.0) > 0
    out.loc[mask_cap, "ocupacao_recalculada_antes_m6"] = (
        out.loc[mask_cap, "peso_itens_antes_m6"] / out.loc[mask_cap, "capacidade_peso_kg_veiculo"] * 100.0
    )

    out["delta_peso_base_vs_itens_antes_m6"] = (out["peso_base_antes_m6"] - out["peso_itens_antes_m6"]).round(3)
    out["delta_km_base_vs_itens_antes_m6"] = (out["km_base_antes_m6"] - out["km_itens_antes_m6"]).round(3)

    return out.reset_index(drop=True)


def _aplicar_score_criticidade_por_mesorregiao(
    df_estatisticas_manifestos_antes_m6: pd.DataFrame,
) -> pd.DataFrame:
    if df_estatisticas_manifestos_antes_m6 is None or df_estatisticas_manifestos_antes_m6.empty:
        return pd.DataFrame()

    df = df_estatisticas_manifestos_antes_m6.copy()

    df["ocupacao_recalculada_antes_m6"] = pd.to_numeric(df["ocupacao_recalculada_antes_m6"], errors="coerce").fillna(0.0)
    df["km_itens_antes_m6"] = pd.to_numeric(df["km_itens_antes_m6"], errors="coerce").fillna(0.0)

    df["score_ocupacao_ruim_m6"] = 0.0
    df["score_km_ruim_m6"] = 0.0
    df["score_criticidade_m6"] = 0.0
    df["ranking_criticidade_m6"] = 0

    partes: List[pd.DataFrame] = []

    for _, grupo in df.groupby("mesorregiao_manifesto_m6", dropna=False, sort=False):
        g = grupo.copy()

        ocup_min = float(g["ocupacao_recalculada_antes_m6"].min()) if not g.empty else 0.0
        ocup_max = float(g["ocupacao_recalculada_antes_m6"].max()) if not g.empty else 0.0
        km_min = float(g["km_itens_antes_m6"].min()) if not g.empty else 0.0
        km_max = float(g["km_itens_antes_m6"].max()) if not g.empty else 0.0

        if ocup_max > ocup_min:
            ocup_norm = (g["ocupacao_recalculada_antes_m6"] - ocup_min) / (ocup_max - ocup_min)
        else:
            ocup_norm = pd.Series([1.0] * len(g), index=g.index)

        if km_max > km_min:
            km_norm = (g["km_itens_antes_m6"] - km_min) / (km_max - km_min)
        else:
            km_norm = pd.Series([0.0] * len(g), index=g.index)

        g["score_ocupacao_ruim_m6"] = (1.0 - ocup_norm).clip(lower=0.0, upper=1.0)
        g["score_km_ruim_m6"] = km_norm.clip(lower=0.0, upper=1.0)

        g["score_criticidade_m6"] = (0.75 * g["score_ocupacao_ruim_m6"] + 0.25 * g["score_km_ruim_m6"]).round(6)

        g = g.sort_values(
            by=[
                "score_criticidade_m6",
                "score_ocupacao_ruim_m6",
                "score_km_ruim_m6",
                "ocupacao_recalculada_antes_m6",
                "km_itens_antes_m6",
                "manifesto_id",
            ],
            ascending=[False, False, False, True, False, True],
            kind="mergesort",
        ).reset_index(drop=True)

        g["ranking_criticidade_m6"] = range(1, len(g) + 1)
        partes.append(g)

    out = pd.concat(partes, ignore_index=True) if partes else df.copy()
    return out.reset_index(drop=True)


def _par_elegivel_por_faixa(r1: pd.Series, r2: pd.Series) -> Tuple[bool, str]:
    meso_1 = _safe_text(r1.get("mesorregiao_manifesto_m6"))
    meso_2 = _safe_text(r2.get("mesorregiao_manifesto_m6"))

    if meso_1 == "" or meso_2 == "":
        return False, "mesorregiao_ausente"
    if meso_1 != meso_2:
        return False, "mesorregiao_diferente"

    peso_1 = _safe_float(r1.get("peso_itens_antes_m6"), 0.0)
    peso_2 = _safe_float(r2.get("peso_itens_antes_m6"), 0.0)
    km_1 = _safe_float(r1.get("km_itens_antes_m6"), 0.0)
    km_2 = _safe_float(r2.get("km_itens_antes_m6"), 0.0)

    if peso_1 <= 0 or peso_2 <= 0:
        return False, "peso_invalido"
    if km_1 <= 0 or km_2 <= 0:
        return False, "km_invalido"

    maior_peso = max(peso_1, peso_2)
    menor_peso = min(peso_1, peso_2)
    maior_km = max(km_1, km_2)
    menor_km = min(km_1, km_2)

    if menor_peso <= 0:
        return False, "peso_invalido"
    if menor_km <= 0:
        return False, "km_invalido"

    razao_peso = maior_peso / menor_peso
    razao_km = maior_km / menor_km

    if razao_peso > 3.5:
        return False, "faixa_peso_muito_distante"
    if razao_km > 2.5:
        return False, "faixa_km_muito_distante"

    return True, "par_elegivel"


def _gerar_pares_elegiveis_otimizacao(df_manifestos_scored_m6: pd.DataFrame) -> pd.DataFrame:
    if df_manifestos_scored_m6 is None or df_manifestos_scored_m6.empty:
        return pd.DataFrame()

    registros: List[Dict[str, Any]] = []

    for meso, grupo in df_manifestos_scored_m6.groupby("mesorregiao_manifesto_m6", dropna=False, sort=False):
        g = grupo.copy().sort_values(
            by=["ranking_criticidade_m6", "score_criticidade_m6", "ocupacao_recalculada_antes_m6"],
            ascending=[True, False, True],
            kind="mergesort",
        ).reset_index(drop=True)

        if len(g) < 2:
            continue

        top_criticos = max(1, int(round(len(g) * 0.5)))
        top_criticos = min(top_criticos, len(g))

        for i in range(top_criticos):
            row_i = g.iloc[i]
            candidatos_j = g.drop(index=i).reset_index(drop=True)
            candidatos_j = candidatos_j.sort_values(
                by=["score_criticidade_m6", "ocupacao_recalculada_antes_m6", "km_itens_antes_m6"],
                ascending=[False, True, False],
                kind="mergesort",
            ).reset_index(drop=True).head(5)

            for _, row_j in candidatos_j.iterrows():
                id_a = _safe_text(row_i.get("manifesto_id"))
                id_b = _safe_text(row_j.get("manifesto_id"))

                if id_a == "" or id_b == "" or id_a == id_b:
                    continue

                manifesto_id_a, manifesto_id_b = sorted([id_a, id_b])
                elegivel, motivo = _par_elegivel_por_faixa(row_i, row_j)
                if not elegivel:
                    continue

                score_par = round(
                    _safe_float(row_i.get("score_criticidade_m6"), 0.0)
                    + _safe_float(row_j.get("score_criticidade_m6"), 0.0),
                    6,
                )

                registros.append(
                    {
                        "mesorregiao_manifesto_m6": meso,
                        "manifesto_id_a": manifesto_id_a,
                        "manifesto_id_b": manifesto_id_b,
                        "origem_manifesto_modulo_a": _safe_text(row_i.get("origem_manifesto_modulo")),
                        "origem_manifesto_modulo_b": _safe_text(row_j.get("origem_manifesto_modulo")),
                        "veiculo_tipo_a": _safe_text(row_i.get("veiculo_tipo")),
                        "veiculo_tipo_b": _safe_text(row_j.get("veiculo_tipo")),
                        "veiculo_perfil_a": _safe_text(row_i.get("veiculo_perfil")),
                        "veiculo_perfil_b": _safe_text(row_j.get("veiculo_perfil")),
                        "veiculo_exclusivo_flag_a": bool(row_i.get("veiculo_exclusivo_flag", False)),
                        "veiculo_exclusivo_flag_b": bool(row_j.get("veiculo_exclusivo_flag", False)),
                        "ocupacao_antes_a": round(_safe_float(row_i.get("ocupacao_recalculada_antes_m6"), 0.0), 3),
                        "ocupacao_antes_b": round(_safe_float(row_j.get("ocupacao_recalculada_antes_m6"), 0.0), 3),
                        "km_antes_a": round(_safe_float(row_i.get("km_itens_antes_m6"), 0.0), 3),
                        "km_antes_b": round(_safe_float(row_j.get("km_itens_antes_m6"), 0.0), 3),
                        "peso_antes_a": round(_safe_float(row_i.get("peso_itens_antes_m6"), 0.0), 3),
                        "peso_antes_b": round(_safe_float(row_j.get("peso_itens_antes_m6"), 0.0), 3),
                        "score_criticidade_a": round(_safe_float(row_i.get("score_criticidade_m6"), 0.0), 6),
                        "score_criticidade_b": round(_safe_float(row_j.get("score_criticidade_m6"), 0.0), 6),
                        "score_par_prioridade_m6": score_par,
                        "motivo_elegibilidade_par_m6": motivo,
                    }
                )

    out = pd.DataFrame(registros)
    if out.empty:
        return out

    out = out.drop_duplicates(subset=["mesorregiao_manifesto_m6", "manifesto_id_a", "manifesto_id_b"], keep="first").copy()
    out = out.sort_values(
        by=["score_par_prioridade_m6", "ocupacao_antes_a", "ocupacao_antes_b", "km_antes_a", "km_antes_b"],
        ascending=[False, True, True, False, False],
        kind="mergesort",
    ).reset_index(drop=True)
    out["ordem_prioridade_par_m6"] = range(1, len(out) + 1)
    return out


def executar_m6_1_consolidacao_manifestos(
    df_manifestos_m4: Optional[pd.DataFrame] = None,
    df_itens_manifestados_m4: Optional[pd.DataFrame] = None,
    df_premanifestos_m5_2: Optional[pd.DataFrame] = None,
    df_itens_premanifestos_m5_2: Optional[pd.DataFrame] = None,
    df_premanifestos_m5_3: Optional[pd.DataFrame] = None,
    df_itens_premanifestos_m5_3: Optional[pd.DataFrame] = None,
    df_premanifestos_m5_4: Optional[pd.DataFrame] = None,
    df_itens_premanifestos_m5_4: Optional[pd.DataFrame] = None,
    data_base_roteirizacao: Optional[Any] = None,
    tipo_roteirizacao: str = "carteira",
    caminhos_pipeline: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    del kwargs

    blocos_manifestos = [
        _padronizar_manifestos(df_manifestos_m4, "M4", "manifesto_fechado"),
        _padronizar_manifestos(df_premanifestos_m5_2, "M5.2", "pre_manifesto_cidade"),
        _padronizar_manifestos(df_premanifestos_m5_3, "M5.3B", "pre_manifesto_subregiao"),
        _padronizar_manifestos(df_premanifestos_m5_4, "M5.4B", "pre_manifesto_mesorregiao"),
    ]

    blocos_itens = [
        _padronizar_itens_manifestados(df_itens_manifestados_m4, "M4", "manifesto_fechado"),
        _padronizar_itens_manifestados(df_itens_premanifestos_m5_2, "M5.2", "pre_manifesto_cidade"),
        _padronizar_itens_manifestados(df_itens_premanifestos_m5_3, "M5.3B", "pre_manifesto_subregiao"),
        _padronizar_itens_manifestados(df_itens_premanifestos_m5_4, "M5.4B", "pre_manifesto_mesorregiao"),
    ]

    df_manifestos_base_m6 = (
        pd.concat([df for df in blocos_manifestos if df is not None and not df.empty], ignore_index=True)
        if any(df is not None and not df.empty for df in blocos_manifestos)
        else pd.DataFrame()
    )

    df_itens_manifestos_base_m6 = (
        pd.concat([df for df in blocos_itens if df is not None and not df.empty], ignore_index=True)
        if any(df is not None and not df.empty for df in blocos_itens)
        else pd.DataFrame()
    )

    if not df_manifestos_base_m6.empty:
        df_manifestos_base_m6 = df_manifestos_base_m6.drop_duplicates(subset=["manifesto_id"], keep="first").reset_index(drop=True)

    if not df_itens_manifestos_base_m6.empty:
        df_itens_manifestos_base_m6 = df_itens_manifestos_base_m6.drop_duplicates(
            subset=["manifesto_id", "id_linha_pipeline"], keep="first"
        ).reset_index(drop=True)

    df_manifestos_base_m6 = _recompor_exclusividade_manifestos_m4(
        df_manifestos_base_m6=df_manifestos_base_m6,
        df_itens_manifestos_base_m6=df_itens_manifestos_base_m6,
    )

    df_estatisticas_manifestos_antes_m6 = _estatisticas_manifestos_antes(
        df_manifestos_base=df_manifestos_base_m6,
        df_itens_base=df_itens_manifestos_base_m6,
    )

    df_manifestos_scored_m6 = _aplicar_score_criticidade_por_mesorregiao(df_estatisticas_manifestos_antes_m6)
    df_pares_elegiveis_otimizacao_m6 = _gerar_pares_elegiveis_otimizacao(df_manifestos_scored_m6)

    resumo_m6_1 = {
        "modulo": "M6.1",
        "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
        "tipo_roteirizacao": tipo_roteirizacao,
        "manifestos_base_total_m6": int(len(df_manifestos_base_m6)),
        "itens_manifestos_base_total_m6": int(len(df_itens_manifestos_base_m6)),
        "pares_elegiveis_otimizacao_m6": int(len(df_pares_elegiveis_otimizacao_m6)),
        "manifestos_exclusivos_base_m6": int(df_manifestos_base_m6["veiculo_exclusivo_flag"].fillna(False).astype(bool).sum()) if not df_manifestos_base_m6.empty else 0,
        "mesorregioes_manifestos_base_m6": int(
            df_manifestos_scored_m6["mesorregiao_manifesto_m6"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
        ) if not df_manifestos_scored_m6.empty else 0,
        "fontes_consolidadas_m6": {
            "manifestos_m4": int(len(df_manifestos_m4)) if isinstance(df_manifestos_m4, pd.DataFrame) else 0,
            "itens_m4": int(len(df_itens_manifestados_m4)) if isinstance(df_itens_manifestados_m4, pd.DataFrame) else 0,
            "manifestos_m5_2": int(len(df_premanifestos_m5_2)) if isinstance(df_premanifestos_m5_2, pd.DataFrame) else 0,
            "itens_m5_2": int(len(df_itens_premanifestos_m5_2)) if isinstance(df_itens_premanifestos_m5_2, pd.DataFrame) else 0,
            "manifestos_m5_3": int(len(df_premanifestos_m5_3)) if isinstance(df_premanifestos_m5_3, pd.DataFrame) else 0,
            "itens_m5_3": int(len(df_itens_premanifestos_m5_3)) if isinstance(df_itens_premanifestos_m5_3, pd.DataFrame) else 0,
            "manifestos_m5_4": int(len(df_premanifestos_m5_4)) if isinstance(df_premanifestos_m5_4, pd.DataFrame) else 0,
            "itens_m5_4": int(len(df_itens_premanifestos_m5_4)) if isinstance(df_itens_premanifestos_m5_4, pd.DataFrame) else 0,
        },
        "estrategia_m6_1": [
            "consolidacao_multiorigem_manifestos",
            "padronizacao_itens_e_manifestos",
            "recomposicao_exclusividade_m4_a_partir_dos_itens",
            "contrato_com_max_paradas_veiculo",
            "contrato_com_veiculo_exclusivo_flag",
            "fallback_veiculo_perfil_para_veiculo_tipo",
            "estatistica_antes_da_otimizacao",
            "score_criticidade_com_prioridade_ocupacao",
            "pares_somente_dentro_da_mesma_mesorregiao",
            "sem_otimizacao_nesta_etapa",
        ],
        "caminhos_pipeline": caminhos_pipeline or {},
    }

    auditoria_m6_1 = {
        "manifestos_por_origem": (
            df_manifestos_base_m6.groupby("origem_manifesto_modulo")["manifesto_id"].nunique().to_dict()
            if not df_manifestos_base_m6.empty else {}
        ),
        "itens_por_origem": (
            df_itens_manifestos_base_m6.groupby("origem_manifesto_modulo")["id_linha_pipeline"].count().to_dict()
            if not df_itens_manifestos_base_m6.empty else {}
        ),
        "mesorregioes_base_m6": (
            sorted(
                df_manifestos_scored_m6["mesorregiao_manifesto_m6"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
            )
            if not df_manifestos_scored_m6.empty else []
        ),
        "veiculos_tipos_base_m6": (
            sorted(df_manifestos_base_m6["veiculo_tipo"].fillna("").astype(str).str.strip().unique().tolist())
            if not df_manifestos_base_m6.empty else []
        ),
        "manifestos_exclusivos_m6": (
            df_manifestos_base_m6.loc[df_manifestos_base_m6["veiculo_exclusivo_flag"] == True, "manifesto_id"].astype(str).tolist()
            if not df_manifestos_base_m6.empty else []
        ),
        "textos_exclusivo_detectados_m6": (
            df_manifestos_base_m6.loc[
                df_manifestos_base_m6["veiculo_exclusivo_flag"] == True,
                ["manifesto_id", "texto_exclusivo_detectado_m6"],
            ].to_dict(orient="records")
            if not df_manifestos_base_m6.empty else []
        ),
        "recomposicao_exclusivo_m4": (
            df_manifestos_base_m6.loc[
                df_manifestos_base_m6["origem_manifesto_modulo"] == "M4",
                [
                    "manifesto_id",
                    "veiculo_exclusivo_flag",
                    "veiculo_exclusivo_recomposto_m4",
                    "veiculo_exclusivo_flag_itens_m4",
                    "origem_etapa_4b1_exclusivo_m4",
                ],
            ].fillna(False).to_dict(orient="records")
            if not df_manifestos_base_m6.empty and "veiculo_exclusivo_recomposto_m4" in df_manifestos_base_m6.columns
            else []
        ),
        "max_paradas_veiculo_por_manifesto": (
            df_manifestos_base_m6.set_index("manifesto_id")["max_paradas_veiculo"].to_dict()
            if not df_manifestos_base_m6.empty else {}
        ),
    }

    outputs = {
        "df_manifestos_base_m6": df_manifestos_base_m6,
        "df_itens_manifestos_base_m6": df_itens_manifestos_base_m6,
        "df_estatisticas_manifestos_antes_m6": df_manifestos_scored_m6,
        "df_pares_elegiveis_otimizacao_m6": df_pares_elegiveis_otimizacao_m6,
    }

    meta = {
        "resumo_m6_1": resumo_m6_1,
        "auditoria_m6_1": auditoria_m6_1,
    }

    return outputs, meta


def executar_m6_consolidacao_manifestos(*args: Any, **kwargs: Any):
    return executar_m6_1_consolidacao_manifestos(*args, **kwargs)


def processar_m6_1_consolidacao_manifestos(*args: Any, **kwargs: Any):
    return executar_m6_1_consolidacao_manifestos(*args, **kwargs)


def rodar_m6_1_consolidacao_manifestos(*args: Any, **kwargs: Any):
    return executar_m6_1_consolidacao_manifestos(*args, **kwargs)
