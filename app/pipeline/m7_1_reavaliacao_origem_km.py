from __future__ import annotations

import math
from typing import Any, Dict, List

import pandas as pd

FATOR_KM_RODOVIARIO_PADRAO = 1.20


def _safe_len(df: Any) -> int:
    try:
        return int(len(df))
    except Exception:
        return 0


def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _pick_col(df: pd.DataFrame, names: List[str], default: str = "") -> str:
    for c in names:
        if c in df.columns:
            return c
    return default


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if any(pd.isna(x) for x in [lat1, lon1, lat2, lon2]):
        return 0.0
    r = 6371.0
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _inferir_fator_km(df: pd.DataFrame) -> float:
    if "fator_km_rodoviario_m7" in df.columns:
        val = _safe_float(df["fator_km_rodoviario_m7"].iloc[0], 0.0)
        if val > 0:
            return val

    if "distancia_rodoviaria_est_km" not in df.columns:
        return FATOR_KM_RODOVIARIO_PADRAO

    ratios: List[float] = []
    for _, row in df.iterrows():
        lat_o = _safe_float(row.get("latitude_filial_m7"), float("nan"))
        lon_o = _safe_float(row.get("longitude_filial_m7"), float("nan"))
        lat_d = _safe_float(row.get("latitude_dest_m7"), float("nan"))
        lon_d = _safe_float(row.get("longitude_dest_m7"), float("nan"))
        dist_est = _safe_float(row.get("distancia_rodoviaria_est_km"), float("nan"))
        dist_hav = _haversine_km(lat_o, lon_o, lat_d, lon_d)
        if dist_est > 0 and dist_hav > 0:
            ratio = dist_est / dist_hav
            if 0.8 <= ratio <= 3.0:
                ratios.append(ratio)
    if not ratios:
        return FATOR_KM_RODOVIARIO_PADRAO
    ratios.sort()
    return float(ratios[len(ratios) // 2])


def _dist_km(lat1: float, lon1: float, lat2: float, lon2: float, fator: float) -> float:
    return _haversine_km(lat1, lon1, lat2, lon2) * max(_safe_float(fator, FATOR_KM_RODOVIARIO_PADRAO), 0.1)


def _calcular_km_rota(df_manifesto: pd.DataFrame, fator: float) -> tuple[float, float]:
    if df_manifesto.empty:
        return 0.0, 0.0

    km_docs = 0.0
    km_cidades = 0.0

    atual_lat = _safe_float(df_manifesto.iloc[0].get("latitude_filial_m7"), float("nan"))
    atual_lon = _safe_float(df_manifesto.iloc[0].get("longitude_filial_m7"), float("nan"))

    for _, row in df_manifesto.iterrows():
        dlat = _safe_float(row.get("latitude_dest_m7"), float("nan"))
        dlon = _safe_float(row.get("longitude_dest_m7"), float("nan"))
        if pd.isna(atual_lat) or pd.isna(atual_lon) or pd.isna(dlat) or pd.isna(dlon):
            continue
        km_docs += _dist_km(atual_lat, atual_lon, dlat, dlon, fator)
        atual_lat, atual_lon = dlat, dlon

    df_city = (
        df_manifesto.assign(_cidade_norm=df_manifesto.get("cidade", "").fillna("").astype(str).str.upper().str.strip())
        .drop_duplicates(subset=["_cidade_norm"], keep="first")
        .reset_index(drop=True)
    )
    atual_lat = _safe_float(df_manifesto.iloc[0].get("latitude_filial_m7"), float("nan"))
    atual_lon = _safe_float(df_manifesto.iloc[0].get("longitude_filial_m7"), float("nan"))
    for _, row in df_city.iterrows():
        dlat = _safe_float(row.get("latitude_dest_m7"), float("nan"))
        dlon = _safe_float(row.get("longitude_dest_m7"), float("nan"))
        if pd.isna(atual_lat) or pd.isna(atual_lon) or pd.isna(dlat) or pd.isna(dlon):
            continue
        km_cidades += _dist_km(atual_lat, atual_lon, dlat, dlon, fator)
        atual_lat, atual_lon = dlat, dlon

    return float(km_docs), float(km_cidades)


def _ordem_parada_por_bloco_cidade(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=int)
    cidade_norm = df.get("cidade", "").fillna("").astype(str).str.upper().str.strip().tolist()
    ordens: List[int] = []
    atual = 0
    anterior = None
    for c in cidade_norm:
        if c != anterior:
            atual += 1
            anterior = c
        ordens.append(atual)
    return pd.Series(ordens, index=df.index)


def _cidade_filial_primeiro_bloco(mask_filial: pd.Series) -> bool:
    if not bool(mask_filial.any()):
        return False
    vals = mask_filial.astype(bool).tolist()
    fim_bloco = 0
    while fim_bloco < len(vals) and vals[fim_bloco]:
        fim_bloco += 1
    return not any(vals[fim_bloco:])


def _peso_operacional_total(df: pd.DataFrame) -> float:
    if "peso_calculado" in df.columns:
        s = pd.to_numeric(df["peso_calculado"], errors="coerce").fillna(0)
        if float(s.sum()) > 0:
            return float(s.sum())
    if "peso_kg" in df.columns:
        return float(pd.to_numeric(df["peso_kg"], errors="coerce").fillna(0).sum())
    return 0.0


def executar_m7_1_reavaliacao_origem_km(
    *,
    df_manifestos_m7: pd.DataFrame | None,
    df_itens_sequenciados_m7: pd.DataFrame | None,
    df_resumo_sequenciamento_m7: pd.DataFrame | None,
    data_base_roteirizacao: Any = None,
    caminhos_pipeline: Dict[str, Any] | None = None,
) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    df_manifestos = df_manifestos_m7.copy() if isinstance(df_manifestos_m7, pd.DataFrame) else pd.DataFrame()
    df_itens = df_itens_sequenciados_m7.copy() if isinstance(df_itens_sequenciados_m7, pd.DataFrame) else pd.DataFrame()
    _ = df_resumo_sequenciamento_m7.copy() if isinstance(df_resumo_sequenciamento_m7, pd.DataFrame) else pd.DataFrame()

    print("[M7.1] executando com:")
    print(f"- df_manifestos_m7 linhas={_safe_len(df_manifestos)}")
    print(f"- df_itens_sequenciados_m7 linhas={_safe_len(df_itens)}")

    if df_itens.empty or "manifesto_id" not in df_itens.columns:
        vazios = {
            "df_manifestos_reavaliados_m7_1": pd.DataFrame(),
            "df_itens_reavaliados_m7_1": df_itens.copy(),
            "df_manifestos_acima_km_m7_1": pd.DataFrame(),
            "df_tentativas_reavaliacao_m7_1": pd.DataFrame(),
        }
        return vazios, {"resumo_m7_1": {"manifestos_processados": 0}, "data_base_roteirizacao": str(data_base_roteirizacao), "caminhos_pipeline": caminhos_pipeline or {}}

    col_ordem_doc = _pick_col(df_itens, ["ordem_entrega_doc_m7", "sequencia_entrega"], "ordem_entrega_doc_m7")
    col_origem_manifesto = _pick_col(df_manifestos, ["origem_cidade", "filial_cidade", "cidade_filial", "cidade_origem"])

    if not df_manifestos.empty and "manifesto_id" in df_manifestos.columns:
        manifestos_idx = df_manifestos.copy()
        manifestos_idx["_manifesto_key"] = manifestos_idx["manifesto_id"].astype(str)
    else:
        manifestos_idx = pd.DataFrame(columns=["_manifesto_key"])

    manifestos_resumo: List[Dict[str, Any]] = []
    tentativas: List[Dict[str, Any]] = []
    itens_reavaliados: List[pd.DataFrame] = []

    for manifesto_id, grupo in df_itens.groupby("manifesto_id", dropna=False):
        g = grupo.copy().reset_index(drop=True)
        g["_ordem_base"] = pd.to_numeric(g.get(col_ordem_doc), errors="coerce")
        g["_ordem_base"] = g["_ordem_base"].fillna(pd.Series(range(1, len(g) + 1), index=g.index))
        g = g.sort_values(by=["_ordem_base"], kind="mergesort").reset_index(drop=True)

        m_key = str(manifesto_id)
        linha_manifesto = manifestos_idx.loc[manifestos_idx["_manifesto_key"] == m_key].head(1)

        cidade_filial = ""
        if not linha_manifesto.empty and col_origem_manifesto:
            cidade_filial = _safe_text(linha_manifesto.iloc[0].get(col_origem_manifesto)).upper()

        g["_cidade_norm"] = g.get("cidade", "").fillna("").astype(str).str.upper().str.strip()
        mask_filial_before = g["_cidade_norm"].eq(cidade_filial) if cidade_filial else pd.Series([False] * len(g), index=g.index)
        possui_cidade_filial = bool(mask_filial_before.any())
        primeiro_bloco_antes = _cidade_filial_primeiro_bloco(mask_filial_before)

        reordenado = False
        if possui_cidade_filial and not primeiro_bloco_antes:
            g_filial = g.loc[mask_filial_before].copy()
            g_outros = g.loc[~mask_filial_before].copy()
            g = pd.concat([g_filial, g_outros], ignore_index=True)
            reordenado = True

        mask_filial_after = g["_cidade_norm"].eq(cidade_filial) if cidade_filial else pd.Series([False] * len(g), index=g.index)
        primeiro_bloco_depois = _cidade_filial_primeiro_bloco(mask_filial_after)

        g["ordem_entrega_doc_m7_1"] = range(1, len(g) + 1)
        g["ordem_parada_m7_1"] = _ordem_parada_por_bloco_cidade(g)
        g["flag_cidade_filial_m7_1"] = mask_filial_after
        g["flag_reordenado_origem_m7_1"] = bool(reordenado)

        fator = _inferir_fator_km(g)
        km_reavaliado, km_cidades_reavaliado = _calcular_km_rota(g, fator)
        g["km_total_reavaliado_m7_1"] = km_reavaliado
        g["km_total_cidades_reavaliado_m7_1"] = km_cidades_reavaliado

        km_original = 0.0
        if not linha_manifesto.empty:
            for c in ["km_total_sequencia_paradas_m7", "km_total_sequencia_cidades_m7", "km_referencia", "km_final_m6_2"]:
                if c in linha_manifesto.columns and pd.notna(linha_manifesto.iloc[0].get(c)):
                    km_original = _safe_float(linha_manifesto.iloc[0].get(c), 0.0)
                    if km_original > 0:
                        break

        max_km = _safe_float(linha_manifesto.iloc[0].get("max_km_distancia_veiculo"), float("inf")) if not linha_manifesto.empty else float("inf")
        status_km = "ok" if km_reavaliado <= max_km else "acima_limite"

        peso_total_original = _peso_operacional_total(g)
        cap_peso = _safe_float(linha_manifesto.iloc[0].get("capacidade_peso_kg_veiculo"), 0.0) if not linha_manifesto.empty else 0.0
        ocupacao_original_m7_1 = (peso_total_original / cap_peso * 100.0) if cap_peso > 0 else _safe_float(
            linha_manifesto.iloc[0].get("ocupacao_oficial_perc"), _safe_float(linha_manifesto.iloc[0].get("ocupacao_final_m6_2"), 0.0)
        ) if not linha_manifesto.empty else 0.0

        print(
            f"[M7.1] manifesto={manifesto_id} cidade_filial={cidade_filial} "
            f"reordenado={reordenado} km_original={km_original:.2f} km_reavaliado={km_reavaliado:.2f}"
        )

        resumo = {
            "manifesto_id": manifesto_id,
            "veiculo_perfil": linha_manifesto.iloc[0].get("veiculo_perfil") if not linha_manifesto.empty else None,
            "veiculo_tipo": linha_manifesto.iloc[0].get("veiculo_tipo") if not linha_manifesto.empty else None,
            "max_km_distancia_veiculo": max_km if max_km != float("inf") else None,
            "capacidade_peso_kg_veiculo": linha_manifesto.iloc[0].get("capacidade_peso_kg_veiculo") if not linha_manifesto.empty else None,
            "capacidade_vol_m3_veiculo": linha_manifesto.iloc[0].get("capacidade_vol_m3_veiculo") if not linha_manifesto.empty else None,
            "ocupacao_minima_perc_veiculo": linha_manifesto.iloc[0].get("ocupacao_minima_perc_veiculo") if not linha_manifesto.empty else None,
            "ocupacao_maxima_perc_veiculo": linha_manifesto.iloc[0].get("ocupacao_maxima_perc_veiculo") if not linha_manifesto.empty else None,
            "ocupacao_original_m7_1": ocupacao_original_m7_1,
            "peso_total_original_m7_1": peso_total_original,
            "cidade_filial_m7_1": cidade_filial,
            "possui_cidade_filial_no_manifesto_m7_1": possui_cidade_filial,
            "cidade_filial_foi_primeiro_bloco_m7_1": primeiro_bloco_depois if possui_cidade_filial else False,
            "flag_reordenado_origem_m7_1": reordenado,
            "km_total_original_m7": km_original,
            "km_total_reavaliado_m7_1": km_reavaliado,
            "diferenca_km_m7_1": km_reavaliado - km_original,
            "status_km_m7_1": status_km,
        }
        manifestos_resumo.append(resumo)
        tentativas.append(
            {
                "manifesto_id": manifesto_id,
                "cidade_filial": cidade_filial,
                "possui_cidade_filial": possui_cidade_filial,
                "reordenado": reordenado,
                "km_original": km_original,
                "km_reavaliado": km_reavaliado,
                "status_final": status_km,
            }
        )
        itens_reavaliados.append(g)

    df_manifestos_reavaliados = pd.DataFrame(manifestos_resumo)
    df_itens_reavaliados = pd.concat(itens_reavaliados, ignore_index=True) if itens_reavaliados else pd.DataFrame()
    df_manifestos_acima_km = df_manifestos_reavaliados.loc[df_manifestos_reavaliados["status_km_m7_1"].eq("acima_limite")].reset_index(drop=True) if not df_manifestos_reavaliados.empty else pd.DataFrame()
    df_tentativas = pd.DataFrame(tentativas)

    print(f"[M7.1] df_manifestos_reavaliados_m7_1 linhas={_safe_len(df_manifestos_reavaliados)}")
    print(f"[M7.1] df_itens_reavaliados_m7_1 linhas={_safe_len(df_itens_reavaliados)}")
    print(f"[M7.1] df_manifestos_acima_km_m7_1 linhas={_safe_len(df_manifestos_acima_km)}")
    print(f"[M7.1] df_tentativas_reavaliacao_m7_1 linhas={_safe_len(df_tentativas)}")

    outputs = {
        "df_manifestos_reavaliados_m7_1": df_manifestos_reavaliados,
        "df_itens_reavaliados_m7_1": df_itens_reavaliados,
        "df_manifestos_acima_km_m7_1": df_manifestos_acima_km,
        "df_tentativas_reavaliacao_m7_1": df_tentativas,
    }
    meta = {
        "resumo_m7_1": {
            "total_manifestos_reavaliados_m7_1": _safe_len(df_manifestos_reavaliados),
            "total_itens_reavaliados_m7_1": _safe_len(df_itens_reavaliados),
            "total_manifestos_acima_km_m7_1": _safe_len(df_manifestos_acima_km),
            "total_tentativas_reavaliacao_m7_1": _safe_len(df_tentativas),
        },
        "data_base_roteirizacao": str(data_base_roteirizacao),
        "caminhos_pipeline": caminhos_pipeline or {},
    }
    return outputs, meta
