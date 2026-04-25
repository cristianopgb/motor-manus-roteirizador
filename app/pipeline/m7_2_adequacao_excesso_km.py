from __future__ import annotations

import math
import traceback
from itertools import combinations
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

FATOR_KM_RODOVIARIO_PADRAO = 1.20


def _safe_len(df: Any) -> int:
    try:
        return int(len(df))
    except Exception:
        return 0


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


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


def _dist_km(lat1: float, lon1: float, lat2: float, lon2: float, fator: float) -> float:
    return _haversine_km(lat1, lon1, lat2, lon2) * max(_safe_float(fator, FATOR_KM_RODOVIARIO_PADRAO), 0.1)


def _inferir_fator_km(df: pd.DataFrame) -> float:
    if "fator_km_rodoviario_m7" in df.columns:
        val = _safe_float(df["fator_km_rodoviario_m7"].iloc[0], 0.0)
        if val > 0:
            return val
    ratios: List[float] = []
    if "distancia_rodoviaria_est_km" in df.columns:
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


def _peso_operacional_serie(df: pd.DataFrame) -> pd.Series:
    if "peso_calculado" in df.columns:
        s = pd.to_numeric(df["peso_calculado"], errors="coerce")
        if float(s.fillna(0).sum()) > 0:
            return s.fillna(0)
    if "peso_kg" in df.columns:
        return pd.to_numeric(df["peso_kg"], errors="coerce").fillna(0)
    return pd.Series([0.0] * len(df), index=df.index)


def _cidade_stats(df: pd.DataFrame, cidade_filial: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["cidade", "distancia", "peso", "docs"])

    pes = _peso_operacional_serie(df)
    ref = df.copy()
    ref["_peso_operacional"] = pes
    if "distancia_rodoviaria_est_km" in ref.columns:
        ref["_dist_ref"] = pd.to_numeric(ref["distancia_rodoviaria_est_km"], errors="coerce")
    else:
        ref["_dist_ref"] = 0.0

    out = ref.groupby("cidade", dropna=False).agg(
        distancia=("_dist_ref", "mean"),
        peso=("_peso_operacional", "sum"),
        docs=("nro_documento", "nunique"),
    ).reset_index()
    out["cidade_norm"] = out["cidade"].fillna("").astype(str).str.upper().str.strip()
    if cidade_filial:
        out = out.loc[~out["cidade_norm"].eq(cidade_filial.upper())].copy()
    out = out.sort_values(by=["distancia", "peso", "docs", "cidade_norm"], ascending=[False, True, True, True], kind="mergesort")
    return out


def _aplicar_remocao(df_manifesto: pd.DataFrame, cidades_removidas: Sequence[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not cidades_removidas:
        return df_manifesto.copy(), df_manifesto.iloc[0:0].copy()
    removidas = set(str(c) for c in cidades_removidas)
    mask = df_manifesto["cidade"].astype(str).isin(removidas)
    return df_manifesto.loc[~mask].copy(), df_manifesto.loc[mask].copy()


def _sequenciar_manifesto_restante(df: pd.DataFrame, cidade_filial: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = df.copy().reset_index(drop=True)
    if "ordem_entrega_doc_m7_1" in work.columns:
        work["_ordem_base"] = pd.to_numeric(work["ordem_entrega_doc_m7_1"], errors="coerce").fillna(range(1, len(work) + 1))
    else:
        work["_ordem_base"] = range(1, len(work) + 1)

    work["_cidade_norm"] = work["cidade"].fillna("").astype(str).str.upper().str.strip()

    cidade_rows = []
    for cn, gc in work.groupby("_cidade_norm", dropna=False):
        gc2 = gc.sort_values(by=["_ordem_base"], kind="mergesort")
        cidade_rows.append(
            {
                "_cidade_norm": cn,
                "_cidade_nome": _safe_text(gc2["cidade"].iloc[0]),
                "lat": _safe_float(gc2.iloc[0].get("latitude_dest_m7"), float("nan")),
                "lon": _safe_float(gc2.iloc[0].get("longitude_dest_m7"), float("nan")),
            }
        )
    df_cidades = pd.DataFrame(cidade_rows)

    origem_lat = _safe_float(work.iloc[0].get("latitude_filial_m7"), float("nan"))
    origem_lon = _safe_float(work.iloc[0].get("longitude_filial_m7"), float("nan"))

    restantes = df_cidades.copy()
    ordem_cidades: List[str] = []

    if cidade_filial:
        cand = restantes.loc[restantes["_cidade_norm"].eq(cidade_filial.upper())]
        if not cand.empty:
            escolhido = cand.iloc[0]
            ordem_cidades.append(str(escolhido["_cidade_norm"]))
            restantes = restantes.loc[~restantes["_cidade_norm"].eq(escolhido["_cidade_norm"])].copy()
            origem_lat, origem_lon = float(escolhido["lat"]), float(escolhido["lon"])

    while not restantes.empty:
        melhores: List[Tuple[float, str, float, float]] = []
        for _, r in restantes.iterrows():
            d = _dist_km(origem_lat, origem_lon, _safe_float(r["lat"], float("nan")), _safe_float(r["lon"], float("nan")), 1.0)
            melhores.append((d, str(r["_cidade_norm"]), _safe_float(r["lat"], float("nan")), _safe_float(r["lon"], float("nan"))))
        melhores.sort(key=lambda x: (x[0], x[1]))
        _, cid, lat, lon = melhores[0]
        ordem_cidades.append(cid)
        origem_lat, origem_lon = lat, lon
        restantes = restantes.loc[~restantes["_cidade_norm"].eq(cid)].copy()

    blocos = []
    for cid in ordem_cidades:
        gc = work.loc[work["_cidade_norm"].eq(cid)].copy().sort_values(by=["_ordem_base"], kind="mergesort")
        blocos.append(gc)
    out = pd.concat(blocos, ignore_index=True) if blocos else work.iloc[0:0].copy()
    out["ordem_entrega_doc_m7_2"] = range(1, len(out) + 1)

    parada = []
    atual = 0
    anterior = None
    for c in out["_cidade_norm"].tolist():
        if c != anterior:
            atual += 1
            anterior = c
        parada.append(atual)
    out["ordem_parada_m7_2"] = parada
    return out


def _calcular_km_rota(df_manifesto: pd.DataFrame, fator: float) -> float:
    if df_manifesto.empty:
        return 0.0
    atual_lat = _safe_float(df_manifesto.iloc[0].get("latitude_filial_m7"), float("nan"))
    atual_lon = _safe_float(df_manifesto.iloc[0].get("longitude_filial_m7"), float("nan"))
    km = 0.0
    for _, row in df_manifesto.iterrows():
        dlat = _safe_float(row.get("latitude_dest_m7"), float("nan"))
        dlon = _safe_float(row.get("longitude_dest_m7"), float("nan"))
        if pd.isna(atual_lat) or pd.isna(atual_lon) or pd.isna(dlat) or pd.isna(dlon):
            continue
        km += _dist_km(atual_lat, atual_lon, dlat, dlon, fator)
        atual_lat, atual_lon = dlat, dlon
    return float(km)


def _calc_ocupacao(df: pd.DataFrame, row_manifesto: pd.Series) -> tuple[float, float]:
    peso = float(_peso_operacional_serie(df).sum())
    cap = _safe_float(row_manifesto.get("capacidade_peso_kg_veiculo"), 0.0)
    if cap > 0:
        return (peso / cap) * 100.0, peso
    base = _safe_float(row_manifesto.get("ocupacao_original_m7_1"), 0.0)
    base_peso = _safe_float(row_manifesto.get("peso_total_original_m7_1"), 0.0)
    if base_peso > 0:
        return base * (peso / base_peso), peso
    return base, peso


def executar_m7_2_adequacao_excesso_km(
    *,
    df_manifestos_reavaliados_m7_1: pd.DataFrame | None,
    df_itens_reavaliados_m7_1: pd.DataFrame | None,
    df_manifestos_acima_km_m7_1: pd.DataFrame | None,
    data_base_roteirizacao: Any = None,
    caminhos_pipeline: Dict[str, Any] | None = None,
    filial_contexto: Optional[Dict[str, Any]] = None,
) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    del filial_contexto
    df_manifestos = df_manifestos_reavaliados_m7_1.copy() if isinstance(df_manifestos_reavaliados_m7_1, pd.DataFrame) else pd.DataFrame()
    df_itens = df_itens_reavaliados_m7_1.copy() if isinstance(df_itens_reavaliados_m7_1, pd.DataFrame) else pd.DataFrame()
    df_acima = df_manifestos_acima_km_m7_1.copy() if isinstance(df_manifestos_acima_km_m7_1, pd.DataFrame) else pd.DataFrame()

    print("[M7.2] executando com:")
    print(f"- df_manifestos_acima_km_m7_1 linhas={_safe_len(df_acima)}")
    print(f"- df_itens_reavaliados_m7_1 linhas={_safe_len(df_itens)}")

    if df_itens.empty or df_acima.empty or "manifesto_id" not in df_itens.columns:
        vazios = {
            "df_manifestos_ajustados_m7_2": pd.DataFrame(),
            "df_itens_manifestos_finais_m7_2": df_itens.copy(),
            "df_itens_retirados_m7_2": pd.DataFrame(),
            "df_manifestos_desfeitos_m7_2": pd.DataFrame(),
            "df_excedente_final_m7_2": pd.DataFrame(),
            "df_tentativas_adequacao_m7_2": pd.DataFrame(),
        }
        return vazios, {"resumo_m7_2": {"total_manifestos_ajustados_m7_2": 0}, "data_base_roteirizacao": str(data_base_roteirizacao), "caminhos_pipeline": caminhos_pipeline or {}}

    acima_ids = set(df_acima["manifesto_id"].astype(str).tolist())
    mref = df_manifestos.copy()
    mref["_manifesto_key"] = mref["manifesto_id"].astype(str)

    tentativas: List[Dict[str, Any]] = []
    res_manifestos: List[Dict[str, Any]] = []
    finais: List[pd.DataFrame] = []
    retirados: List[pd.DataFrame] = []
    desfeitos: List[Dict[str, Any]] = []

    for manifesto_id, grupo in df_itens.groupby("manifesto_id", dropna=False):
        m_id = str(manifesto_id)
        g = grupo.copy().reset_index(drop=True)
        base = mref.loc[mref["_manifesto_key"].eq(m_id)].head(1)
        row_manifesto = base.iloc[0] if not base.empty else pd.Series(dtype=object)
        try:
            cidade_filial = _safe_text(row_manifesto.get("cidade_filial_m7_1")).upper()
            fator_km = _inferir_fator_km(g)

            if m_id not in acima_ids:
                g_keep = _sequenciar_manifesto_restante(g, cidade_filial)
                g_keep["status_item_m7_2"] = "mantido"
                finais.append(g_keep)
                continue

            max_km = _safe_float(row_manifesto.get("max_km_distancia_veiculo"), float("inf"))
            ocup_min = _safe_float(row_manifesto.get("ocupacao_minima_perc_veiculo"), 0.0)

            g_base = _sequenciar_manifesto_restante(g, cidade_filial)
            km_original = _calcular_km_rota(g_base, fator_km)
            ocup_original, _ = _calc_ocupacao(g_base, row_manifesto)

            stats_cidades = _cidade_stats(g_base, cidade_filial)
            candidatas = stats_cidades["cidade"].astype(str).tolist()

            melhor = None

            for cidade in candidatas:
                g_keep, g_remove = _aplicar_remocao(g_base, [cidade])
                g_keep = _sequenciar_manifesto_restante(g_keep, cidade_filial)
                km_new = _calcular_km_rota(g_keep, fator_km)
                ocup_new, _ = _calc_ocupacao(g_keep, row_manifesto)
                ok = (km_new <= max_km) and (ocup_new >= ocup_min)
                print(f"[M7.2] manifesto={m_id} tentativa=remocao_1_cidade cidades={[cidade]} km_resultante={km_new:.2f} ocupacao_resultante={ocup_new:.2f} aceito={ok}")
                tentativas.append({"manifesto_id": m_id, "tipo_tentativa": "remocao_1_cidade", "cidades_testadas": [cidade], "km_resultante": km_new, "ocupacao_resultante": ocup_new, "aceito": ok, "motivo": "avaliacao"})
                if ok:
                    melhor = ("remocao_1_cidade", [cidade], g_keep, g_remove, km_new, ocup_new)
                    break

            if melhor is None and candidatas:
                top4 = candidatas[:4]
                for par in combinations(top4, 2):
                    cidades_par = list(par)
                    g_keep, g_remove = _aplicar_remocao(g_base, cidades_par)
                    g_keep = _sequenciar_manifesto_restante(g_keep, cidade_filial)
                    km_new = _calcular_km_rota(g_keep, fator_km)
                    ocup_new, _ = _calc_ocupacao(g_keep, row_manifesto)
                    ok = (km_new <= max_km) and (ocup_new >= ocup_min)
                    print(f"[M7.2] manifesto={m_id} tentativa=remocao_2_cidades cidades={cidades_par} km_resultante={km_new:.2f} ocupacao_resultante={ocup_new:.2f} aceito={ok}")
                    tentativas.append({"manifesto_id": m_id, "tipo_tentativa": "remocao_2_cidades", "cidades_testadas": cidades_par, "km_resultante": km_new, "ocupacao_resultante": ocup_new, "aceito": ok, "motivo": "avaliacao"})
                    if ok:
                        melhor = ("remocao_2_cidades", cidades_par, g_keep, g_remove, km_new, ocup_new)
                        break

            if melhor is None:
                g_ret = g_base.copy()
                g_ret["status_item_m7_2"] = "retirado"
                g_ret["motivo_retirada_m7_2"] = "manifesto_desfeito_por_excesso_km_sem_solucao"
                retirados.append(g_ret)
                desfeitos.append({"manifesto_id": m_id, "manifesto_desfeito_m7_2": True, "motivo_ajuste_m7_2": "manifesto_desfeito_por_excesso_km_sem_solucao"})
                tentativas.append({"manifesto_id": m_id, "tipo_tentativa": "desfeito", "cidades_testadas": [], "km_resultante": None, "ocupacao_resultante": None, "aceito": False, "motivo": "sem_solucao"})
                print(f"[M7.2] manifesto={m_id} status_final=desfeito km_final=None ocupacao_final=None")
                res_manifestos.append({
                    "manifesto_id": m_id,
                    "status_final_m7_2": "desfeito",
                    "km_total_reavaliado_m7_1": km_original,
                    "km_total_final_m7_2": None,
                    "ocupacao_original_m7_2": ocup_original,
                    "ocupacao_final_m7_2": None,
                    "cidade_removida_m7_2": None,
                    "quantidade_cidades_removidas_m7_2": 0,
                    "manifesto_desfeito_m7_2": True,
                    "motivo_ajuste_m7_2": "manifesto_desfeito_por_excesso_km_sem_solucao",
                })
                continue

            _, cidades_rm, g_keep, g_remove, km_new, ocup_new = melhor
            g_keep["status_item_m7_2"] = "mantido"
            finais.append(g_keep)

            g_remove = g_remove.copy()
            g_remove["status_item_m7_2"] = "retirado"
            g_remove["motivo_retirada_m7_2"] = "remocao_cidade_por_excesso_km"
            retirados.append(g_remove)

            print(f"[M7.2] manifesto={m_id} status_final=ajustado km_final={km_new:.2f} ocupacao_final={ocup_new:.2f}")
            res_manifestos.append({
                "manifesto_id": m_id,
                "status_final_m7_2": "ajustado",
                "km_total_reavaliado_m7_1": km_original,
                "km_total_final_m7_2": km_new,
                "ocupacao_original_m7_2": ocup_original,
                "ocupacao_final_m7_2": ocup_new,
                "cidade_removida_m7_2": "|".join(cidades_rm),
                "quantidade_cidades_removidas_m7_2": len(cidades_rm),
                "manifesto_desfeito_m7_2": False,
                "motivo_ajuste_m7_2": "remocao_cidade_por_excesso_km",
            })
        except Exception as exc:
            mensagem_erro = f"{exc.__class__.__name__}: {exc}"
            print(f"[M7.2][ERRO] manifesto={m_id} erro={mensagem_erro}")
            tb = traceback.format_exc(limit=2)
            g_err = g.copy()
            g_err["status_item_m7_2"] = "retirado"
            g_err["motivo_retirada_m7_2"] = "erro_manifesto_m7_2"
            g_err["mensagem_erro_m7_2"] = mensagem_erro
            retirados.append(g_err)
            desfeitos.append(
                {
                    "manifesto_id": m_id,
                    "manifesto_desfeito_m7_2": True,
                    "motivo_ajuste_m7_2": "erro_manifesto_m7_2",
                    "mensagem_erro_m7_2": mensagem_erro,
                }
            )
            tentativas.append(
                {
                    "manifesto_id": m_id,
                    "tipo_tentativa": "erro_manifesto",
                    "cidades_testadas": [],
                    "km_resultante": None,
                    "ocupacao_resultante": None,
                    "aceito": False,
                    "motivo": mensagem_erro,
                    "mensagem_erro_m7_2": tb[:500],
                }
            )
            res_manifestos.append(
                {
                    "manifesto_id": m_id,
                    "status_final_m7_2": "desfeito",
                    "km_total_reavaliado_m7_1": None,
                    "km_total_final_m7_2": None,
                    "ocupacao_original_m7_2": None,
                    "ocupacao_final_m7_2": None,
                    "cidade_removida_m7_2": None,
                    "quantidade_cidades_removidas_m7_2": 0,
                    "manifesto_desfeito_m7_2": True,
                    "motivo_ajuste_m7_2": "erro_manifesto_m7_2",
                    "mensagem_erro_m7_2": mensagem_erro,
                }
            )

    df_manifestos_ajustados = pd.DataFrame(res_manifestos)
    df_itens_finais = pd.concat(finais, ignore_index=True) if finais else pd.DataFrame()
    df_itens_retirados = pd.concat(retirados, ignore_index=True) if retirados else pd.DataFrame()
    df_manifestos_desfeitos = pd.DataFrame(desfeitos)
    df_excedente = df_itens_retirados.copy()
    df_tentativas = pd.DataFrame(tentativas)

    print(f"[M7.2] df_manifestos_ajustados_m7_2 linhas={_safe_len(df_manifestos_ajustados)}")
    print(f"[M7.2] df_itens_manifestos_finais_m7_2 linhas={_safe_len(df_itens_finais)}")
    print(f"[M7.2] df_itens_retirados_m7_2 linhas={_safe_len(df_itens_retirados)}")
    print(f"[M7.2] df_manifestos_desfeitos_m7_2 linhas={_safe_len(df_manifestos_desfeitos)}")
    print(f"[M7.2] df_excedente_final_m7_2 linhas={_safe_len(df_excedente)}")
    print(f"[M7.2] df_tentativas_adequacao_m7_2 linhas={_safe_len(df_tentativas)}")

    outputs = {
        "df_manifestos_ajustados_m7_2": df_manifestos_ajustados,
        "df_itens_manifestos_finais_m7_2": df_itens_finais,
        "df_itens_retirados_m7_2": df_itens_retirados,
        "df_manifestos_desfeitos_m7_2": df_manifestos_desfeitos,
        "df_excedente_final_m7_2": df_excedente,
        "df_tentativas_adequacao_m7_2": df_tentativas,
    }
    meta = {
        "resumo_m7_2": {
            "total_manifestos_ajustados_m7_2": _safe_len(df_manifestos_ajustados),
            "total_itens_finais_m7_2": _safe_len(df_itens_finais),
            "total_itens_retirados_m7_2": _safe_len(df_itens_retirados),
            "total_manifestos_desfeitos_m7_2": _safe_len(df_manifestos_desfeitos),
            "total_excedente_final_m7_2": _safe_len(df_excedente),
            "total_tentativas_adequacao_m7_2": _safe_len(df_tentativas),
        },
        "data_base_roteirizacao": str(data_base_roteirizacao),
        "caminhos_pipeline": caminhos_pipeline or {},
    }
    return outputs, meta
