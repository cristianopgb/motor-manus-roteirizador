# ============================================================
# MÓDULO 1 — LIMPEZA, PADRONIZAÇÃO E TIPAGEM
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────
# HELPERS DE TEXTO
# ─────────────────────────────────────────────

def _safe_nan(valor: Any) -> bool:
    try:
        return bool(pd.isna(valor))
    except Exception:
        return False


def normalizar_texto_basico(valor: Any) -> Any:
    if valor is None:
        return np.nan
    if _safe_nan(valor):
        return np.nan
    texto = str(valor).replace("\u00a0", " ").strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto if texto != "" else np.nan


def remover_acentos(texto: Any) -> str:
    if texto is None or _safe_nan(texto):
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


def padronizar_nome_coluna(col: Any) -> str:
    col = str(col).replace("\u00a0", " ").strip()
    col = remover_acentos(col).lower()
    for src, dst in [("/", "_"), (".", ""), ("-", "_"), ("(", "_"), (")", "_"), ("%", "perc")]:
        col = col.replace(src, dst)
    col = re.sub(r"[^a-z0-9_]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def garantir_colunas_unicas(colunas: List[str]) -> List[str]:
    novas: List[str] = []
    contador: Dict[str, int] = {}
    for c in colunas:
        if c not in contador:
            contador[c] = 0
            novas.append(c)
        else:
            contador[c] += 1
            novas.append(f"{c}_{contador[c]}")
    return novas


def normalizar_chave_texto(serie: pd.Series) -> pd.Series:
    return serie.apply(
        lambda x: remover_acentos(str(x)).upper().strip() if pd.notna(x) else ""
    )


# ─────────────────────────────────────────────
# CONVERSORES DE TIPO
# ─────────────────────────────────────────────

def converter_numerico_brasil(serie: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(serie):
        return pd.to_numeric(serie, errors="coerce")

    def _conv(x: Any) -> float:
        if x is None or _safe_nan(x):
            return np.nan
        x = str(x).replace("R$", "").replace(" ", "")
        if "." in x and "," in x:
            if x.rfind(",") > x.rfind("."):
                x = x.replace(".", "").replace(",", ".")
            else:
                x = x.replace(",", "")
        elif "," in x:
            x = x.replace(",", ".")
        return pd.to_numeric(x, errors="coerce")

    return serie.astype(str).str.strip().apply(_conv)


def converter_coordenada(serie: pd.Series) -> pd.Series:
    def _coord(x: Any) -> float:
        if x is None or _safe_nan(x):
            return np.nan
        x = str(x).replace(" ", "")
        if "." in x and "," in x:
            if x.rfind(",") > x.rfind("."):
                x = x.replace(".", "").replace(",", ".")
            else:
                x = x.replace(",", "")
        elif "," in x:
            x = x.replace(",", ".")
        return pd.to_numeric(x, errors="coerce")

    return serie.astype(str).str.strip().apply(_coord)


def converter_data(serie: pd.Series) -> pd.Series:
    def _limpar(x: Any) -> Any:
        if x is None or _safe_nan(x):
            return np.nan
        if isinstance(x, (pd.Timestamp, datetime)):
            return x
        texto = str(x).strip()
        if texto.lower() in {"", "-", "--", "null", "none", "nan", "nat", "n/a", "na"}:
            return np.nan
        return texto

    serie_limpa = serie.apply(_limpar)
    convertido = pd.to_datetime(serie_limpa, errors="coerce", dayfirst=True)
    mask_falha = convertido.isna() & serie_limpa.notna()
    if mask_falha.any():
        convertido.loc[mask_falha] = pd.to_datetime(
            serie_limpa.loc[mask_falha], errors="coerce", dayfirst=False
        )
    return convertido


def converter_hora(serie: pd.Series) -> pd.Series:
    def _hora(x: Any) -> Optional[time]:
        if x is None or _safe_nan(x):
            return None
        if isinstance(x, time):
            return x
        if isinstance(x, pd.Timestamp):
            return None if pd.isna(x) else x.time()
        texto = str(x).strip()
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                return datetime.strptime(texto, fmt).time()
            except Exception:
                continue
        return None

    return serie.apply(_hora)


def converter_flag_agendamento(serie: pd.Series) -> pd.Series:
    """Regra oficial: qualquer valor não-nulo em data_agenda = agendada=True."""
    def _f(x: Any) -> bool:
        if x is None or _safe_nan(x):
            return False
        return True
    return serie.apply(_f)


def converter_flag_sim_nao(serie: pd.Series) -> pd.Series:
    def _f(x: Any) -> bool:
        if x is None or _safe_nan(x):
            return False
        texto = remover_acentos(str(x)).strip().lower()
        return texto in {"sim", "s", "yes", "y", "true", "1"}
    return serie.apply(_f)


def _coerce_float_or_nan(valor: Any) -> float:
    if valor is None or _safe_nan(valor):
        return np.nan
    try:
        texto = str(valor).strip().replace(",", ".")
        return float(texto)
    except Exception:
        return np.nan


def normalizar_valor_parametro(x: Any) -> Any:
    if x is None or x is pd.NaT:
        return None
    if isinstance(x, pd.Timestamp):
        return None if pd.isna(x) else x.isoformat()
    if _safe_nan(x):
        return None
    return str(x).strip()


# ─────────────────────────────────────────────
# COALESCE DE COLUNAS (aliases do dataset real)
# ─────────────────────────────────────────────

def _coalescer_colunas(df: pd.DataFrame, alvo: str, candidatos: List[str]) -> pd.DataFrame:
    presentes = [c for c in candidatos if c in df.columns]
    if not presentes:
        return df
    if alvo not in df.columns:
        df[alvo] = np.nan
    for col in presentes:
        df[alvo] = df[alvo].where(df[alvo].notna(), df[col])
    return df


def _garantir_colunas_carteira(carteira: pd.DataFrame) -> pd.DataFrame:
    """Consolida layout novo, antigo e nomes truncados em colunas estáveis."""
    coalesces = [
        ("filial_r",        ["filial_r", "filial"]),
        ("romane",          ["romane", "romanei", "romaneio"]),
        ("filial_d",        ["filial_d", "filial_origem", "filial_1"]),
        ("serie",           ["serie", "serie_d"]),
        ("nro_doc",         ["nro_doc", "nro_do", "numero_documento"]),
        ("data_des",        ["data_des", "data", "data_d", "data_descarga"]),
        ("data_nf",         ["data_nf", "data_n"]),
        ("dle",             ["dle", "data_leadtime"]),
        ("agendam",         ["agendam", "data_agenda", "agenda"]),
        ("vlrmerc",         ["vlrmerc", "valor_nf"]),
        ("palet",           ["palet", "qtd_pallet"]),
        ("conf",            ["conf", "conferencia"]),
        ("peso",            ["peso", "peso_kg"]),
        ("qtd",             ["qtd", "qtd_volumes"]),
        ("peso_cub",        ["peso_cub", "peso_c", "vol_m3"]),
        ("peso_calculo",    ["peso_calculo", "peso_calculado"]),
        ("classif",         ["classif", "classifi", "classifica"]),
        ("tomad",           ["tomad", "tomador"]),
        ("destin",          ["destin", "destinatario", "destina"]),
        ("cidad",           ["cidad", "cidade"]),
        ("uf",              ["uf"]),
        ("tipo_ca",         ["tipo_ca", "tipo_carg"]),
        ("tipo_carga",      ["tipo_carga", "tipo_c"]),
        ("regiao",          ["regiao"]),
        ("mesoregiao",      ["mesoregiao", "mesorregiao"]),
        ("sub_regiao",      ["sub_regiao", "subregiao"]),
        ("ocorrencias_nf",  ["ocorrencias_nf", "ocorrencias_nfs", "ocorrencias_n"]),
        ("observacao",      ["observacao", "observacao_r"]),
        ("cidade_dest",     ["cidade_dest"]),
        ("ultima_ocorrencia", ["ultima_ocorrencia", "ultima"]),
        ("status_r",        ["status_r", "status"]),
        ("latitude",        ["latitude", "lat", "latitude_destinatario"]),
        ("longitude",       ["longitude", "lon", "longitude_destinatario"]),
        ("restricao_veiculo", ["restricao_veiculo", "restricao_veic"]),
        ("carro_dedicado",  ["carro_dedicado", "veiculo_exclusivo", "dedicado"]),
        ("inicio_ent",      ["inicio_ent", "inicio_entrega"]),
        ("fim_en",          ["fim_en", "fim_ent", "fim_entrega"]),
        ("prioridade",      ["prioridade", "prioridade_embarque"]),
        ("cte",             ["cte"]),
    ]
    for alvo, candidatos in coalesces:
        carteira = _coalescer_colunas(carteira, alvo, candidatos)
    return carteira


# ─────────────────────────────────────────────
# EXTRAÇÃO DE PARÂMETROS
# ─────────────────────────────────────────────

def _extrair_parametros_dict(parametros: pd.DataFrame) -> Dict[str, Any]:
    if parametros.empty:
        return {}
    if "parametro" in parametros.columns and "valor" in parametros.columns:
        p = parametros.copy()
        p["parametro"] = p["parametro"].apply(normalizar_texto_basico)
        p["valor"] = p["valor"].apply(normalizar_valor_parametro)
        return {str(k): v for k, v in zip(p["parametro"], p["valor"]) if pd.notna(k)}
    if len(parametros) == 1:
        linha = parametros.iloc[0].to_dict()
        return {padronizar_nome_coluna(k): normalizar_valor_parametro(v)
                for k, v in linha.items() if str(k).strip()}
    raise Exception(
        "A base de parâmetros não está em formato compatível. "
        "Esperado: colunas 'parametro'/'valor' ou registro único."
    )


# ─────────────────────────────────────────────
# FUNÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def executar_m1_padronizacao(
    df_carteira_raw: pd.DataFrame,
    df_geo_raw: pd.DataFrame,
    df_parametros_raw: pd.DataFrame,
    df_veiculos_raw: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M1 — Limpeza, padronização e tipagem.
    Retorna: df_carteira_tratada, df_geo_tratado, df_parametros_tratados,
             df_veiculos_tratados, param_dict
    """
    carteira = df_carteira_raw.copy()
    geo = df_geo_raw.copy()
    parametros = df_parametros_raw.copy()
    veiculos = df_veiculos_raw.copy()

    # 1) Padronizar nomes de colunas
    for df in [carteira, geo, parametros, veiculos]:
        cols = [padronizar_nome_coluna(c) for c in df.columns]
        df.columns = garantir_colunas_unicas(cols)

    # 2) Consolidar aliases da carteira
    carteira = _garantir_colunas_carteira(carteira)

    # 3) Renomear carteira para layout interno estável
    mapa_carteira = {
        "filial_r":         "filial_roteirizacao",
        "romane":           "romaneio",
        "filial_d":         "filial_origem",
        "serie":            "serie",
        "nro_doc":          "nro_documento",
        "data_des":         "data_descarga",
        "data_nf":          "data_nf",
        "dle":              "data_leadtime",
        "agendam":          "data_agenda",
        "vlrmerc":          "valor_nf",
        "palet":            "qtd_pallet",
        "conf":             "conferencia",
        "peso":             "peso_kg",
        "qtd":              "qtd_volumes",
        "peso_cub":         "vol_m3",
        "classif":          "classifi",
        "tomad":            "tomador",
        "destin":           "destinatario",
        "cidad":            "cidade",
        "tipo_ca":          "tipo_ca",
        "tipo_carga":       "tipo_carga",
        "regiao":           "regiao",
        "mesoregiao":       "mesorregiao",
        "sub_regiao":       "sub_regiao",
        "ocorrencias_nf":   "ocorrencias_nfs",
        "observacao":       "observacao_r",
        "cidade_dest":      "cidade_dest",
        "ultima_ocorrencia": "ultima",
        "status_r":         "status",
        "latitude":         "latitude_destinatario",
        "longitude":        "longitude_destinatario",
        "peso_calculo":     "peso_calculado",
        "prioridade":       "prioridade_embarque",
        "restricao_veiculo": "restricao_veiculo",
        "carro_dedicado":   "veiculo_exclusivo",
        "inicio_ent":       "inicio_entrega",
        "fim_en":           "fim_entrega",
        "cte":              "cte",
    }
    carteira = carteira.rename(
        columns={k: v for k, v in mapa_carteira.items() if k in carteira.columns}
    )
    # 3b) Deduplicar colunas: manter apenas a primeira ocorrência de cada nome
    if carteira.columns.duplicated().any():
        carteira = carteira.loc[:, ~carteira.columns.duplicated(keep="first")]

    # 4) Renomear geo
    if "nome" not in geo.columns and "cidade" in geo.columns:
        geo["nome"] = geo["cidade"]
    if "cidade" not in geo.columns and "nome" in geo.columns:
        geo["cidade"] = geo["nome"]
    if "mesoregiao" in geo.columns and "mesorregiao" not in geo.columns:
        geo = geo.rename(columns={"mesoregiao": "mesorregiao"})
    if "sub_regiao" in geo.columns and "subregiao" not in geo.columns:
        geo = geo.rename(columns={"sub_regiao": "subregiao"})

    # 5) Extrair parâmetros
    if "parametro" not in parametros.columns and "chave" in parametros.columns:
        parametros = parametros.rename(columns={"chave": "parametro"})
    param_dict = _extrair_parametros_dict(parametros)

    origem_latitude  = param_dict.get("origem_latitude")  or param_dict.get("latitude_filial")
    origem_longitude = param_dict.get("origem_longitude") or param_dict.get("longitude_filial")
    origem_cidade    = param_dict.get("origem_cidade")
    origem_uf        = param_dict.get("origem_uf")
    data_base        = param_dict.get("data_base_roteirizacao") or param_dict.get("data_execucao")

    # 6) Tipagem numérica da carteira
    colunas_num = [
        "filial_roteirizacao", "romaneio", "filial_origem", "serie",
        "nro_documento", "qtd_pallet", "peso_kg", "valor_nf",
        "qtd_volumes", "vol_m3", "qtd_nf", "peso_calculado",
    ]
    for c in colunas_num:
        if c in carteira.columns:
            carteira[c] = converter_numerico_brasil(carteira[c])

    if "prioridade_embarque" in carteira.columns:
        prioridade_num = converter_numerico_brasil(carteira["prioridade_embarque"])
        carteira["prioridade_embarque_num"] = prioridade_num

    for c in ["latitude_destinatario", "longitude_destinatario"]:
        if c in carteira.columns:
            carteira[c] = converter_coordenada(carteira[c])

    for c in ["data_descarga", "data_nf", "data_leadtime", "data_agenda"]:
        if c in carteira.columns:
            carteira[c] = converter_data(carteira[c])

    for c in ["inicio_entrega", "fim_entrega"]:
        if c in carteira.columns:
            carteira[c] = converter_hora(carteira[c])

    colunas_texto = [
        "conferencia", "classifi", "tomador", "destinatario", "bairro",
        "cidade", "uf", "nf_serie", "tipo_ca", "tipo_carga", "regiao",
        "sub_regiao", "ocorrencias_nfs", "remetente", "observacao_r",
        "ref_cliente", "cidade_dest", "mesorregiao", "ultima", "status",
        "veiculo_exclusivo", "restricao_veiculo", "cte",
    ]
    for c in colunas_texto:
        if c in carteira.columns:
            carteira[c] = carteira[c].apply(normalizar_texto_basico)

    # 7) Flags
    if "data_agenda" in carteira.columns:
        carteira["agendada"] = converter_flag_agendamento(carteira["data_agenda"])
    else:
        carteira["agendada"] = False

    if "veiculo_exclusivo" in carteira.columns:
        carteira["veiculo_exclusivo_flag"] = converter_flag_sim_nao(carteira["veiculo_exclusivo"])
    else:
        carteira["veiculo_exclusivo_flag"] = False

    # 8) REGRA OFICIAL DE PESO: peso_calculado = "Peso Calculo"; fallback = "Peso"
    # NUNCA usar cubagem como fallback de peso
    if "peso_calculado" not in carteira.columns:
        carteira["peso_calculado"] = np.nan
    if "peso_kg" not in carteira.columns:
        carteira["peso_kg"] = np.nan
    carteira["peso_calculado"] = carteira["peso_calculado"].where(
        carteira["peso_calculado"].notna(), carteira["peso_kg"]
    )

    # 9) Injetar origem na carteira
    carteira["origem_cidade"]    = origem_cidade
    carteira["origem_uf"]        = origem_uf
    carteira["latitude_filial"]  = _coerce_float_or_nan(origem_latitude)
    carteira["longitude_filial"] = _coerce_float_or_nan(origem_longitude)
    carteira["data_base_roteirizacao"] = data_base

    # 10) Tipagem geo
    for c in ["cidade", "nome", "uf", "mesorregiao", "microrregiao", "subregiao"]:
        if c in geo.columns:
            geo[c] = geo[c].apply(normalizar_texto_basico)
    for c in ["latitude", "longitude"]:
        if c in geo.columns:
            geo[c] = converter_coordenada(geo[c])

    # 11) Chaves geo para join
    if not geo.empty:
        if "cidade" in geo.columns:
            geo["cidade_chave"] = normalizar_chave_texto(geo["cidade"])
        else:
            geo["cidade_chave"] = ""
        if "uf" in geo.columns:
            geo["uf_chave"] = normalizar_chave_texto(geo["uf"])
        else:
            geo["uf_chave"] = ""
    carteira["cidade_chave"] = normalizar_chave_texto(carteira["cidade"])
    carteira["uf_chave"]     = normalizar_chave_texto(carteira["uf"])

    # 12) Tipagem veículos
    for c in ["qtd_eixos", "capacidade_peso_kg", "capacidade_vol_m3",
              "max_entregas", "max_km_distancia", "ocupacao_minima_perc"]:
        if c in veiculos.columns:
            veiculos[c] = converter_numerico_brasil(veiculos[c])
    if "perfil" in veiculos.columns:
        veiculos["perfil"] = veiculos["perfil"].apply(normalizar_texto_basico)
    if "dedicado" in veiculos.columns:
        veiculos["dedicado"] = converter_flag_sim_nao(veiculos["dedicado"])
    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]
    veiculos["ordem_porte"] = np.arange(1, len(veiculos) + 1)

    # 13) Safeguards mínimos de colunas
    colunas_minimas = [
        "nro_documento", "peso_kg", "vol_m3", "peso_calculado",
        "destinatario", "cidade", "uf", "regiao", "mesorregiao", "sub_regiao",
        "latitude_destinatario", "longitude_destinatario",
        "prioridade_embarque", "restricao_veiculo", "veiculo_exclusivo",
        "inicio_entrega", "fim_entrega", "cte",
    ]
    for col in colunas_minimas:
        if col not in carteira.columns:
            carteira[col] = np.nan

    # 14) Validações obrigatórias
    if "cidade" not in carteira.columns:
        raise Exception("A carteira tratada não contém a coluna obrigatória 'cidade'.")
    if "uf" not in carteira.columns:
        raise Exception("A carteira tratada não contém a coluna obrigatória 'uf'.")

    return {
        "df_carteira_tratada":    carteira,
        "df_geo_tratado":         geo,
        "df_parametros_tratados": parametros,
        "df_veiculos_tratados":   veiculos,
        "param_dict":             param_dict,
    }
