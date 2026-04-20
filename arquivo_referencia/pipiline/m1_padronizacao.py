# ============================================================
# MÓDULO 1 - LIMPEZA, PADRONIZAÇÃO E TIPAGEM
# (VERSÃO API - AJUSTADA AO CONTRATO NOVO DO SISTEMA 1)
# ============================================================

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, time
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


def normalizar_texto_basico(valor: Any) -> Any:
    if valor is None:
        return np.nan

    try:
        resultado_isna = pd.isna(valor)
        if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
            return np.nan
    except Exception:
        pass

    texto = str(valor).replace("\u00a0", " ")
    texto = texto.strip()
    texto = re.sub(r"\s+", " ", texto)

    return texto if texto != "" else np.nan


def remover_acentos(texto: Any) -> Any:
    if texto is None:
        return np.nan

    try:
        resultado_isna = pd.isna(texto)
        if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
            return np.nan
    except Exception:
        pass

    texto = str(texto)
    return "".join(
        c for c in unicodedata.normalize("NFKD", texto)
        if not unicodedata.combining(c)
    )


def padronizar_nome_coluna(col: Any) -> str:
    col = normalizar_texto_basico(col)
    col = remover_acentos(col)
    col = str(col).lower()
    col = col.replace("/", "_")
    col = col.replace(".", "")
    col = col.replace("-", "_")
    col = col.replace("(", "_")
    col = col.replace(")", "_")
    col = col.replace("%", "perc")
    col = re.sub(r"[^a-z0-9_]+", "_", col)
    col = re.sub(r"_+", "_", col)
    col = col.strip("_")
    return col


def garantir_colunas_unicas(colunas: list[str]) -> list[str]:
    novas = []
    contador: Dict[str, int] = {}

    for c in colunas:
        if c not in contador:
            contador[c] = 0
            novas.append(c)
        else:
            contador[c] += 1
            novas.append(f"{c}_{contador[c]}")

    return novas


def escolher_coluna(df: pd.DataFrame, candidatos: list[str]) -> Optional[str]:
    for c in candidatos:
        if c in df.columns:
            return c
    return None


def converter_numerico_brasil(serie: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(serie):
        return pd.to_numeric(serie, errors="coerce")

    s = serie.astype(str).str.strip()

    def _conv(x: Any) -> float:
        if x is None:
            return np.nan

        try:
            resultado_isna = pd.isna(x)
            if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
                return np.nan
        except Exception:
            pass

        x = str(x).replace("R$", "").replace(" ", "")

        if "." in x and "," in x:
            if x.rfind(",") > x.rfind("."):
                x = x.replace(".", "").replace(",", ".")
            else:
                x = x.replace(",", "")
        elif "," in x:
            x = x.replace(".", "").replace(",", ".")

        return pd.to_numeric(x, errors="coerce")

    return s.apply(_conv)


def converter_coordenada(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip()

    def _coord(x: Any) -> float:
        if x is None:
            return np.nan

        try:
            resultado_isna = pd.isna(x)
            if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
                return np.nan
        except Exception:
            pass

        x = str(x).replace(" ", "")

        if "." in x and "," in x:
            if x.rfind(",") > x.rfind("."):
                x = x.replace(".", "").replace(",", ".")
            else:
                x = x.replace(",", "")
        elif "," in x:
            x = x.replace(",", ".")

        return pd.to_numeric(x, errors="coerce")

    return s.apply(_coord)


def _limpar_texto_data(valor: Any) -> Any:
    """
    Higieniza datas vindas do dataset REC sem alterar a regra do pipeline.
    Casos tratados:
    - espaços extras
    - vírgula/; no final: "26/12/2025 06:00:00,"
    - placeholders textuais: "-", "null", "nan", etc.
    - timestamps/datetime já prontos
    """
    if valor is None:
        return np.nan

    try:
        resultado_isna = pd.isna(valor)
        if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
            return np.nan
    except Exception:
        pass

    if isinstance(valor, pd.Timestamp):
        return valor

    if isinstance(valor, datetime):
        return pd.Timestamp(valor)

    texto = str(valor).replace("\u00a0", " ")
    texto = texto.strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"^[,;]+", "", texto)
    texto = re.sub(r"[,;]+$", "", texto)
    texto = texto.strip()

    texto_lower = texto.lower()
    if texto_lower in {"", "-", "--", "null", "none", "nan", "nat", "n/a", "na"}:
        return np.nan

    return texto


def converter_data(serie: pd.Series) -> pd.Series:
    serie_limpa = serie.apply(_limpar_texto_data)

    convertido = pd.to_datetime(serie_limpa, errors="coerce", dayfirst=True)

    mask_falha = convertido.isna() & serie_limpa.notna()
    if mask_falha.any():
        convertido.loc[mask_falha] = pd.to_datetime(
            serie_limpa.loc[mask_falha],
            errors="coerce",
            dayfirst=False
        )

    return convertido


def _parse_hora_flex(valor: Any) -> Optional[time]:
    if valor is None:
        return None

    try:
        resultado_isna = pd.isna(valor)
        if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
            return None
    except Exception:
        pass

    if isinstance(valor, time):
        return valor

    if isinstance(valor, pd.Timestamp):
        if pd.isna(valor):
            return None
        return valor.time()

    texto = str(valor).strip()
    if texto == "":
        return None

    formatos = ("%H:%M", "%H:%M:%S")
    for fmt in formatos:
        try:
            return datetime.strptime(texto, fmt).time()
        except Exception:
            continue

    return None


def converter_hora(serie: pd.Series) -> pd.Series:
    return serie.apply(_parse_hora_flex)


def converter_flag_agendamento(serie: pd.Series) -> pd.Series:
    def _f(x: Any) -> bool:
        if x is None:
            return False

        try:
            resultado_isna = pd.isna(x)
            if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
                return False
        except Exception:
            pass

        return True

    return serie.apply(_f)


def converter_flag_sim_nao(serie: pd.Series) -> pd.Series:
    def _f(x: Any) -> bool:
        if x is None:
            return False

        try:
            resultado_isna = pd.isna(x)
            if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
                return False
        except Exception:
            pass

        texto = normalizar_texto_basico(x)
        if texto is None or (isinstance(texto, float) and pd.isna(texto)):
            return False

        texto = remover_acentos(texto)
        texto = str(texto).strip().lower()

        if texto in {"sim", "s", "yes", "y", "true", "1"}:
            return True

        if texto in {"nao", "não", "n", "no", "false", "0", ""}:
            return False

        return False

    return serie.apply(_f)


def normalizar_chave_texto(serie: pd.Series) -> pd.Series:
    return serie.apply(
        lambda x: remover_acentos(str(x)).upper().strip() if pd.notna(x) else np.nan
    )


def normalizar_valor_parametro(x: Any) -> Any:
    if x is None:
        return None

    if x is pd.NaT:
        return None

    if isinstance(x, pd.Timestamp):
        if pd.isna(x):
            return None
        return x.isoformat()

    if isinstance(x, np.ndarray):
        return str(x.tolist()).strip()

    if isinstance(x, pd.Series):
        return str(x.tolist()).strip()

    if isinstance(x, pd.Index):
        return str(x.tolist()).strip()

    if isinstance(x, (list, tuple, set)):
        return str(list(x)).strip()

    try:
        resultado_isna = pd.isna(x)
        if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
            return None
    except Exception:
        pass

    return str(x).strip()


def _coalescer_colunas(df: pd.DataFrame, alvo: str, candidatos: list[str]) -> pd.DataFrame:
    presentes = [c for c in candidatos if c in df.columns]

    if not presentes:
        return df

    if alvo not in df.columns:
        df[alvo] = np.nan

    for col in presentes:
        df[alvo] = df[alvo].where(df[alvo].notna(), df[col])

    return df


def _garantir_colunas_carteira_v2(carteira: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida layout novo, layout antigo e nomes truncados do dataset real
    em um conjunto estável de colunas brutas.
    """
    carteira = _coalescer_colunas(carteira, "filial_r", ["filial_r", "filial"])
    carteira = _coalescer_colunas(carteira, "romane", ["romane", "romanei"])
    carteira = _coalescer_colunas(carteira, "filial_d", ["filial_d", "filial_origem", "filial_1"])
    carteira = _coalescer_colunas(carteira, "serie", ["serie", "serie_d"])
    carteira = _coalescer_colunas(carteira, "nro_doc", ["nro_doc", "nro_do"])

    carteira = _coalescer_colunas(carteira, "data_des", ["data_des", "data", "data_d"])
    carteira = _coalescer_colunas(carteira, "data_nf", ["data_nf", "data_n"])
    carteira = _coalescer_colunas(carteira, "dle", ["dle"])
    carteira = _coalescer_colunas(carteira, "agendam", ["agendam"])

    carteira = _coalescer_colunas(carteira, "vlrmerc", ["vlrmerc"])
    carteira = _coalescer_colunas(carteira, "qtd", ["qtd"])
    carteira = _coalescer_colunas(carteira, "qtdnf", ["qtdnf"])
    carteira = _coalescer_colunas(carteira, "peso_cub", ["peso_cub", "peso_c"])
    carteira = _coalescer_colunas(carteira, "peso_calculo", ["peso_calculo", "peso_calculado"])

    carteira = _coalescer_colunas(carteira, "classif", ["classif", "classifi", "classifica"])
    carteira = _coalescer_colunas(carteira, "tomad", ["tomad", "tomador"])
    carteira = _coalescer_colunas(carteira, "destin", ["destin", "destinatario", "destina"])
    carteira = _coalescer_colunas(carteira, "cidad", ["cidad", "cida"])
    carteira = _coalescer_colunas(carteira, "tipo_ca", ["tipo_ca", "tipo_carg"])
    carteira = _coalescer_colunas(carteira, "tipo_carga", ["tipo_carga", "tipo_c"])
    carteira = _coalescer_colunas(carteira, "regiao", ["regiao"])
    carteira = _coalescer_colunas(carteira, "mesoregiao", ["mesoregiao"])
    carteira = _coalescer_colunas(carteira, "sub_regiao", ["sub_regiao"])
    carteira = _coalescer_colunas(carteira, "ocorrencias_nf", ["ocorrencias_nf", "ocorrencias_nfs", "ocorrencias_n"])
    carteira = _coalescer_colunas(carteira, "observacao", ["observacao", "observacao_r"])
    carteira = _coalescer_colunas(carteira, "cidade_dest", ["cidade_dest"])
    carteira = _coalescer_colunas(carteira, "ultima_ocorrencia", ["ultima_ocorrencia", "ultima"])
    carteira = _coalescer_colunas(carteira, "status_r", ["status_r", "status"])

    carteira = _coalescer_colunas(carteira, "latitude", ["latitude", "lat"])
    carteira = _coalescer_colunas(carteira, "longitude", ["longitude", "lon"])

    carteira = _coalescer_colunas(carteira, "restricao_veiculo", ["restricao_veiculo", "restricao_veic"])
    carteira = _coalescer_colunas(carteira, "carro_dedicado", ["carro_dedicado", "veiculo_exclusivo"])
    carteira = _coalescer_colunas(carteira, "inicio_ent", ["inicio_ent"])
    carteira = _coalescer_colunas(carteira, "fim_en", ["fim_en", "fim_ent", "fim_ent_1"])

    return carteira


def _extrair_parametros_dict(parametros: pd.DataFrame) -> Dict[str, Any]:
    """
    Suporta dois formatos de entrada:
    1) formato antigo: colunas 'parametro' e 'valor'
    2) formato novo: objeto/registro único com colunas já nomeadas
    """
    if parametros.empty:
        return {}

    if "parametro" in parametros.columns and "valor" in parametros.columns:
        parametros_local = parametros.copy()
        parametros_local["parametro"] = parametros_local["parametro"].apply(normalizar_texto_basico)
        parametros_local["valor"] = parametros_local["valor"].apply(normalizar_valor_parametro)
        return dict(zip(parametros_local["parametro"], parametros_local["valor"]))

    if len(parametros) == 1:
        linha = parametros.iloc[0].to_dict()
        resultado: Dict[str, Any] = {}

        for chave, valor in linha.items():
            chave_norm = normalizar_texto_basico(chave)
            if chave_norm is None or (isinstance(chave_norm, float) and pd.isna(chave_norm)):
                continue

            chave_norm = padronizar_nome_coluna(chave_norm)
            resultado[chave_norm] = normalizar_valor_parametro(valor)

        return resultado

    raise Exception(
        "A base de parâmetros não está em formato compatível. "
        "Esperado: colunas 'parametro' e 'valor' ou registro único com campos do contexto."
    )


def _coerce_float_or_nan(valor: Any) -> float:
    if valor is None:
        return np.nan

    try:
        resultado_isna = pd.isna(valor)
        if isinstance(resultado_isna, (bool, np.bool_)) and bool(resultado_isna):
            return np.nan
    except Exception:
        pass

    try:
        texto = str(valor).strip()

        if "." in texto and "," in texto:
            if texto.rfind(",") > texto.rfind("."):
                texto = texto.replace(".", "").replace(",", ".")
            else:
                texto = texto.replace(",", "")
        elif "," in texto:
            texto = texto.replace(",", ".")

        return float(texto)
    except Exception:
        return np.nan


def executar_m1_padronizacao(
    df_carteira_raw: pd.DataFrame,
    df_geo_raw: pd.DataFrame,
    df_parametros_raw: pd.DataFrame,
    df_veiculos_raw: pd.DataFrame
) -> Dict[str, Any]:

    # --------------------------------------------------------
    # 1) CÓPIAS
    # --------------------------------------------------------
    carteira = df_carteira_raw.copy()
    geo = df_geo_raw.copy()
    parametros = df_parametros_raw.copy()
    veiculos = df_veiculos_raw.copy()

    # --------------------------------------------------------
    # 2) PADRONIZA NOMES DE COLUNAS
    # --------------------------------------------------------
    for df in [carteira, geo, parametros, veiculos]:
        cols = [padronizar_nome_coluna(c) for c in df.columns]
        df.columns = garantir_colunas_unicas(cols)

    carteira = _garantir_colunas_carteira_v2(carteira)

    # --------------------------------------------------------
    # 3) MAPA CARTEIRA
    # Layout interno estável do pipeline
    # --------------------------------------------------------
    mapa_carteira = {
        "filial_r": "filial_roteirizacao",
        "romane": "romaneio",
        "filial_d": "filial_origem",
        "serie": "serie",
        "nro_doc": "nro_documento",
        "data_des": "data_descarga",
        "data_nf": "data_nf",
        "dle": "data_leadtime",
        "agendam": "data_agenda",
        "palet": "qtd_pallet",
        "conf": "conferencia",
        "peso": "peso_kg",
        "vlrmerc": "valor_nf",
        "qtd": "qtd_volumes",
        "peso_cub": "vol_m3",
        "classif": "classifi",
        "tomad": "tomador",
        "destin": "destinatario",
        "bairro": "bairro",
        "cidad": "cidade",
        "uf": "uf",
        "nf_serie": "nf_serie",
        "tipo_ca": "tipo_ca",
        "tipo_carga": "tipo_carga",
        "qtdnf": "qtd_nf",
        "regiao": "regiao",
        "sub_regiao": "sub_regiao",
        "ocorrencias_nf": "ocorrencias_nfs",
        "remetente": "remetente",
        "observacao": "observacao_r",
        "ref_cliente": "ref_cliente",
        "cidade_dest": "cidade_dest",
        "mesoregiao": "mesorregiao",
        "agenda": "agenda",
        "ultima_ocorrencia": "ultima",
        "status_r": "status",
        "latitude": "latitude_destinatario",
        "longitude": "longitude_destinatario",
        "peso_calculo": "peso_calculado",
        "prioridade": "prioridade_embarque",
        "restricao_veiculo": "restricao_veiculo",
        "carro_dedicado": "veiculo_exclusivo",
        "inicio_ent": "inicio_entrega",
        "fim_en": "fim_entrega",
    }

    carteira = carteira.rename(
        columns={k: v for k, v in mapa_carteira.items() if k in carteira.columns}
    )

    # --------------------------------------------------------
    # 4) MAPA GEO / REGIONALIDADES
    # --------------------------------------------------------
    mapa_geo = {
        "cidade": "cidade",
        "nome": "nome",
        "uf": "uf",
        "mesorregiao": "mesorregiao",
        "microrregiao": "microrregiao",
        "latitude": "latitude",
        "longitude": "longitude",
    }

    geo = geo.rename(columns={k: v for k, v in mapa_geo.items() if k in geo.columns})

    if "nome" not in geo.columns and "cidade" in geo.columns:
        geo["nome"] = geo["cidade"]

    if "cidade" not in geo.columns and "nome" in geo.columns:
        geo["cidade"] = geo["nome"]

    # --------------------------------------------------------
    # 5) MAPA PARÂMETROS
    # --------------------------------------------------------
    if "parametro" not in parametros.columns and "chave" in parametros.columns:
        parametros = parametros.rename(columns={"chave": "parametro"})

    if "valor" not in parametros.columns:
        col_valor = escolher_coluna(parametros, ["valor", "value"])
        if col_valor and col_valor != "valor":
            parametros = parametros.rename(columns={col_valor: "valor"})

    param_dict = _extrair_parametros_dict(parametros)

    # normaliza aliases esperados do contrato novo
    origem_cidade = param_dict.get("origem_cidade")
    origem_uf = param_dict.get("origem_uf")

    origem_latitude = param_dict.get("origem_latitude")
    if origem_latitude is None:
        origem_latitude = param_dict.get("latitude_filial")

    origem_longitude = param_dict.get("origem_longitude")
    if origem_longitude is None:
        origem_longitude = param_dict.get("longitude_filial")

    data_base_roteirizacao = param_dict.get("data_base_roteirizacao")
    if data_base_roteirizacao is None:
        data_base_roteirizacao = param_dict.get("data_execucao")

    # --------------------------------------------------------
    # 6) MAPA VEÍCULOS
    # --------------------------------------------------------
    mapa_veiculos = {
        "id": "id",
        "perfil": "perfil",
        "placa": "placa",
        "qtd_eixos": "qtd_eixos",
        "capacidade_peso_kg": "capacidade_peso_kg",
        "capacidade_vol_m3": "capacidade_vol_m3",
        "max_entregas": "max_entregas",
        "max_km_distancia": "max_km_distancia",
        "ocupacao_minima_perc": "ocupacao_minima_perc",
        "filial_id": "filial_id",
        "tipo_frota": "tipo_frota",
        "ativo": "ativo",
        "dedicado": "dedicado",
    }

    veiculos = veiculos.rename(
        columns={k: v for k, v in mapa_veiculos.items() if k in veiculos.columns}
    )

    # --------------------------------------------------------
    # 7) TIPAGEM CARTEIRA
    # --------------------------------------------------------
    colunas_num = [
        "filial_roteirizacao",
        "romaneio",
        "filial_origem",
        "serie",
        "nro_documento",
        "qtd_pallet",
        "peso_kg",
        "valor_nf",
        "qtd_volumes",
        "vol_m3",
        "qtd_nf",
        "peso_calculado",
    ]

    for c in colunas_num:
        if c in carteira.columns:
            carteira[c] = converter_numerico_brasil(carteira[c])

    if "prioridade_embarque" in carteira.columns:
        prioridade_num = converter_numerico_brasil(carteira["prioridade_embarque"])
        carteira["prioridade_embarque_num"] = prioridade_num

        carteira["prioridade_embarque"] = carteira["prioridade_embarque"].apply(normalizar_texto_basico)

        carteira["prioridade_embarque"] = carteira["prioridade_embarque"].where(
            prioridade_num.isna(),
            prioridade_num
        )

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
        "conferencia",
        "classifi",
        "tomador",
        "destinatario",
        "bairro",
        "cidade",
        "uf",
        "nf_serie",
        "tipo_ca",
        "tipo_carga",
        "regiao",
        "sub_regiao",
        "ocorrencias_nfs",
        "remetente",
        "observacao_r",
        "ref_cliente",
        "cidade_dest",
        "mesorregiao",
        "agenda",
        "ultima",
        "status",
        "veiculo_exclusivo",
        "restricao_veiculo",
    ]

    for c in colunas_texto:
        if c in carteira.columns:
            carteira[c] = carteira[c].apply(normalizar_texto_basico)

    # regra oficial: só Agendam. / data_agenda define se é agendada
    if "data_agenda" in carteira.columns:
        carteira["agendada"] = converter_flag_agendamento(carteira["data_agenda"])
    else:
        carteira["agendada"] = False

    if "veiculo_exclusivo" in carteira.columns:
        carteira["veiculo_exclusivo_flag"] = converter_flag_sim_nao(carteira["veiculo_exclusivo"])
    else:
        carteira["veiculo_exclusivo_flag"] = False

    # --------------------------------------------------------
    # REGRA OFICIAL DE PESO DO MOTOR
    # peso_calculado = Peso Calculo
    # fallback = Peso
    # PROIBIDO usar cubagem/volume como fallback de peso
    # --------------------------------------------------------
    if "peso_calculado" not in carteira.columns:
        carteira["peso_calculado"] = np.nan

    if "peso_kg" not in carteira.columns:
        carteira["peso_kg"] = np.nan

    carteira["peso_calculado"] = carteira["peso_calculado"].where(
        carteira["peso_calculado"].notna(),
        carteira["peso_kg"]
    )

    carteira["veiculo_exclusivo"] = carteira["veiculo_exclusivo_flag"]

    # --------------------------------------------------------
    # 8) TIPAGEM GEO
    # --------------------------------------------------------
    if "cidade" in geo.columns:
        geo["cidade"] = geo["cidade"].apply(normalizar_texto_basico)

    if "nome" in geo.columns:
        geo["nome"] = geo["nome"].apply(normalizar_texto_basico)

    if "uf" in geo.columns:
        geo["uf"] = geo["uf"].apply(normalizar_texto_basico)

    if "mesorregiao" in geo.columns:
        geo["mesorregiao"] = geo["mesorregiao"].apply(normalizar_texto_basico)

    if "microrregiao" in geo.columns:
        geo["microrregiao"] = geo["microrregiao"].apply(normalizar_texto_basico)

    # --------------------------------------------------------
    # 9) TIPAGEM PARÂMETROS
    # --------------------------------------------------------
    if "parametro" in parametros.columns:
        parametros["parametro"] = parametros["parametro"].apply(normalizar_texto_basico)

    if "valor" in parametros.columns:
        parametros["valor"] = parametros["valor"].apply(normalizar_valor_parametro)

    carteira["origem_cidade"] = origem_cidade
    carteira["origem_uf"] = origem_uf
    carteira["latitude_filial"] = _coerce_float_or_nan(origem_latitude)
    carteira["longitude_filial"] = _coerce_float_or_nan(origem_longitude)
    carteira["data_base_roteirizacao"] = data_base_roteirizacao

    # --------------------------------------------------------
    # 10) CHAVES GEO
    # --------------------------------------------------------
    if "cidade" not in carteira.columns:
        raise Exception("A carteira tratada não contém a coluna obrigatória 'cidade'.")

    if "uf" not in carteira.columns:
        raise Exception("A carteira tratada não contém a coluna obrigatória 'uf'.")

    if "cidade" not in geo.columns:
        raise Exception("A base de regionalidades não contém a coluna obrigatória 'cidade'.")

    if "uf" not in geo.columns:
        raise Exception("A base de regionalidades não contém a coluna obrigatória 'uf'.")

    geo["cidade_chave"] = normalizar_chave_texto(geo["cidade"])
    geo["uf_chave"] = normalizar_chave_texto(geo["uf"])

    carteira["cidade_chave"] = normalizar_chave_texto(carteira["cidade"])
    carteira["uf_chave"] = normalizar_chave_texto(carteira["uf"])

    # --------------------------------------------------------
    # 11) TIPAGEM VEÍCULOS
    # --------------------------------------------------------
    colunas_num_veiculos = [
        "qtd_eixos",
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
    ]

    for c in colunas_num_veiculos:
        if c in veiculos.columns:
            veiculos[c] = converter_numerico_brasil(veiculos[c])

    if "perfil" in veiculos.columns:
        veiculos["perfil"] = veiculos["perfil"].apply(normalizar_texto_basico)

    if "dedicado" in veiculos.columns:
        veiculos["dedicado"] = converter_flag_sim_nao(veiculos["dedicado"])

    veiculos["ordem_porte"] = np.arange(1, len(veiculos) + 1)

    # --------------------------------------------------------
    # 12) SAFEGUARDS MÍNIMOS DE COLUNAS
    # --------------------------------------------------------
    colunas_minimas = [
        "nro_documento",
        "peso_kg",
        "vol_m3",
        "peso_calculado",
        "destinatario",
        "cidade",
        "uf",
        "regiao",
        "mesorregiao",
        "sub_regiao",
        "latitude_destinatario",
        "longitude_destinatario",
        "prioridade_embarque",
        "restricao_veiculo",
        "veiculo_exclusivo",
        "inicio_entrega",
        "fim_entrega",
    ]

    for col in colunas_minimas:
        if col not in carteira.columns:
            carteira[col] = np.nan

    # --------------------------------------------------------
    # 13) OUTPUT
    # --------------------------------------------------------
    return {
        "df_carteira_tratada": carteira,
        "df_geo_tratado": geo,
        "df_parametros_tratados": parametros,
        "df_veiculos_tratados": veiculos,
    }
