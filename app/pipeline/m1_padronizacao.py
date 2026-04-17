"""
M1 — Padronização e Validação de Dados
Responsabilidade: limpar, normalizar e validar a carteira recebida.
Retorna o DataFrame padronizado e o log da etapa.
"""
from __future__ import annotations
import math
import pandas as pd
from typing import Tuple, List, Dict, Any


PRIORIDADE_ORDEM = {"URGENTE": 0, "ALTA": 1, "MEDIA": 2, "NORMAL": 3, "BAIXA": 4}


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        v = float(str(val).replace(",", ".").strip())
        return v if math.isfinite(v) else default
    except (ValueError, TypeError):
        return default


def _safe_str(val: Any, default: str = "") -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return str(val).strip().upper()


def executar(carteira_raw: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, Dict]:
    """
    Recebe a lista de dicts da carteira (já parseada do JSON de entrada),
    padroniza tipos e retorna (df_padronizado, log_etapa).
    """
    entrada_total = len(carteira_raw)
    descartados: List[Dict] = []
    registros: List[Dict] = []

    for i, item in enumerate(carteira_raw):
        # --- Peso de cálculo: usa peso_calculo se disponível, senão peso, senão peso_cubico
        peso_calc = _safe_float(item.get("peso_calculo")) or \
                    _safe_float(item.get("peso")) or \
                    _safe_float(item.get("peso_cubico"))

        # --- Coordenadas obrigatórias
        lat = _safe_float(item.get("latitude"), default=None)
        lon = _safe_float(item.get("longitude"), default=None)

        if lat is None or lon is None or lat == 0.0 or lon == 0.0:
            descartados.append({
                "nro_doc": item.get("nro_doc"),
                "destin": item.get("destin"),
                "cidade": item.get("cidade"),
                "uf": item.get("uf"),
                "peso": peso_calc,
                "vlr_merc": _safe_float(item.get("vlr_merc")),
                "motivo": "Coordenadas geográficas ausentes ou inválidas (lat/lon = 0 ou nulo)"
            })
            continue

        # --- Peso mínimo
        if peso_calc <= 0:
            descartados.append({
                "nro_doc": item.get("nro_doc"),
                "destin": item.get("destin"),
                "cidade": item.get("cidade"),
                "uf": item.get("uf"),
                "peso": peso_calc,
                "vlr_merc": _safe_float(item.get("vlr_merc")),
                "motivo": "Peso de cálculo ausente ou zero"
            })
            continue

        prioridade_raw = _safe_str(item.get("prioridade"), "NORMAL")
        prioridade = prioridade_raw if prioridade_raw in PRIORIDADE_ORDEM else "NORMAL"

        registros.append({
            # Identificação
            "_idx": i,
            "nro_doc": item.get("nro_doc"),
            "romane": item.get("romane"),
            "filial_r": item.get("filial_r"),
            "filial_d": item.get("filial_d"),
            "serie": item.get("serie"),
            # Destinatário
            "destin": _safe_str(item.get("destin")),
            "tomad": _safe_str(item.get("tomad")),
            "bairro": _safe_str(item.get("bairro")),
            "cidade": _safe_str(item.get("cidade")),
            "uf": _safe_str(item.get("uf")),
            "latitude": lat,
            "longitude": lon,
            # Carga
            "peso_calculo": peso_calc,
            "peso_cubico": _safe_float(item.get("peso_cubico")),
            "vlr_merc": _safe_float(item.get("vlr_merc")),
            "qtd": item.get("qtd"),
            "tipo_carga": _safe_str(item.get("tipo_carga"), "CARGA GERAL"),
            "tipo_ca": _safe_str(item.get("tipo_ca")),
            # Datas
            "data_des": item.get("data_des"),
            "data_nf": item.get("data_nf"),
            "dle": item.get("dle"),
            "agendam": item.get("agendam"),
            # Restrições
            "prioridade": prioridade,
            "prioridade_ordem": PRIORIDADE_ORDEM.get(prioridade, 3),
            "restricao_veiculo": _safe_str(item.get("restricao_veiculo")),
            "carro_dedicado": bool(item.get("carro_dedicado", False)),
            "inicio_entrega": item.get("inicio_entrega"),
            "fim_entrega": item.get("fim_entrega"),
            # Classificação
            "mesoregiao": _safe_str(item.get("mesoregiao")),
            "sub_regiao": _safe_str(item.get("sub_regiao")),
            "classif": _safe_str(item.get("classif")),
            "status_r": _safe_str(item.get("status_r")),
            "ocorrencias_nf": _safe_str(item.get("ocorrencias_nf")),
            "observacao": _safe_str(item.get("observacao")),
            "remetente": _safe_str(item.get("remetente")),
            "ref_cliente": _safe_str(item.get("ref_cliente")),
        })

    df = pd.DataFrame(registros) if registros else pd.DataFrame()

    log = {
        "etapa": "M1 — Padronização",
        "descricao": "Limpeza, normalização de tipos e validação de campos obrigatórios",
        "qtd_entrada": entrada_total,
        "qtd_saida": len(df),
        "qtd_descartada": len(descartados),
        "detalhes": f"{len(descartados)} registros descartados por ausência de coordenadas ou peso inválido"
    }

    return df, log, descartados
