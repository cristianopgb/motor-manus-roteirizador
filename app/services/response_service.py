# ============================================================
# RESPONSE SERVICE — Contrato de Retorno para o Sistema 1
# Transforma o resultado bruto do pipeline em JSON estruturado
# pronto para consumo pelo frontend React + Supabase
# ============================================================
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers de serialização segura
# ---------------------------------------------------------------------------

def _safe_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    try:
        if isinstance(v, float) and math.isnan(v):
            return default
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    if v is None:
        return default
    try:
        if isinstance(v, float) and math.isnan(v):
            return default
        return round(float(v), 4)
    except Exception:
        return default


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    if v is None:
        return default
    try:
        if isinstance(v, float) and math.isnan(v):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    try:
        if isinstance(v, float) and math.isnan(v):
            return default
    except Exception:
        pass
    return bool(v)


def _safe_date(v: Any) -> Optional[str]:
    """Serializa datas para ISO 8601 string ou None."""
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    try:
        ts = pd.Timestamp(v)
        if pd.isna(ts):
            return None
        return ts.isoformat()
    except Exception:
        s = _safe_str(v)
        return s if s else None


def _safe_list_str(v: Any) -> List[str]:
    """Converte campo de NFs (string separada por ';' ou lista) em lista de strings."""
    if v is None:
        return []
    try:
        if isinstance(v, float) and math.isnan(v):
            return []
    except Exception:
        pass
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None]
    s = str(v).strip()
    if not s:
        return []
    # Separadores comuns: ponto-e-vírgula, vírgula, pipe
    for sep in [";", ",", "|"]:
        if sep in s:
            return [x.strip() for x in s.split(sep) if x.strip()]
    return [s]


# ---------------------------------------------------------------------------
# Serialização de uma entrega (linha do manifesto)
# ---------------------------------------------------------------------------

def _serializar_entrega(row: Dict[str, Any], sequencia: int) -> Dict[str, Any]:
    """Converte uma linha de entrega do M7 no contrato de entrega do Sistema 1."""

    # Documentos fiscais
    nro_doc = _safe_str(row.get("nro_documento") or row.get("cte") or row.get("doc_ctrc"))
    nfs_raw = row.get("nro_nf") or row.get("n_fiscal") or row.get("nro_documento")
    lista_nfs = _safe_list_str(nfs_raw)

    # Destinatário — o pipeline usa 'cidade' (chave direta) e 'cidade_dest' como alias
    destinatario = _safe_str(
        row.get("destin") or row.get("destinatario") or row.get("destinatario_nome")
    )
    # Priorizar 'cidade' (chave do item no pipeline), depois 'cidade_dest'
    _cidade_raw = row.get("cidade") or row.get("cidade_dest") or row.get("municipio")
    cidade = _safe_str(_cidade_raw)
    uf           = _safe_str(row.get("uf") or row.get("uf_dest") or row.get("uf_chave"))
    cep          = _safe_str(row.get("cep") or row.get("cep_dest"))
    endereco     = _safe_str(row.get("endereco") or row.get("logradouro"))

    # Remetente
    remetente = _safe_str(row.get("remetente") or row.get("emitente") or row.get("remetente_nome"))

    # Pesos e volumes
    peso_kg      = _safe_float(row.get("peso_kg") or row.get("peso_calculado") or row.get("peso_bruto"))
    peso_bruto   = _safe_float(row.get("peso_bruto") or row.get("peso_kg") or row.get("peso_calculado"))
    qtd_volumes  = _safe_int(row.get("qtd_volumes") or row.get("qtd_vol") or row.get("volumes"))
    valor_merc   = _safe_float(row.get("valor_mercadoria") or row.get("vir_mercadoria") or row.get("valor_nf"))

    # Tipo de carga
    tipo_carga = _safe_str(row.get("tipo_carga") or row.get("tipo_produto") or "DIVERSOS")

    # Datas
    data_limite = _safe_date(
        row.get("data_limite_considerada") or row.get("data_leadtime") or row.get("data_agenda")
    )
    data_agenda  = _safe_date(row.get("data_agenda"))
    hora_agenda  = None
    if data_agenda:
        try:
            ts = pd.Timestamp(row.get("data_agenda"))
            hora_agenda = ts.strftime("%H:%M") if not pd.isna(ts) else None
        except Exception:
            pass

    agendada = _safe_bool(row.get("agendada"))
    info_agendamento = None
    if agendada and data_agenda:
        data_fmt = pd.Timestamp(row["data_agenda"]).strftime("%d/%m/%Y") if row.get("data_agenda") else ""
        hora_fmt = hora_agenda or "07:00"
        info_agendamento = f"Agendamento para {data_fmt} – {hora_fmt}"

    # Doc CTRC
    doc_ctrc = _safe_str(row.get("doc_ctrc") or row.get("cte"))

    # Distância e prioridade
    distancia_km = _safe_float(row.get("distancia_rodoviaria_est_km"))
    status_folga = _safe_str(row.get("status_folga"))
    folga_dias   = _safe_float(row.get("folga_dias"))

    # Coordenadas do destinatário
    lat_dest = _safe_float(row.get("latitude_destinatario"))
    lon_dest = _safe_float(row.get("longitude_destinatario"))

    return {
        "sequencia":            sequencia,
        "nro_documento":        nro_doc,
        "lista_nfs":            lista_nfs,
        "doc_ctrc":             doc_ctrc,
        "remetente":            remetente,
        "destinatario":         destinatario,
        "cidade":               cidade,
        "uf":                   uf,
        "cep":                  cep,
        "endereco":             endereco,
        "tipo_carga":           tipo_carga,
        "peso_kg":              peso_kg,
        "peso_bruto_kg":        peso_bruto,
        "qtd_volumes":          qtd_volumes,
        "valor_mercadoria":     valor_merc,
        "data_limite_entrega":  data_limite,
        "agendada":             agendada,
        "data_agenda":          data_agenda,
        "hora_agenda":          hora_agenda,
        "info_agendamento":     info_agendamento,
        "distancia_km":         distancia_km,
        "status_folga":         status_folga,
        "folga_dias":           folga_dias,
        "latitude_destinatario": lat_dest,
        "longitude_destinatario": lon_dest,
    }


# ---------------------------------------------------------------------------
# Serialização de um manifesto completo
# ---------------------------------------------------------------------------

def _serializar_manifesto(manifesto: Dict[str, Any], numero: int) -> Dict[str, Any]:
    """Converte um manifesto bruto do M7 no contrato de manifesto do Sistema 1."""

    # Chaves reais do pipeline: manifesto_id, veiculo_tipo, veiculo_perfil,
    # capacidade_peso_kg, mesorregiao_manifesto, km_rota_total,
    # ocupacao_final_m6_2, peso_final_m6_2, qtd_paradas_final_m6_2
    id_manifesto = _safe_str(
        manifesto.get("manifesto_id") or manifesto.get("id_manifesto") or f"MAN_{numero:04d}"
    )

    # Veículo
    veiculo_tipo  = _safe_str(
        manifesto.get("veiculo_tipo") or manifesto.get("perfil_final_m6_2") or manifesto.get("veiculo_perfil")
    )
    veiculo_cod   = _safe_str(manifesto.get("veiculo_codigo") or manifesto.get("codigo_veiculo"))
    placa         = _safe_str(manifesto.get("placa") or manifesto.get("placa_veiculo"))
    num_eixos     = _safe_int(manifesto.get("num_eixos") or manifesto.get("eixos"))
    cap_peso_kg   = _safe_float(
        manifesto.get("capacidade_peso_kg") or manifesto.get("capacidade_peso_kg_veiculo")
    )
    motorista     = _safe_str(manifesto.get("motorista") or manifesto.get("nome_motorista"))

    # Filial e linha
    filial        = _safe_str(manifesto.get("filial") or manifesto.get("filial_origem") or manifesto.get("nome_filial"))
    linha_op      = _safe_str(manifesto.get("linha_operacao") or manifesto.get("linha") or manifesto.get("rota"))
    regiao        = _safe_str(
        manifesto.get("mesorregiao_manifesto") or manifesto.get("regiao") or manifesto.get("mesorregiao")
    )
    data_rot      = _safe_date(manifesto.get("data_roteirizacao") or manifesto.get("data_base"))

    # Totais (usar valores finais do M6.2 quando disponíveis)
    total_entregas = _safe_int(
        manifesto.get("qtd_paradas_final_m6_2") or manifesto.get("total_entregas") or manifesto.get("qtd_paradas")
    )
    total_peso     = _safe_float(
        manifesto.get("peso_final_m6_2") or manifesto.get("total_peso_kg") or manifesto.get("peso_total_kg")
    )
    total_valor    = _safe_float(manifesto.get("total_valor_mercadoria") or manifesto.get("valor_total"))
    total_volumes  = _safe_int(manifesto.get("total_volumes") or manifesto.get("qtd_volumes"))
    km_estimado    = _safe_float(
        manifesto.get("km_rota_total") or manifesto.get("km_final_m6_2") or manifesto.get("km_estimado")
    )
    ocupacao_perc  = _safe_float(
        manifesto.get("ocupacao_final_m6_2") or manifesto.get("ocupacao_percentual") or manifesto.get("ocupacao_perc")
    )

    # Entregas sequenciadas
    entregas_raw = manifesto.get("entregas", [])
    entregas = []
    for i, e in enumerate(entregas_raw, start=1):
        if isinstance(e, dict):
            entregas.append(_serializar_entrega(e, i))

    # Recalcular totais a partir das entregas se não vieram do manifesto
    if not total_entregas:
        total_entregas = len(entregas)
    if not total_peso:
        total_peso = round(sum(e["peso_kg"] or 0 for e in entregas), 2)
    if not total_valor:
        total_valor = round(sum(e["valor_mercadoria"] or 0 for e in entregas), 2)
    if not total_volumes:
        total_volumes = sum(e["qtd_volumes"] or 0 for e in entregas)

    # Agendamentos (entregas com data_agenda)
    agendamentos = [
        {
            "sequencia":         e["sequencia"],
            "nro_documento":     e["nro_documento"],
            "destinatario":      e["destinatario"],
            "cidade":            e["cidade"],
            "uf":                e["uf"],
            "data_agenda":       e["data_agenda"],
            "hora_agenda":       e["hora_agenda"],
            "info_agendamento":  e["info_agendamento"],
        }
        for e in entregas if e["agendada"] and e["data_agenda"]
    ]

    return {
        "id_manifesto":           id_manifesto,
        "numero_manifesto":       numero,
        "veiculo_tipo":           veiculo_tipo,
        "veiculo_codigo":         veiculo_cod,
        "placa":                  placa,
        "num_eixos":              num_eixos,
        "capacidade_peso_kg":     cap_peso_kg,
        "motorista":              motorista,
        "filial_origem":          filial,
        "linha_operacao":         linha_op,
        "regiao":                 regiao,
        "data_roteirizacao":      data_rot,
        "total_entregas":         total_entregas,
        "total_peso_kg":          total_peso,
        "total_valor_mercadoria": total_valor,
        "total_volumes":          total_volumes,
        "km_estimado":            km_estimado,
        "ocupacao_percentual":    ocupacao_perc,
        "frete_minimo_antt":      None,   # calculado pelo Sistema 1 após retorno
        "agendamentos":           agendamentos,
        "entregas":               entregas,
    }


# ---------------------------------------------------------------------------
# Serialização de carga não roteirizada
# ---------------------------------------------------------------------------

def _serializar_nao_roteirizado(row: Dict[str, Any]) -> Dict[str, Any]:
    """Converte uma carga não roteirizada no contrato do Sistema 1."""
    return {
        "nro_documento":         _safe_str(row.get("nro_documento") or row.get("cte")),
        "destinatario":          _safe_str(row.get("destin") or row.get("destinatario")),
        "cidade":                _safe_str(row.get("cidade_dest") or row.get("cidade")),
        "uf":                    _safe_str(row.get("uf")),
        "peso_kg":               _safe_float(row.get("peso_kg") or row.get("peso_calculado")),
        "valor_mercadoria":      _safe_float(row.get("valor_mercadoria") or row.get("vir_mercadoria")),
        "motivo":                _safe_str(row.get("motivo_nao_roteirizavel") or row.get("motivo")),
        "status_triagem":        _safe_str(row.get("status_triagem")),
        "data_limite_entrega":   _safe_date(row.get("data_limite_considerada") or row.get("data_leadtime")),
        "data_agenda":           _safe_date(row.get("data_agenda")),
        "agendada":              _safe_bool(row.get("agendada")),
        "folga_dias":            _safe_float(row.get("folga_dias")),
        "status_folga":          _safe_str(row.get("status_folga")),
        "mesorregiao":           _safe_str(row.get("mesorregiao")),
        "sub_regiao":            _safe_str(row.get("sub_regiao") or row.get("subregiao")),
    }


# ---------------------------------------------------------------------------
# Função principal: montar o contrato completo de retorno
# ---------------------------------------------------------------------------

def montar_resposta_sistema1(resultado_pipeline: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma o resultado bruto do pipeline no contrato JSON estruturado
    para consumo pelo Sistema 1 (React + Supabase).

    Estrutura de retorno:
    {
      "status": "sucesso" | "erro",
      "erro": null | { "codigo": ..., "mensagem": ... },
      "resumo": { KPIs do dashboard },
      "encadeamento": [ { etapa, entrada, saida, remanescente, detalhes } ],
      "manifestos": [ { cabeçalho + agendamentos + entregas } ],
      "nao_roteirizados": [ { campos de diagnóstico } ],
      "cargas_agendamento_futuro": [ { campos } ],
      "cargas_agenda_vencida": [ { campos } ],
    }
    """
    status = resultado_pipeline.get("status", "sucesso")
    erro   = resultado_pipeline.get("erro")

    # ---- Resumo / KPIs ----
    resumo_raw = resultado_pipeline.get("resumo", {})
    encadeamento = resultado_pipeline.get("encadeamento", [])

    # Extrair contagens do encadeamento do M3 para o dashboard
    m3_det = next((e.get("detalhes", {}) for e in encadeamento if e.get("etapa") == "M3_triagem"), {})

    resumo = {
        "data_base_roteirizacao":    _safe_date(
            next((e.get("detalhes", {}).get("data_base") for e in encadeamento if e.get("etapa") == "M2_enriquecimento"), None)
        ),
        "total_cargas_entrada":      _safe_int(resumo_raw.get("total_linhas_entrada")),
        "total_manifestos":          _safe_int(resumo_raw.get("total_manifestos")),
        "total_itens_manifestados":  _safe_int(resumo_raw.get("total_itens_manifestados")),
        "total_nao_roteirizados":    _safe_int(resumo_raw.get("total_nao_roteirizados")),
        "total_roteirizaveis":       _safe_int(m3_det.get("roteirizavel")),
        "total_agendamento_futuro":  _safe_int(m3_det.get("agendamento_futuro")),
        "total_agenda_vencida":      _safe_int(m3_det.get("agenda_vencida")),
        "total_excecao_triagem":     _safe_int(m3_det.get("excecao_triagem")),
        "km_total_frota":            _safe_float(resumo_raw.get("km_total_frota")),
        "ocupacao_media_percentual": _safe_float(resumo_raw.get("ocupacao_media_perc")),
        "tempo_processamento_ms":    _safe_int(resumo_raw.get("tempo_processamento_ms")),
    }

    # ---- JOIN: agrupar itens por manifesto_id ----
    # O pipeline retorna manifestos (cabeçalhos) e itens (entregas) separados
    # Precisamos fazer o JOIN para montar o manifesto completo com suas entregas
    itens_raw = resultado_pipeline.get("itens", [])
    itens_por_manifesto: Dict[str, List[Dict]] = {}
    for item in itens_raw:
        if isinstance(item, dict):
            mid = _safe_str(item.get("manifesto_id"))
            if mid:
                if mid not in itens_por_manifesto:
                    itens_por_manifesto[mid] = []
                itens_por_manifesto[mid].append(item)

    # ---- Manifestos ----
    manifestos_raw = resultado_pipeline.get("manifestos", [])
    manifestos = []
    for i, m in enumerate(manifestos_raw):
        if isinstance(m, dict):
            mid = _safe_str(m.get("manifesto_id"))
            # Injetar as entregas no manifesto antes de serializar
            m_com_entregas = dict(m)
            m_com_entregas["entregas"] = itens_por_manifesto.get(mid, [])
            manifestos.append(_serializar_manifesto(m_com_entregas, i + 1))

    # ---- Não roteirizados (todos os que não viraram manifesto) ----
    nao_rot_raw = resultado_pipeline.get("nao_roteirizados", [])
    nao_roteirizados = [
        _serializar_nao_roteirizado(r)
        for r in nao_rot_raw
        if isinstance(r, dict)
    ]

    # Separar por categoria para facilitar o frontend
    cargas_agendamento_futuro = [r for r in nao_roteirizados if r["status_triagem"] == "agendamento_futuro"]
    cargas_agenda_vencida     = [r for r in nao_roteirizados if r["status_triagem"] == "agenda_vencida"]
    cargas_excecao            = [r for r in nao_roteirizados if r["status_triagem"] == "excecao_triagem"]
    cargas_nao_alocadas       = [r for r in nao_roteirizados if r["status_triagem"] == "roteirizavel"]

    return {
        "status":                    status,
        "erro":                      erro,
        "resumo":                    resumo,
        "encadeamento":              encadeamento,
        "manifestos":                manifestos,
        "nao_roteirizados":          nao_roteirizados,
        "cargas_agendamento_futuro": cargas_agendamento_futuro,
        "cargas_agenda_vencida":     cargas_agenda_vencida,
        "cargas_excecao_triagem":    cargas_excecao,
        "cargas_nao_alocadas":       cargas_nao_alocadas,
    }
