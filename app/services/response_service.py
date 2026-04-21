from typing import Dict, Any, List


def _safe_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return value
    return []


def _is_manifesto_fechado(manifesto: Dict[str, Any]) -> bool:
    origem = str(manifesto.get("origem_manifesto_tipo", "")).strip().lower()
    if "manifesto_fechado" in origem:
        return True

    origem_modulo = str(manifesto.get("origem_manifesto_modulo", "")).strip().upper()
    if origem_modulo == "M4":
        return True

    return False


def _is_manifesto_composto(manifesto: Dict[str, Any]) -> bool:
    if _is_manifesto_fechado(manifesto):
        return False

    origem = str(manifesto.get("origem_manifesto_tipo", "")).strip().lower()
    if origem.startswith("pre_manifesto_"):
        return True

    origem_modulo = str(manifesto.get("origem_manifesto_modulo", "")).strip().upper()
    return origem_modulo in {"M5.2", "M5.3B", "M5.4B"}


def montar_resposta_sucesso(resultado_pipeline: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta a resposta padronizada de sucesso a partir do resultado do pipeline.
    """

    manifestos_fechados = _safe_list(resultado_pipeline.get("manifestos_fechados"))
    manifestos_compostos = _safe_list(resultado_pipeline.get("manifestos_compostos"))
    nao_roteirizados = _safe_list(resultado_pipeline.get("nao_roteirizados"))
    logs = resultado_pipeline.get("logs", [])

    manifestos_m7 = _safe_list(resultado_pipeline.get("manifestos_m7"))
    resumo_negocio = resultado_pipeline.get("resumo_negocio", {}) or {}

    if not manifestos_fechados and manifestos_m7:
        manifestos_fechados = [m for m in manifestos_m7 if _is_manifesto_fechado(m)]

    if not manifestos_compostos and manifestos_m7:
        manifestos_compostos = [m for m in manifestos_m7 if _is_manifesto_composto(m)]

    total_carteira = resultado_pipeline.get(
        "total_carteira",
        resumo_negocio.get("total_carteira", 0),
    )
    total_roteirizado = resultado_pipeline.get(
        "total_roteirizado",
        resumo_negocio.get("total_itens_manifestos_sequenciados_m7", 0),
    )
    total_nao_roteirizado = resultado_pipeline.get(
        "total_nao_roteirizado",
        resultado_pipeline.get(
            "total_nao_roteirizados",
            len(nao_roteirizados) if nao_roteirizados else resumo_negocio.get("total_remanescente_m6_2", 0),
        ),
    )

    total_manifestos_fechados = len(manifestos_fechados)
    total_manifestos_compostos = len(manifestos_compostos)

    print("[RESPONSE] chaves de saída disponíveis:", list(resultado_pipeline.keys()))
    print("[RESPONSE] total manifestos fechados serializados:", len(manifestos_fechados))
    print("[RESPONSE] total manifestos compostos serializados:", len(manifestos_compostos))
    print("[RESPONSE] total nao roteirizados serializados:", len(nao_roteirizados))

    resumo = {
        "total_carteira": total_carteira,
        "total_roteirizado": total_roteirizado,
        "total_nao_roteirizado": total_nao_roteirizado,
        "total_manifestos_fechados": total_manifestos_fechados,
        "total_manifestos_compostos": total_manifestos_compostos,
        "ocupacao_media_peso": resultado_pipeline.get("ocupacao_media_peso", 0),
        "ocupacao_media_volume": resultado_pipeline.get("ocupacao_media_volume", 0),
    }

    return {
        "status": "sucesso",
        "mensagem": "Roteirização executada com sucesso",
        "resumo": resumo,
        "manifestos_fechados": manifestos_fechados,
        "manifestos_compostos": manifestos_compostos,
        "nao_roteirizados": nao_roteirizados,
        "logs": logs,
    }


def montar_resposta_erro(mensagem: str, tipo_erro: str = "ERRO") -> Dict[str, Any]:
    """
    Monta resposta padronizada de erro.
    Não levanta exceção — retorna erro controlado.
    """

    return {
        "status": "erro",
        "mensagem": mensagem,
        "tipo_erro": tipo_erro,
        "resumo": {},
        "manifestos_fechados": [],
        "manifestos_compostos": [],
        "nao_roteirizados": [],
        "logs": [
            {
                "modulo": "erro",
                "status": "falha",
                "mensagem": mensagem,
                "quantidade_entrada": None,
                "quantidade_saida": None,
            }
        ],
    }
