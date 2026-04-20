from typing import Dict, Any, List


def montar_resposta_sucesso(resultado_pipeline: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta a resposta padronizada de sucesso a partir do resultado do pipeline.
    """

    # Valores defensivos (caso algum módulo ainda não preencha tudo)
    manifestos_fechados = resultado_pipeline.get("manifestos_fechados", [])
    manifestos_compostos = resultado_pipeline.get("manifestos_compostos", [])
    nao_roteirizados = resultado_pipeline.get("nao_roteirizados", [])
    logs = resultado_pipeline.get("logs", [])

    total_carteira = resultado_pipeline.get("total_carteira", 0)
    total_roteirizado = resultado_pipeline.get("total_roteirizado", 0)
    total_nao_roteirizado = resultado_pipeline.get("total_nao_roteirizado", 0)

    total_manifestos_fechados = len(manifestos_fechados)
    total_manifestos_compostos = len(manifestos_compostos)

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
