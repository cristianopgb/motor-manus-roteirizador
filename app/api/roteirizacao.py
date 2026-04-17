from __future__ import annotations
from fastapi import APIRouter, HTTPException, Request
from app.models.schemas import RoteirizacaoPayload, RoteirizacaoResponse
from app.services.pipeline_service import executar_pipeline_completo
import logging

logger = logging.getLogger("motor.api")
router = APIRouter()

@router.post("/roteirizar", response_model=RoteirizacaoResponse, summary="Executa pipeline de roteirização M1→M7")
async def roteirizar(payload: RoteirizacaoPayload):
    try:
        payload_dict = payload.model_dump()
        resultado = executar_pipeline_completo(payload_dict)
        resultado["rodada_id"] = payload.rodada_id
        return resultado
    except Exception as e:
        logger.exception("Erro fatal na roteirização")
        raise HTTPException(status_code=500, detail=str(e))
