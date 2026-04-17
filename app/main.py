"""
Motor de Roteirização REC — FastAPI Application
Endpoints: GET /health | POST /roteirizar
"""
from __future__ import annotations
import time
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.models.schemas import RoteirizacaoRequest, RoteirizacaoResponse
from app.services.pipeline_service import executar_pipeline

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("motor-rec")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Motor de Roteirização REC",
    description=(
        "Microsserviço de otimização de rotas para a REC Transportes. "
        "Recebe carteira de entregas e frota, retorna manifestos otimizados."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Middleware de log de requisições
# ---------------------------------------------------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed = round((time.time() - start) * 1000, 1)
    logger.info(
        f"{request.method} {request.url.path} → {response.status_code} ({elapsed}ms)"
    )
    return response

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/", tags=["Status"])
async def root():
    return {
        "servico": "Motor de Roteirização REC",
        "versao": "1.0.0",
        "status": "online",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health", tags=["Status"])
async def health():
    """Liveness probe para o Render e Sistema 1."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post(
    "/roteirizar",
    response_model=RoteirizacaoResponse,
    tags=["Roteirização"],
    summary="Executa o pipeline de roteirização",
    description=(
        "Recebe a carteira de entregas, frota disponível e parâmetros da filial. "
        "Executa o pipeline M1→M5 e retorna manifestos otimizados, "
        "cargas não roteirizáveis e o encadeamento de cada etapa."
    ),
)
async def roteirizar(req: RoteirizacaoRequest):
    """
    Pipeline completo de roteirização:
    - M1: Padronização e validação de dados
    - M2: Triagem operacional (dedicados, restritos, livres)
    - M3: Cálculo de distâncias (Haversine × fator rodoviário)
    - M4: Consolidação em manifestos (First Fit Decreasing)
    - M5: Sequenciamento de entregas (Nearest Neighbor + prioridade)
    """
    logger.info(
        f"[RODADA {req.rodada_id}] Iniciando roteirização | "
        f"Filial: {req.filial.nome} | "
        f"Carteira: {len(req.carteira)} itens | "
        f"Veículos: {len(req.veiculos)} | "
        f"Tipo: {req.tipo_roteirizacao}"
    )

    # Validações de negócio antes do pipeline
    if not req.carteira:
        return RoteirizacaoResponse(
            status="erro",
            mensagem="Carteira vazia. Nenhuma carga para roteirizar.",
            rodada_id=req.rodada_id,
        )

    if not req.veiculos:
        return RoteirizacaoResponse(
            status="erro",
            mensagem="Nenhum veículo disponível para roteirização.",
            rodada_id=req.rodada_id,
        )

    if req.filial.latitude == 0.0 or req.filial.longitude == 0.0:
        return RoteirizacaoResponse(
            status="erro",
            mensagem="Coordenadas da filial inválidas (lat/lon = 0).",
            rodada_id=req.rodada_id,
        )

    if req.tipo_roteirizacao == "frota" and not req.configuracao_frota:
        return RoteirizacaoResponse(
            status="erro",
            mensagem="Tipo 'frota' requer configuracao_frota com ao menos 1 perfil.",
            rodada_id=req.rodada_id,
        )

    start = time.time()

    try:
        resposta = executar_pipeline(req)
    except Exception as e:
        logger.exception(f"[RODADA {req.rodada_id}] Erro inesperado no pipeline: {e}")
        return RoteirizacaoResponse(
            status="erro",
            mensagem=f"Erro interno no motor de roteirização: {str(e)}",
            rodada_id=req.rodada_id,
        )

    elapsed = round(time.time() - start, 2)
    logger.info(
        f"[RODADA {req.rodada_id}] Concluído em {elapsed}s | "
        f"Status: {resposta.status} | "
        f"Manifestos: {len(resposta.manifestos)} | "
        f"Não roteirizados: {len(resposta.nao_roteirizados)}"
    )

    return resposta
