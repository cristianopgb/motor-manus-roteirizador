from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class FilialInput(BaseModel):
    id: Optional[str] = None
    nome: Optional[str] = None
    cnpj: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    uf: Optional[str] = None
    cidade: Optional[str] = None

class VeiculoInput(BaseModel):
    id: Optional[str] = None
    tipo: Optional[str] = None
    perfil: Optional[str] = None
    placa: Optional[str] = None
    capacidade_peso_kg: Optional[float] = None
    capacidade_vol_m3: Optional[float] = None
    qtd_eixos: Optional[int] = None
    max_entregas: Optional[int] = None
    max_km_distancia: Optional[float] = None
    ocupacao_minima_perc: Optional[float] = None
    ocupacao_maxima_perc: Optional[float] = None
    ativo: Optional[bool] = True

class CarteiraItemInput(BaseModel):
    model_config = {"extra": "allow"}
    id_carga: Optional[str] = None
    romaneio: Optional[str] = None
    nro_documento: Optional[str] = None
    cte: Optional[str] = None
    destinatario: Optional[str] = None
    cnpj_destinatario: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    cep: Optional[str] = None
    latitude_destinatario: Optional[float] = None
    longitude_destinatario: Optional[float] = None
    peso_calculado: Optional[float] = None
    peso_kg: Optional[float] = None
    vol_m3: Optional[float] = None
    valor_nf: Optional[float] = None
    data_leadtime: Optional[str] = None
    data_agenda: Optional[str] = None
    agendada: Optional[bool] = False
    restricao_veiculo: Optional[str] = None
    veiculo_exclusivo_flag: Optional[bool] = False
    prioridade_embarque: Optional[Any] = None
    sub_regiao: Optional[str] = None
    mesorregiao: Optional[str] = None

class GeoItemInput(BaseModel):
    model_config = {"extra": "allow"}
    cidade: Optional[str] = None
    uf: Optional[str] = None
    cidade_chave: Optional[str] = None
    uf_chave: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    subregiao: Optional[str] = None
    mesorregiao: Optional[str] = None

class ParametrosInput(BaseModel):
    data_base_roteirizacao: Optional[str] = None
    data_execucao: Optional[str] = None
    tipo_roteirizacao: Optional[str] = "padrao"
    fator_km_rodoviario: Optional[float] = 1.20
    km_dia_operacional: Optional[float] = 400.0
    latitude_filial: Optional[float] = None
    longitude_filial: Optional[float] = None
    ocupacao_minima_padrao: Optional[float] = 70.0
    ocupacao_maxima_padrao: Optional[float] = 100.0

class RoteirizacaoPayload(BaseModel):
    rodada_id: Optional[str] = None
    filial: Optional[FilialInput] = None
    veiculos: List[VeiculoInput] = Field(default_factory=list)
    carteira: List[CarteiraItemInput] = Field(default_factory=list)
    geo: List[GeoItemInput] = Field(default_factory=list)
    parametros: Optional[ParametrosInput] = None

class EncadeamentoEtapa(BaseModel):
    etapa: str
    entrada: int
    saida_principal: int
    remanescente: int
    detalhes: Dict[str, Any] = Field(default_factory=dict)

class ResumoRoteirizacao(BaseModel):
    total_linhas_entrada: int
    total_manifestos: int
    total_itens_manifestados: int
    total_nao_roteirizados: int
    km_total_frota: float
    ocupacao_media_perc: float
    tempo_processamento_ms: int

class ErroEtapa(BaseModel):
    etapa: str
    erro: str

class RoteirizacaoResponse(BaseModel):
    status: str
    rodada_id: Optional[str] = None
    manifestos: List[Dict[str, Any]] = Field(default_factory=list)
    itens: List[Dict[str, Any]] = Field(default_factory=list)
    nao_roteirizados: List[Dict[str, Any]] = Field(default_factory=list)
    encadeamento: List[EncadeamentoEtapa] = Field(default_factory=list)
    resumo: Optional[ResumoRoteirizacao] = None
    erros: List[ErroEtapa] = Field(default_factory=list)
    etapa_falha: Optional[str] = None
    erro: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    versao: str
    modulos: List[str]
