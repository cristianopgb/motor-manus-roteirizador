from dataclasses import dataclass
from typing import Optional


@dataclass
class ResultadoManifesto:
    manifesto_id: str
    tipo_manifesto: str
    veiculo_id: Optional[str] = None
    placa: Optional[str] = None
    perfil_veiculo: Optional[str] = None
    qtd_ctes: int = 0
    qtd_entregas: int = 0
    peso_total: float = 0.0
    volume_total: float = 0.0
    ocupacao_peso_perc: float = 0.0
    ocupacao_volume_perc: float = 0.0
    km_estimado: float = 0.0


@dataclass
class ResultadoNaoRoteirizado:
    identificador_carga: str
    destinatario: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    peso: float = 0.0
    volume: float = 0.0
    motivo_nao_roteirizado: str = ""


@dataclass
class LogPipeline:
    modulo: str
    status: str
    mensagem: str
    quantidade_entrada: Optional[int] = None
    quantidade_saida: Optional[int] = None
