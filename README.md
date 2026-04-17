# Motor de Roteirização REC

Microsserviço Python (FastAPI) de otimização de rotas para a REC Transportes.
Parte do Sistema 2 do Roteirizador REC — recebe carteira de entregas e frota, retorna manifestos otimizados.

## Pipeline (M1 → M5)

| Módulo | Responsabilidade |
|--------|-----------------|
| M1 — Padronização | Limpeza, normalização de tipos, descarte de registros sem coordenadas ou peso |
| M2 — Triagem | Separação em dedicados (carro_dedicado), restritos (restricao_veiculo) e livres |
| M3 — Distâncias | Cálculo Haversine × fator rodoviário 1.25 para cada entrega |
| M4 — Consolidação | First Fit Decreasing por peso + prioridade → manifestos |
| M5 — Sequenciamento | Nearest Neighbor por manifesto + priorização URGENTE/ALTA |

## Endpoints

- `GET /` — status do serviço
- `GET /health` — liveness probe (usado pelo Render e pelo Sistema 1)
- `POST /roteirizar` — executa o pipeline completo
- `GET /docs` — Swagger UI interativo

## Resposta

```json
{
  "status": "sucesso | parcial | erro",
  "mensagem": "...",
  "rodada_id": "uuid",
  "resumo": { "total_carteira": 10, "total_roteirizado": 8, ... },
  "manifestos": [ { "manifesto_id": "...", "entregas": [...] } ],
  "nao_roteirizados": [ { "nro_doc": 1009, "motivo": "..." } ],
  "encadeamento": [ { "etapa": "M1", "qtd_entrada": 10, "qtd_saida": 8, ... } ]
}
```

## Deploy no Render

1. Conecte este repositório no Render como **Web Service**
2. **Build Command:** `pip install -r requirements.txt`
3. **Start Command:** `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. **Python version:** 3.11

## Teste local

```bash
pip install -r requirements.txt
python test_local.py
```
