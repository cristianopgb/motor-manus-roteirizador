"""
Teste local do Motor de Roteirização REC.
Executa o pipeline diretamente sem subir o servidor HTTP.
"""
import json
from app.models.schemas import RoteirizacaoRequest
from app.services.pipeline_service import executar_pipeline

payload = {
    "rodada_id": "test-rodada-001",
    "upload_id": "test-upload-001",
    "usuario_id": "test-user-001",
    "filial_id": "test-filial-001",
    "data_base_roteirizacao": "2026-04-17T10:00:00Z",
    "tipo_roteirizacao": "carteira",
    "filial": {
        "id": "test-filial-001",
        "nome": "Filial São Paulo",
        "cidade": "São Paulo",
        "uf": "SP",
        "latitude": -23.5505,
        "longitude": -46.6333
    },
    "veiculos": [
        {
            "id": "v1",
            "perfil": "TRUCK",
            "placa": "ABC-1234",
            "qtd_eixos": 5,
            "capacidade_peso_kg": 14000,
            "capacidade_vol_m3": 40.0,
            "max_entregas": 20,
            "max_km_distancia": 300,
            "ocupacao_minima_perc": 60.0,
            "tipo_frota": "proprio",
            "ativo": True
        },
        {
            "id": "v2",
            "perfil": "VUC",
            "placa": "DEF-5678",
            "qtd_eixos": 2,
            "capacidade_peso_kg": 3500,
            "capacidade_vol_m3": 12.0,
            "max_entregas": 15,
            "max_km_distancia": 150,
            "ocupacao_minima_perc": 50.0,
            "tipo_frota": "proprio",
            "ativo": True
        },
        {
            "id": "v3",
            "perfil": "TOCO",
            "placa": "GHI-9012",
            "qtd_eixos": 4,
            "capacidade_peso_kg": 8000,
            "capacidade_vol_m3": 25.0,
            "max_entregas": 18,
            "max_km_distancia": 250,
            "ocupacao_minima_perc": 55.0,
            "tipo_frota": "proprio",
            "ativo": True
        }
    ],
    "regionalidades": [
        {"cidade": "CAMPINAS", "uf": "SP", "mesorregiao": "CAMPINAS", "microrregiao": "CAMPINAS"},
        {"cidade": "SANTOS", "uf": "SP", "mesorregiao": "LITORAL", "microrregiao": "BAIXADA SANTISTA"},
        {"cidade": "SOROCABA", "uf": "SP", "mesorregiao": "SOROCABA", "microrregiao": "SOROCABA"},
    ],
    "configuracao_frota": [],
    "parametros": {
        "usuario_id": "test-user-001",
        "usuario_nome": "Operador Teste",
        "filial_id": "test-filial-001",
        "filial_nome": "Filial São Paulo",
        "upload_id": "test-upload-001",
        "rodada_id": "test-rodada-001",
        "data_execucao": "2026-04-17T10:00:00Z",
        "data_base_roteirizacao": "2026-04-17T10:00:00Z",
        "origem_sistema": "sistema_1",
        "modelo_roteirizacao": "padrao",
        "tipo_roteirizacao": "carteira",
        "configuracao_frota": [],
        "filtros_aplicados": {}
    },
    "carteira": [
        # Campinas — 3 entregas normais
        {"nro_doc": 1001, "romane": 101, "destin": "EMPRESA A", "cidade": "CAMPINAS", "uf": "SP",
         "latitude": -22.9056, "longitude": -47.0608, "peso": 1200, "peso_cubico": 3.5,
         "peso_calculo": 1200, "vlr_merc": 15000, "qtd": 10, "tipo_carga": "CARGA GERAL",
         "prioridade": "NORMAL", "carro_dedicado": False},
        {"nro_doc": 1002, "romane": 102, "destin": "EMPRESA B", "cidade": "CAMPINAS", "uf": "SP",
         "latitude": -22.9100, "longitude": -47.0650, "peso": 800, "peso_cubico": 2.0,
         "peso_calculo": 800, "vlr_merc": 8000, "qtd": 5, "tipo_carga": "CARGA GERAL",
         "prioridade": "ALTA", "carro_dedicado": False},
        {"nro_doc": 1003, "romane": 103, "destin": "EMPRESA C", "cidade": "CAMPINAS", "uf": "SP",
         "latitude": -22.9200, "longitude": -47.0700, "peso": 2500, "peso_cubico": 6.0,
         "peso_calculo": 2500, "vlr_merc": 30000, "qtd": 20, "tipo_carga": "CARGA GERAL",
         "prioridade": "URGENTE", "carro_dedicado": False},
        # Santos — 2 entregas
        {"nro_doc": 1004, "romane": 104, "destin": "EMPRESA D", "cidade": "SANTOS", "uf": "SP",
         "latitude": -23.9618, "longitude": -46.3322, "peso": 3000, "peso_cubico": 8.0,
         "peso_calculo": 3000, "vlr_merc": 45000, "qtd": 15, "tipo_carga": "GRANEL SÓLIDO",
         "prioridade": "NORMAL", "carro_dedicado": False},
        {"nro_doc": 1005, "romane": 105, "destin": "EMPRESA E", "cidade": "SANTOS", "uf": "SP",
         "latitude": -23.9700, "longitude": -46.3400, "peso": 1500, "peso_cubico": 4.0,
         "peso_calculo": 1500, "vlr_merc": 20000, "qtd": 8, "tipo_carga": "GRANEL SÓLIDO",
         "prioridade": "MEDIA", "carro_dedicado": False},
        # Sorocaba — 2 entregas, 1 dedicada
        {"nro_doc": 1006, "romane": 106, "destin": "EMPRESA F", "cidade": "SOROCABA", "uf": "SP",
         "latitude": -23.5015, "longitude": -47.4526, "peso": 4000, "peso_cubico": 10.0,
         "peso_calculo": 4000, "vlr_merc": 60000, "qtd": 25, "tipo_carga": "CARGA GERAL",
         "prioridade": "ALTA", "carro_dedicado": True},
        {"nro_doc": 1007, "romane": 107, "destin": "EMPRESA G", "cidade": "SOROCABA", "uf": "SP",
         "latitude": -23.5100, "longitude": -47.4600, "peso": 900, "peso_cubico": 2.5,
         "peso_calculo": 900, "vlr_merc": 12000, "qtd": 6, "tipo_carga": "CARGA GERAL",
         "prioridade": "BAIXA", "carro_dedicado": False},
        # Carga com restrição de veículo VUC
        {"nro_doc": 1008, "romane": 108, "destin": "EMPRESA H", "cidade": "SÃO PAULO", "uf": "SP",
         "latitude": -23.5600, "longitude": -46.6500, "peso": 500, "peso_cubico": 1.5,
         "peso_calculo": 500, "vlr_merc": 5000, "qtd": 3, "tipo_carga": "CARGA GERAL",
         "prioridade": "NORMAL", "restricao_veiculo": "VUC", "carro_dedicado": False},
        # Carga sem coordenadas (deve ser descartada no M1)
        {"nro_doc": 1009, "romane": 109, "destin": "EMPRESA I", "cidade": "RIBEIRÃO PRETO", "uf": "SP",
         "latitude": None, "longitude": None, "peso": 700, "peso_cubico": 2.0,
         "peso_calculo": 700, "vlr_merc": 9000, "qtd": 4, "tipo_carga": "CARGA GERAL",
         "prioridade": "NORMAL", "carro_dedicado": False},
        # Carga sem peso (deve ser descartada no M1)
        {"nro_doc": 1010, "romane": 110, "destin": "EMPRESA J", "cidade": "GUARULHOS", "uf": "SP",
         "latitude": -23.4543, "longitude": -46.5333, "peso": 0, "peso_cubico": 0,
         "peso_calculo": 0, "vlr_merc": 3000, "qtd": 2, "tipo_carga": "CARGA GERAL",
         "prioridade": "NORMAL", "carro_dedicado": False},
    ]
}

if __name__ == "__main__":
    print("=" * 60)
    print("TESTE LOCAL — Motor de Roteirização REC")
    print("=" * 60)

    req = RoteirizacaoRequest(**payload)
    resposta = executar_pipeline(req)

    print(f"\nStatus: {resposta.status}")
    print(f"Mensagem: {resposta.mensagem}")

    if resposta.resumo:
        r = resposta.resumo
        print(f"\n--- RESUMO ---")
        print(f"Total carteira:        {r.total_carteira}")
        print(f"Total roteirizado:     {r.total_roteirizado}")
        print(f"Não roteirizados:      {r.total_nao_roteirizado}")
        print(f"Total manifestos:      {r.total_manifestos}")
        print(f"  Fechados:            {r.total_manifestos_fechados}")
        print(f"  Compostos:           {r.total_manifestos_compostos}")
        print(f"Ocupação média peso:   {r.ocupacao_media_peso_perc}%")
        print(f"Ocupação média volume: {r.ocupacao_media_volume_perc}%")
        print(f"KM total estimado:     {r.km_total_estimado} km")

    print(f"\n--- MANIFESTOS ({len(resposta.manifestos)}) ---")
    for m in resposta.manifestos:
        print(f"\n  [{m.tipo.upper()}] {m.manifesto_id[:8]}... | Veículo: {m.veiculo_perfil} ({m.qtd_eixos} eixos)")
        print(f"  Entregas: {m.qtd_entregas} | Peso: {m.peso_total_kg:.0f}kg | Ocup: {m.ocupacao_peso_perc}% | KM: {m.km_total_estimado:.1f}")
        for e in m.entregas:
            print(f"    #{e.sequencia:02d} {e.destin} — {e.cidade}/{e.uf} | {e.peso:.0f}kg | {e.km_acumulado:.1f}km acum.")

    if resposta.nao_roteirizados:
        print(f"\n--- NÃO ROTEIRIZADOS ({len(resposta.nao_roteirizados)}) ---")
        for nr in resposta.nao_roteirizados:
            print(f"  Doc {nr.nro_doc} | {nr.destin} | {nr.cidade} | Motivo: {nr.motivo}")

    print(f"\n--- ENCADEAMENTO (M1→M5) ---")
    for log in resposta.encadeamento:
        print(f"  {log.etapa}: {log.qtd_entrada} → {log.qtd_saida} (descartados: {log.qtd_descartada})")
        print(f"    {log.detalhes}")

    print("\n" + "=" * 60)
    print("Teste concluído com sucesso!")
    print("=" * 60)
