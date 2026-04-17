import sys, json
sys.path.insert(0, "/home/ubuntu/motor-manus-roteirizador")

from app.services.pipeline_service import executar_pipeline_completo

payload = {
    "rodada_id": "TESTE_001",
    "filial": {
        "id": "FIL001", "nome": "Filial SP", "cnpj": "00.000.000/0001-00",
        "latitude": -23.5505, "longitude": -46.6333, "uf": "SP", "cidade": "SAO PAULO"
    },
    "veiculos": [
        {"id":"V1","tipo":"VUC","perfil":"VUC","capacidade_peso_kg":1500,"capacidade_vol_m3":8,"qtd_eixos":2,"max_entregas":10,"max_km_distancia":150,"ocupacao_minima_perc":70,"ocupacao_maxima_perc":100,"ativo":True},
        {"id":"V2","tipo":"TOCO","perfil":"TOCO","capacidade_peso_kg":6000,"capacidade_vol_m3":30,"qtd_eixos":4,"max_entregas":15,"max_km_distancia":400,"ocupacao_minima_perc":70,"ocupacao_maxima_perc":100,"ativo":True},
        {"id":"V3","tipo":"TRUCK","perfil":"TRUCK","capacidade_peso_kg":14000,"capacidade_vol_m3":60,"qtd_eixos":6,"max_entregas":20,"max_km_distancia":800,"ocupacao_minima_perc":70,"ocupacao_maxima_perc":100,"ativo":True},
        {"id":"V4","tipo":"CARRETA","perfil":"CARRETA","capacidade_peso_kg":28000,"capacidade_vol_m3":90,"qtd_eixos":9,"max_entregas":25,"max_km_distancia":2000,"ocupacao_minima_perc":70,"ocupacao_maxima_perc":100,"ativo":True},
    ],
    "carteira": [
        # Clientes SP - cidade (deve fechar em M4/M5.2)
        {"id_carga":"C001","romaneio":"ROM001","nro_documento":"NF001","cte":"CTE001","destinatario":"CLIENTE A","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5505,"longitude_destinatario":-46.6333,"peso_calculado":2000,"vol_m3":10,"data_leadtime":"2026-04-25","agendada":False,"folga_dias":5,"ranking_prioridade_operacional":4},
        {"id_carga":"C002","romaneio":"ROM002","nro_documento":"NF002","cte":"CTE002","destinatario":"CLIENTE A","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5505,"longitude_destinatario":-46.6333,"peso_calculado":1500,"vol_m3":8,"data_leadtime":"2026-04-25","agendada":False,"folga_dias":5,"ranking_prioridade_operacional":4},
        {"id_carga":"C003","romaneio":"ROM003","nro_documento":"NF003","cte":"CTE003","destinatario":"CLIENTE B","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5600,"longitude_destinatario":-46.6400,"peso_calculado":3000,"vol_m3":15,"data_leadtime":"2026-04-24","agendada":False,"folga_dias":4,"ranking_prioridade_operacional":4},
        {"id_carga":"C004","romaneio":"ROM004","nro_documento":"NF004","cte":"CTE004","destinatario":"CLIENTE C","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5700,"longitude_destinatario":-46.6500,"peso_calculado":2500,"vol_m3":12,"data_leadtime":"2026-04-23","agendada":False,"folga_dias":3,"ranking_prioridade_operacional":4},
        # Campinas - sub-região
        {"id_carga":"C005","romaneio":"ROM005","nro_documento":"NF005","cte":"CTE005","destinatario":"CLIENTE D","cidade":"CAMPINAS","uf":"SP","latitude_destinatario":-22.9099,"longitude_destinatario":-47.0626,"peso_calculado":5000,"vol_m3":25,"data_leadtime":"2026-04-26","agendada":False,"sub_regiao":"INTERIOR_SP","folga_dias":6,"ranking_prioridade_operacional":4},
        {"id_carga":"C006","romaneio":"ROM006","nro_documento":"NF006","cte":"CTE006","destinatario":"CLIENTE E","cidade":"CAMPINAS","uf":"SP","latitude_destinatario":-22.9200,"longitude_destinatario":-47.0700,"peso_calculado":4000,"vol_m3":20,"data_leadtime":"2026-04-26","agendada":False,"sub_regiao":"INTERIOR_SP","folga_dias":6,"ranking_prioridade_operacional":4},
        # Agendado urgente
        {"id_carga":"C007","romaneio":"ROM007","nro_documento":"NF007","cte":"CTE007","destinatario":"CLIENTE F","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5800,"longitude_destinatario":-46.6600,"peso_calculado":800,"vol_m3":4,"data_agenda":"2026-04-18","agendada":True,"folga_dias":1,"ranking_prioridade_operacional":2},
        # Dedicado
        {"id_carga":"C008","romaneio":"ROM008","nro_documento":"NF008","cte":"CTE008","destinatario":"CLIENTE G","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5900,"longitude_destinatario":-46.6700,"peso_calculado":10000,"vol_m3":50,"data_leadtime":"2026-04-25","agendada":False,"veiculo_exclusivo_flag":True,"folga_dias":5,"ranking_prioridade_operacional":4},
        # Restrição VUC
        {"id_carga":"C009","romaneio":"ROM009","nro_documento":"NF009","cte":"CTE009","destinatario":"CLIENTE H","cidade":"SAO PAULO","uf":"SP","latitude_destinatario":-23.5400,"longitude_destinatario":-46.6200,"peso_calculado":900,"vol_m3":4,"data_leadtime":"2026-04-25","agendada":False,"restricao_veiculo":"VUC","folga_dias":5,"ranking_prioridade_operacional":4},
        # Sem coordenadas (deve ser descartado no M1)
        {"id_carga":"C010","romaneio":"ROM010","nro_documento":"NF010","cte":"CTE010","destinatario":"CLIENTE I","cidade":"DESCONHECIDA","uf":"SP","latitude_destinatario":None,"longitude_destinatario":None,"peso_calculado":500,"vol_m3":2,"data_leadtime":"2026-04-25","agendada":False},
    ],
    "geo": [
        {"cidade":"SAO PAULO","uf":"SP","cidade_chave":"SAO_PAULO","uf_chave":"SP","latitude":-23.5505,"longitude":-46.6333,"subregiao":"GRANDE_SP","mesorregiao":"METROPOLITANA_SP"},
        {"cidade":"CAMPINAS","uf":"SP","cidade_chave":"CAMPINAS","uf_chave":"SP","latitude":-22.9099,"longitude":-47.0626,"subregiao":"INTERIOR_SP","mesorregiao":"CAMPINAS_PIRACICABA"},
    ],
    "parametros": {
        "data_base_roteirizacao": "2026-04-17",
        "tipo_roteirizacao": "padrao",
        "fator_km_rodoviario": 1.20,
        "km_dia_operacional": 400.0,
        "latitude_filial": -23.5505,
        "longitude_filial": -46.6333,
    }
}

print("=" * 60)
print("INICIANDO TESTE DO PIPELINE COMPLETO M1→M7")
print("=" * 60)

resultado = executar_pipeline_completo(payload)

print(f"\nSTATUS: {resultado['status']}")
print(f"\nRESUMO:")
for k, v in resultado.get("resumo", {}).items():
    print(f"  {k}: {v}")

print(f"\nENCEADEAMENTO ({len(resultado.get('encadeamento', []))} etapas):")
for e in resultado.get("encadeamento", []):
    print(f"  {e['etapa']:40s} entrada={e['entrada']:4d} saida={e['saida_principal']:4d} rem={e['remanescente']:4d}")

print(f"\nMANIFESTOS ({len(resultado.get('manifestos', []))}):")
for m in resultado.get("manifestos", []):
    print(f"  {m.get('manifesto_id')} | {m.get('veiculo_tipo')} | itens={m.get('qtd_itens')} | ocup={m.get('ocupacao_oficial_perc')}% | km={m.get('km_rota_total')}")

print(f"\nNÃO ROTEIRIZADOS ({len(resultado.get('nao_roteirizados', []))}):")
for nr in resultado.get("nao_roteirizados", []):
    print(f"  {nr.get('id_carga')} | {nr.get('destinatario')} | motivo={nr.get('motivo_nao_roteirizavel') or nr.get('motivo_rejeicao_m3_1') or nr.get('status_triagem')}")

if resultado.get("erros"):
    print(f"\nERROS ({len(resultado['erros'])}):")
    for e in resultado["erros"]:
        print(f"  {e['etapa']}: {e['erro'][:100]}")

print("\n" + "=" * 60)
print("TESTE CONCLUÍDO")
