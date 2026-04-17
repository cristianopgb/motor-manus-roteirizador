"""Inspecionar o retorno atual do motor e mapear o que existe vs o que falta."""
import json, sys
sys.path.insert(0, '.')
from app.services.pipeline_service import executar_pipeline_completo

with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

result = executar_pipeline_completo(payload)

print("=== CHAVES DO RESULTADO GERAL ===")
print(list(result.keys()))
print()

manifestos = result.get('manifestos', [])
print(f"Total manifestos: {len(manifestos)}")
if manifestos:
    m = manifestos[0]
    print()
    print("=== CHAVES DO MANIFESTO [0] ===")
    print(list(m.keys()))
    print()
    print("=== AMOSTRA MANIFESTO [0] ===")
    for k, v in m.items():
        if k != 'entregas':
            print(f"  {k}: {v}")
    print()
    entregas = m.get('entregas', [])
    print(f"  Total entregas: {len(entregas)}")
    if entregas:
        print()
        print("=== CHAVES DE CADA ENTREGA ===")
        print(list(entregas[0].keys()))
        print()
        print("=== AMOSTRA ENTREGA [0] ===")
        for k, v in entregas[0].items():
            print(f"  {k}: {v}")

print()
nr = result.get('nao_roteirizados', [])
print(f"Total não roteirizados: {len(nr)}")
if nr:
    print()
    print("=== CHAVES DE NÃO ROTEIRIZADOS ===")
    print(list(nr[0].keys()))
    print()
    print("=== AMOSTRA NÃO ROTEIRIZADO [0] ===")
    for k, v in nr[0].items():
        print(f"  {k}: {v}")

print()
print("=== ENCADEAMENTO ===")
enc = result.get('encadeamento', [])
for e in enc:
    print(f"  {e}")

print()
print("=== RESUMO ===")
resumo = result.get('resumo', {})
for k, v in resumo.items():
    print(f"  {k}: {v}")
