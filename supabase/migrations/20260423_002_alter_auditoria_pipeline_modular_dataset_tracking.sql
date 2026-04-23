alter table public.auditoria_pipeline_modular add column if not exists chave_linha_dataset text;
alter table public.auditoria_pipeline_modular add column if not exists snapshot_nome text;
alter table public.auditoria_pipeline_modular add column if not exists modulo_origem text;
alter table public.auditoria_pipeline_modular add column if not exists tipo_registro text;

create index if not exists idx_auditoria_pipeline_modular_chave_linha_dataset
    on public.auditoria_pipeline_modular (chave_linha_dataset);

create index if not exists idx_auditoria_pipeline_modular_teste_modulo_ordem
    on public.auditoria_pipeline_modular (teste_id, modulo, ordem_modulo);

create index if not exists idx_auditoria_pipeline_modular_teste_chave_linha_dataset
    on public.auditoria_pipeline_modular (teste_id, chave_linha_dataset);
