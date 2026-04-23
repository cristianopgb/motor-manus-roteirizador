alter table public.auditoria_pipeline_modular add column if not exists peso_total_subregiao text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_linhas_subregiao text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_clientes_subregiao text;
alter table public.auditoria_pipeline_modular add column if not exists km_referencia_subregiao text;
alter table public.auditoria_pipeline_modular add column if not exists status_perfil_subregiao text;
alter table public.auditoria_pipeline_modular add column if not exists motivo_status_perfil_subregiao text;
alter table public.auditoria_pipeline_modular add column if not exists subregiao_elegivel_m5_3 text;
alter table public.auditoria_pipeline_modular add column if not exists motivo_status_subregiao_m5_3 text;
alter table public.auditoria_pipeline_modular add column if not exists ordem_subregiao_m5_3 text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_cidades text;

create index if not exists idx_auditoria_pipeline_modular_snapshot_m5_3
    on public.auditoria_pipeline_modular (snapshot_nome, modulo);
