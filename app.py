"""
APP:       portal-fin-contas-receber (Databricks App · Python/Flask)
O QUE FAZ: Painel de Contas a Receber, layout v3 da Vera (5 abas: Resumo,
           Liquidação, Devedores, Cobradoras, Meu painel). Monta o JSON do
           contrato v3 (claude/contrato_dados_contas_receber_v3.md) e troca o
           marcador /*__DADOS_JSON__*/null do template.html.
           - Base em memória, renovada de hora em hora em segundo plano.
           - Filtros na URL com parâmetros repetidos (canal, marca, cobrador,
             uf, operador; mcob = cobradora do Meu painel). Lista vazia = todos;
             canal padrão 40 e 50. Cada combinação vira um JSON em cache.
           - Exceções: o mapa (resumo.ufs) ignora UF; cobradoras ignora UF e
             operador.
           - Recebido = só status_titulo 'Liquidado'. Baixa sem Caixa = acordo.
           - Quem não é gestor recebe só o Meu painel, da própria carteira.
FONTES:    data_analytics.contas_receber.vw_fato_financeiro_contas_receber
           data_analytics.contas_receber.vw_cr_eventos_titulo
           data_analytics.contas_receber.vw_cr_liquidacao_titulo
           data_analytics.contas_receber.tb_calendario_recebimento
QUEM USA:  gestores e cobradoras do Contas a Receber (link no hub portal-financeiro).
HISTÓRICO: 05/11/2026 · v2.0–v2.2 · app sobre o template v11.
           06/11/2026 · v3.0–v3.3 · modelo oficial (4 abas), SQL em texto fixo,
                        compactação de títulos, correções do Meu painel.
           10/11/2026 · v3.4 · operador, UF, nome fantasia.
           10/11/2026 · v4.0 · contrato v3 (layout escuro, 5 abas): filtros
                        múltiplos, aba Liquidação (vw_cr_liquidacao_titulo),
                        ranking e árvore pelo atraso real (vw_cr_eventos_titulo,
                        com canal e marca), calendário de dias úteis da
                        tb_calendario_recebimento, títulos completos no Meu painel.
"""

import gzip
import json
import logging
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from functools import lru_cache
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from databricks import sql
from databricks.sdk.core import Config
from flask import Flask, Response, jsonify, request

import config

logging.basicConfig(level=logging.INFO)

PASTA = Path(__file__).resolve().parent
ARQUIVO_HTML = PASTA / config.ARQUIVO_HTML
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
CACHE_SEGUNDOS = int(os.getenv("CACHE_SEGUNDOS", "3600"))
FUSO = ZoneInfo("America/Sao_Paulo")
MARCADOR_DADOS = "/*__DADOS_JSON__*/null"
PORTAL_PADRAO_TEMPLATE = "https://portal-financeiro.databricksapps.com"
SEM_COBRADOR = "SEM COBRADOR"
SEM_OPERADOR = "SEM OPERADOR"
SEM_UF = "SEM UF"
FK = ["canal", "marca", "cobrador", "uf", "operador"]          # filtros da URL
CAMPO = {"canal": "canal", "marca": "marca", "cobrador": "cob", "uf": "uf", "operador": "op"}
CANAL_PADRAO = ["40", "50"]
MESES_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
MESES_LONGO = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
               "agosto", "setembro", "outubro", "novembro", "dezembro"]
DIAS_PT = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
ORIGEM_UF = {"Venda (cliente + marca)": "venda na marca", "Cadastro da loja": "cadastro da loja",
             "Venda (cliente, outra marca)": "venda em outra marca", "Sem UF": "sem UF"}
COLUNAS_TITULOS = ["operador", "conta", "nome_fantasia", "razao_social", "nr_documento", "nr_item",
                   "tipo_documento", "chave_lancamento", "atribuicao", "referencia", "data_documento",
                   "data_vencimento_liquido", "dias_atraso", "valor", "bloqueio_pgto", "canal", "marca",
                   "uf", "origem_uf", "cidade", "cobrador"]

GESTORES = {e.strip().lower() for e in config.GESTORES}
ACESSOS = {
    e.strip().lower(): [str(c).strip() for c in cods]
    for e, cods in config.ACESSOS.items()
}

app = Flask(__name__)
log = app.logger


# =====================================================================
# Conexão com o SQL Warehouse
# =====================================================================
_cfg = None


def _config():
    global _cfg
    if _cfg is None:
        _cfg = Config()
    return _cfg


def _limpar(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()[:10]
    return v


def consultar(query, params=None):
    """Devolve (colunas, linhas como tuplas). Valores só entram por parâmetro."""
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID vazio: falta o recurso 'sql-warehouse' no app "
            "(Editar > Recursos do app > SQL Warehouse, chave sql-warehouse)."
        )
    cfg = _config()
    host = cfg.host.replace("https://", "").rstrip("/")
    with sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
        _retry_stop_after_attempts_count=3,
        _retry_stop_after_attempts_duration=180,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or {})
            cols = [c[0] for c in cur.description]
            return cols, [tuple(_limpar(v) for v in linha) for linha in cur.fetchall()]


def consultar_dicts(query, params=None):
    cols, linhas = consultar(query, params)
    return [dict(zip(cols, l)) for l in linhas]


# =====================================================================
# Calendário · dias úteis da tb_calendario_recebimento; se a tabela não
# responder, vale a regra de reserva (seg–sex menos feriados abaixo).
# =====================================================================
def _pascoa(ano):
    a, b, c = ano % 19, ano // 100, ano % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    return date(ano, (h + l - 7 * m + 114) // 31, (h + l - 7 * m + 114) % 31 + 1)


def _feriados(ano):
    fixos = [(1, 1), (4, 21), (5, 1), (9, 7), (9, 20), (10, 12), (11, 2), (11, 15), (11, 20), (12, 25), (12, 31)]
    p = _pascoa(ano)
    moveis = [p - timedelta(days=48), p - timedelta(days=47), p - timedelta(days=2), p + timedelta(days=60)]
    return {date(ano, m, d) for m, d in fixos} | set(moveis)


FERIADOS = set().union(*(_feriados(a) for a in range(2023, 2031))) | {
    date.fromisoformat(x) for x in getattr(config, "FERIADOS_EXTRAS", [])
}
CAL = {}   # date -> True/False (dia útil), carregado da tb_calendario_recebimento


def eh_util(d):
    u = CAL.get(d)
    return u if u is not None else (d.weekday() < 5 and d not in FERIADOS)


def util_anterior(d):
    x = d - timedelta(days=1)
    while not eh_util(x):
        x -= timedelta(days=1)
    return x


def proximo_util(d):
    x = d + timedelta(days=1)
    while not eh_util(x):
        x += timedelta(days=1)
    return x


def ultimo_dia(ano, mes):
    return date(ano + mes // 12, mes % 12 + 1, 1) - timedelta(days=1)


def mes_add(d, n):
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def dias_uteis_mes(ano, mes):
    return [date(ano, mes, k) for k in range(1, ultimo_dia(ano, mes).day + 1) if eh_util(date(ano, mes, k))]


@lru_cache(maxsize=None)
def data_recebimento(venc):
    """D+1 útil do vencimento ajustado (início do atraso real)."""
    aj = venc if eh_util(venc) else proximo_util(venc)
    return proximo_util(aj)


def datas_da_posicao(pos):
    du = dias_uteis_mes(pos.year, pos.month)
    idx = sum(1 for d in du if d <= pos)
    ma = mes_add(pos, -1)
    du_ant = dias_uteis_mes(ma.year, ma.month)
    mes_ant = du_ant[min(idx, len(du_ant)) - 1] if du_ant and idx else (du_ant[0] if du_ant else ma)
    return {
        "pos": pos, "idx": idx, "dias_mes": du, "dias_ant": du_ant,
        "mes_ant": mes_ant,                                   # mesmo dia útil do mês anterior
        "d0dia": util_anterior(pos),                          # árvore "no dia"
        "d0mes": du_ant[-1] if du_ant else util_anterior(date(pos.year, pos.month, 1)),  # "no mês"
        "ini_mes": date(pos.year, pos.month, 1),
        "fim_mes": ultimo_dia(pos.year, pos.month),
    }


def rotulo_mes(d, ano4=True):
    return f"{MESES_PT[d.month - 1]}/{d.year if ano4 else str(d.year)[2:]}"


def dbr(d):
    return d.strftime("%d/%m/%Y") if d else ""


# =====================================================================
# Consultas · texto fixo (sem montar SQL com variáveis). As datas entram
# como parâmetros nomeados (:pos, :corte...), passados ao conector.
# =====================================================================
SQL_META = """
SELECT
  MAX(CASE WHEN status_titulo = 'Em Aberto' THEN CAST(data_posicao AS DATE) END) AS pos,
  MAX(CASE WHEN status_titulo = 'Liquidado' THEN data_compensacao END)          AS comp
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
"""

SQL_CAL = """
SELECT data, fl_dia_util AS util
FROM data_analytics.contas_receber.tb_calendario_recebimento
"""

SQL_COBRADORES = """
SELECT
  NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), '') AS cod,
  MAX(COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR')) AS nome
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Em Aberto'
GROUP BY 1
"""

# 1) Títulos em aberto da posição (nível título)
SQL_ABERTO = """
SELECT
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  CAST(nr_documento AS STRING) AS doc,
  CAST(nr_item AS STRING) AS item,
  tipo_documento AS tipo,
  data_documento AS emis,
  data_vencimento_liquido AS venc,
  DATEDIFF(CAST(data_posicao AS DATE), data_vencimento_liquido) AS dias,
  CAST(valor_moeda_local AS DOUBLE) AS v,
  COALESCE(TRIM(bloqueio_pgto), '') AS bloq,
  COALESCE(NULLIF(TRIM(operador), ''), 'SEM OPERADOR') AS op,
  COALESCE(NULLIF(TRIM(uf), ''), 'SEM UF') AS uf,
  COALESCE(NULLIF(TRIM(origem_uf), ''), 'Sem UF') AS ufo
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Em Aberto'
  AND tipo_documento IN ('RV', 'RF', 'RR', 'DR', 'RM', 'RP', 'ZN', 'ZV', 'EC')
  AND valor_moeda_local > 0
  AND data_vencimento_liquido IS NOT NULL
"""

# 1b) Dados da loja (uma linha por conta com título em aberto)
SQL_LOJAS = """
SELECT
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  MAX(NULLIF(TRIM(nome), '')) AS razao,
  MAX(NULLIF(TRIM(nome_fantasia), '')) AS fant,
  MAX(NULLIF(TRIM(cidade_cadastro), '')) AS cidade
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Em Aberto'
  AND tipo_documento IN ('RV', 'RF', 'RR', 'DR', 'RM', 'RP', 'ZN', 'ZV', 'EC')
  AND valor_moeda_local > 0
GROUP BY 1
"""

# 2) Liquidado (só dinheiro) por loja e mês, 12 meses · parâmetros :ini, :pos
SQL_LIQ12 = """
SELECT
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  COALESCE(NULLIF(TRIM(operador), ''), 'SEM OPERADOR') AS op,
  COALESCE(NULLIF(TRIM(uf), ''), 'SEM UF') AS uf,
  DATE_FORMAT(data_compensacao, 'yyyy-MM') AS mes,
  ROUND(SUM(valor_moeda_local), 2) AS v
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Liquidado'
  AND data_compensacao BETWEEN CAST(:ini AS DATE) AND CAST(:pos AS DATE)
GROUP BY ALL
"""

# 3) Liquidação: previsto × liquidado por dia (mês atual e anterior)
SQL_LIQV = """
SELECT
  janela AS jan,
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
  COALESCE(NULLIF(TRIM(uf), ''), 'SEM UF') AS uf,
  COALESCE(NULLIF(TRIM(operador), ''), 'SEM OPERADOR') AS op,
  CASE WHEN fl_previsto THEN data_prevista_recebimento END AS dp,
  CASE WHEN fl_liquidado_no_mes THEN data_retorno_baixa END AS dr,
  SUM(valor_previsto) AS prev,
  SUM(CASE WHEN fl_previsto AND fl_liquidado_no_mes THEN valor ELSE 0 END) AS rec,
  SUM(CASE WHEN NOT fl_previsto AND fl_liquidado_no_mes THEN valor ELSE 0 END) AS fora,
  SUM(CASE WHEN classificacao_liquidacao LIKE 'Do previsto%antecipado' THEN valor ELSE 0 END) AS antec,
  SUM(CASE WHEN situacao_previsto = 'Resolvido sem caixa' THEN valor ELSE 0 END) AS semcx,
  SUM(CASE WHEN classificacao_liquidacao LIKE 'Fora do previsto%recupera%' THEN valor ELSE 0 END) AS f_rec,
  SUM(CASE WHEN classificacao_liquidacao LIKE 'Fora do previsto%meses seguintes' THEN valor ELSE 0 END) AS f_ant,
  SUM(CASE WHEN classificacao_liquidacao LIKE 'Fora do previsto%faturado no m%' THEN valor ELSE 0 END) AS f_fat,
  SUM(CASE WHEN classificacao_liquidacao LIKE 'Fora do previsto%outros' THEN valor ELSE 0 END) AS f_out
FROM data_analytics.contas_receber.vw_cr_liquidacao_titulo
GROUP BY ALL
"""

# 3b) Ofensores do mês atual (previsto até hoje não pago), por loja
SQL_LIQO = """
SELECT
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
  COALESCE(NULLIF(TRIM(uf), ''), 'SEM UF') AS uf,
  COALESCE(NULLIF(TRIM(operador), ''), 'SEM OPERADOR') AS op,
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  SUM(valor) AS v
FROM data_analytics.contas_receber.vw_cr_liquidacao_titulo
WHERE janela = 'Mês atual'
  AND situacao_previsto LIKE 'N%o pago%'
GROUP BY ALL
"""

# 4) Árvore (atraso real, base da série diária) · :corte, :pos, :d0dia, :d0mes
SQL_ARV = """
WITH b AS (
  SELECT
    COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
    canal_ajustado AS canal,
    LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
    COALESCE(NULLIF(TRIM(uf), ''), 'SEM UF') AS uf,
    COALESCE(NULLIF(TRIM(operador), ''), 'SEM OPERADOR') AS op,
    tipo_documento AS tipo, status_titulo AS st, valor AS x,
    data_emissao AS e, data_vencimento AS ve, data_entrada_atraso AS ea, data_baixa AS s,
    fl_entrou_em_atraso AS fa
  FROM data_analytics.contas_receber.vw_cr_eventos_titulo
  WHERE data_baixa IS NULL OR data_baixa > CAST(:corte AS DATE)
),
w AS (
  SELECT 'dia' AS j, CAST(:d0dia AS DATE) AS d0
  UNION ALL
  SELECT 'mes' AS j, CAST(:d0mes AS DATE) AS d0
),
p AS (
  SELECT CAST(:pos AS DATE) AS pos
)
SELECT
  w.j, b.cob, b.canal, b.org, b.uf, b.op,
  SUM(CASE WHEN b.e <= w.d0 AND (b.s IS NULL OR b.s > w.d0) THEN b.x ELSE 0 END) AS c0,
  SUM(CASE WHEN b.e <= w.d0 AND (b.s IS NULL OR b.s > w.d0) AND b.ve <= w.d0 THEN b.x ELSE 0 END) AS v0,
  SUM(CASE WHEN b.st = 'Liquidado' AND b.fa AND b.s > w.d0 AND b.s <= p.pos THEN b.x ELSE 0 END) AS rec,
  SUM(CASE WHEN b.st = 'Baixa sem Caixa' AND b.fa AND b.s > w.d0 AND b.s <= p.pos THEN b.x ELSE 0 END) AS neg,
  SUM(CASE WHEN b.tipo = 'ZN' AND b.fa AND b.ea > w.d0 AND b.ea <= p.pos THEN b.x ELSE 0 END) AS novzn,
  SUM(CASE WHEN b.e > w.d0 AND b.e <= p.pos THEN b.x ELSE 0 END) AS ent
FROM b CROSS JOIN w CROSS JOIN p
GROUP BY ALL
"""

# 4b) Faturamento para lojas que já tinham atraso real na véspera · mesmos parâmetros
SQL_ENTV = """
WITH b AS (
  SELECT
    conta_cliente AS cli,
    COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
    canal_ajustado AS canal,
    LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
    COALESCE(NULLIF(TRIM(uf), ''), 'SEM UF') AS uf,
    COALESCE(NULLIF(TRIM(operador), ''), 'SEM OPERADOR') AS op,
    tipo_documento AS tipo, valor AS x,
    data_emissao AS e, data_entrada_atraso AS ea, data_baixa AS s, fl_entrou_em_atraso AS fa
  FROM data_analytics.contas_receber.vw_cr_eventos_titulo
  WHERE data_baixa IS NULL OR data_baixa > CAST(:corte AS DATE)
),
w AS (
  SELECT 'dia' AS j, CAST(:d0dia AS DATE) AS d0
  UNION ALL
  SELECT 'mes' AS j, CAST(:d0mes AS DATE) AS d0
),
emis AS (
  SELECT w.j, b.cli, b.cob, b.canal, b.org, b.uf, b.op, b.e, b.x
  FROM b CROSS JOIN w
  WHERE b.tipo <> 'ZN' AND b.e > w.d0 AND b.e <= CAST(:pos AS DATE)
),
atr AS (
  SELECT DISTINCT m.j, m.cli, m.e
  FROM emis m
  JOIN b t
    ON  t.cli = m.cli
    AND t.fa
    AND t.ea <= DATE_SUB(m.e, 1)
    AND (t.s IS NULL OR t.s > DATE_SUB(m.e, 1))
)
SELECT m.j, m.cob, m.canal, m.org, m.uf, m.op, m.cli, SUM(m.x) AS v
FROM emis m
JOIN atr a ON a.j = m.j AND a.cli = m.cli AND a.e = m.e
GROUP BY ALL
"""

# 5) Ranking (lógica da vw_cr_ranking_cobrador_mensal, com canal e marca):
#    meses -4 a 0 a partir da posição · parâmetros :pos, :mes_ant
SQL_RANK = """
WITH b AS (
  SELECT
    COALESCE(NULLIF(TRIM(nome_cobrador), ''), 'SEM COBRADOR') AS cob,
    canal_ajustado AS canal,
    LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
    status_titulo AS st, valor AS x, data_vencimento AS ve,
    data_entrada_atraso AS ea, data_baixa AS s
  FROM data_analytics.contas_receber.vw_cr_eventos_titulo
  WHERE fl_entrou_em_atraso
),
p AS (
  SELECT CAST(:pos AS DATE) AS pos, CAST(:mes_ant AS DATE) AS dm
),
seq AS (
  SELECT EXPLODE(SEQUENCE(-4, 0)) AS n
),
m AS (
  SELECT
    ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n)                                 AS mi,
    DATE_SUB(ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n), 1)                    AS db,
    LAST_DAY(ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n))                       AS ud,
    LEAST(LAST_DAY(ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n)), p.pos)         AS fm,
    CASE WHEN seq.n = -1 THEN p.dm ELSE DATE '1900-01-01' END             AS cdm
  FROM seq CROSS JOIN p
),
est AS (
  SELECT
    m.mi, b.cob, b.canal, b.org,
    SUM(b.x) AS vi,
    SUM(CASE WHEN b.st = 'Liquidado' AND b.s <= m.fm THEN b.x ELSE 0 END) AS rec,
    SUM(CASE WHEN b.st = 'Liquidado' AND b.s <= m.cdm THEN b.x ELSE 0 END) AS rec_dm,
    SUM(CASE WHEN b.st = 'Baixa sem Caixa' AND b.s <= m.fm THEN b.x ELSE 0 END) AS acd,
    SUM(CASE WHEN DATEDIFF(m.db, b.ve) BETWEEN 61 AND 90 THEN b.x ELSE 0 END) AS s6,
    SUM(CASE WHEN DATEDIFF(m.db, b.ve) BETWEEN 61 AND 90
              AND (b.s IS NULL OR b.s > m.fm) THEN b.x ELSE 0 END) AS s6a,
    SUM(CASE WHEN DATEDIFF(m.db, b.ve) BETWEEN 61 AND 90
              AND b.st = 'Baixa sem Caixa' AND b.s <= m.fm THEN b.x ELSE 0 END) AS s6b,
    SUM(CASE WHEN DATEDIFF(m.db, b.ve) > 90 THEN b.x ELSE 0 END) AS v9,
    SUM(CASE WHEN DATEDIFF(m.db, b.ve) > 90
              AND b.st = 'Liquidado' AND b.s <= m.fm THEN b.x ELSE 0 END) AS r9,
    0 AS novm, 0 AS curp
  FROM m
  JOIN b ON b.ea <= m.db AND (b.s IS NULL OR b.s > m.db)
  GROUP BY m.mi, b.cob, b.canal, b.org
),
cura AS (
  SELECT
    m.mi, b.cob, b.canal, b.org,
    0 AS vi, 0 AS rec, 0 AS rec_dm, 0 AS acd, 0 AS s6, 0 AS s6a, 0 AS s6b, 0 AS v9, 0 AS r9,
    SUM(b.x) AS novm,
    SUM(CASE WHEN b.st = 'Liquidado' AND b.s <= DATE_ADD(b.ve, 30) THEN b.x ELSE 0 END) AS curp
  FROM m
  JOIN b ON b.ea BETWEEN m.mi AND m.ud
  GROUP BY m.mi, b.cob, b.canal, b.org
),
u AS (
  SELECT * FROM est
  UNION ALL
  SELECT * FROM cura
)
SELECT
  DATE_FORMAT(mi, 'yyyy-MM') AS mes, cob, canal, org,
  SUM(vi) AS vi, SUM(rec) AS rec, SUM(rec_dm) AS rec_dm, SUM(acd) AS acd,
  SUM(s6) AS s6, SUM(s6a) AS s6a, SUM(s6b) AS s6b, SUM(v9) AS v9, SUM(r9) AS r9,
  SUM(novm) AS novm, SUM(curp) AS curp
FROM u
GROUP BY 1, 2, 3, 4
"""


def params_liq12(dt):
    return {"ini": str(mes_add(dt["pos"], -11)), "pos": str(dt["pos"])}


def params_arv(dt):
    return {"corte": str(dt["d0mes"]), "pos": str(dt["pos"]),
            "d0dia": str(dt["d0dia"]), "d0mes": str(dt["d0mes"])}


def params_rank(dt):
    return {"pos": str(dt["pos"]), "mes_ant": str(dt["mes_ant"])}


# =====================================================================
# Dimensões
# =====================================================================
_txt = {}


def txt(s):
    """Uma única cópia de cada texto repetido: menos memória."""
    return _txt.setdefault(s, s)


def marca_de(org):
    return config.MARCAS.get(str(org or ""), config.MARCA_PADRAO)


def canal_cod(c):
    return txt(str(c or "").strip())


def fmt_cnpj(c):
    d = re.sub(r"\D", "", str(c or ""))
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}" if len(d) == 14 else str(c or "")


def faixa(d):
    """Aging do layout v3: até 10, 10–30, 31–60, 61–90, 91–180, 181–360, > 360."""
    for i, lim in enumerate((10, 30, 60, 90, 180, 360)):
        if d <= lim:
            return i
    return 6


def ordem_cob(n):
    return (n == SEM_COBRADOR, n.startswith("SEM NOME"), n)


# =====================================================================
# Base (consultas pesadas, renovadas de hora em hora)
# =====================================================================
def _tentar(nome, fn, *args):
    try:
        return fn(*args), None
    except Exception as e:  # bloco opcional: a aba mostra "sem bloco"
        log.warning("Consulta %s falhou: %s", nome, str(e)[:300])
        return None, f"{nome}: {str(e)[:200]}"


def montar_base():
    inicio = time.time()
    meta = consultar_dicts(SQL_META)[0]
    if not meta["pos"]:
        raise RuntimeError("A view não retornou títulos em aberto.")
    pos = date.fromisoformat(meta["pos"])
    comp = min(date.fromisoformat(meta["comp"]) if meta["comp"] else pos, pos)

    avisos = []
    cal, err = _tentar("tb_calendario_recebimento", consultar_dicts, SQL_CAL)
    if cal:
        CAL.clear()
        CAL.update({date.fromisoformat(r["data"]): bool(r["util"]) for r in cal})
    if err:
        avisos.append(err + " (usando a regra de reserva de dias úteis)")
    data_recebimento.cache_clear()
    dt = datas_da_posicao(pos)

    with ThreadPoolExecutor(max_workers=8) as ex:
        f_cob = ex.submit(consultar_dicts, SQL_COBRADORES)
        f_ab = ex.submit(consultar, SQL_ABERTO)
        f_loj = ex.submit(consultar_dicts, SQL_LOJAS)
        f_liq = ex.submit(consultar_dicts, SQL_LIQ12, params_liq12(dt))
        f_lv = ex.submit(_tentar, "vw_cr_liquidacao_titulo", consultar_dicts, SQL_LIQV)
        f_lo = ex.submit(_tentar, "vw_cr_liquidacao_titulo (ofensores)", consultar_dicts, SQL_LIQO)
        f_arv = ex.submit(_tentar, "vw_cr_eventos_titulo (árvore)", consultar_dicts, SQL_ARV, params_arv(dt))
        f_env = ex.submit(_tentar, "vw_cr_eventos_titulo (faturamento p/ atraso)", consultar_dicts, SQL_ENTV, params_arv(dt))
        f_ran = ex.submit(_tentar, "vw_cr_eventos_titulo (ranking)", consultar_dicts, SQL_RANK, params_rank(dt))
        cobs = f_cob.result()
        cols, ab = f_ab.result()
        loj, liq = f_loj.result(), f_liq.result()
        extras = {}
        for k, f in (("liqv", f_lv), ("liqo", f_lo), ("arv", f_arv), ("entv", f_env), ("ran", f_ran)):
            extras[k], err = f.result()
            if err:
                avisos.append(err)

    # código -> nome do cobrador (para o tooltip e para os ACESSOS do config)
    nome_por_cod = {r["cod"]: r["nome"] for r in cobs if r["cod"]}
    cod_por_nome = {}
    for c, n in nome_por_cod.items():
        cod_por_nome.setdefault(n, c)

    ix = {c: i for i, c in enumerate(cols)}
    tit = []
    peso_op = defaultdict(lambda: defaultdict(float))   # loja -> operador -> valor
    peso_uf = defaultdict(lambda: defaultdict(float))   # loja -> uf -> valor
    for r in ab:
        cli = r[ix["cli"]]
        v = float(r[ix["v"]])
        uf = txt(r[ix["uf"]] or SEM_UF)
        venc = r[ix["venc"]]
        peso_op[cli][txt(r[ix["op"]] or SEM_OPERADOR)] += v
        peso_uf[cli][uf] += v
        tipo = txt(r[ix["tipo"]] or "")
        bloq = txt((r[ix["bloq"]] or "").upper())
        tit.append({
            "cli": cli, "cob": txt(r[ix["cob"]]), "canal": canal_cod(r[ix["canal"]]),
            "marca": marca_de(r[ix["org"]]), "uf": uf, "ufo": txt(r[ix["ufo"]]),
            "doc": r[ix["doc"]] or "", "item": r[ix["item"]] or "", "tipo": tipo,
            "emis": r[ix["emis"]] or venc, "venc": venc, "d": int(r[ix["dias"]]), "v": v, "bl": bloq,
            "neg": tipo == "ZN" or bloq == "N",
            "atr": data_recebimento(date.fromisoformat(venc)) <= pos,   # atraso real
        })
    # um operador por loja (o de maior valor em aberto): a loja nunca se divide
    cli_op = {c: max(d.items(), key=lambda kv: kv[1])[0] for c, d in peso_op.items()}
    for t in tit:
        t["op"] = cli_op[t["cli"]]

    cad = {r["cli"]: r for r in loj}
    lojas = {}
    for cli, ufs in peso_uf.items():
        c = cad.get(cli, {})
        lojas[cli] = {"razao": c.get("razao") or str(cli), "fant": c.get("fant") or "",
                      "cidade": c.get("cidade") or "", "uf": max(ufs.items(), key=lambda kv: kv[1])[0]}

    def dims(r, geo=True):
        out = {"cob": txt(r["cob"]), "canal": canal_cod(r["canal"]), "marca": marca_de(r["org"])}
        if geo:
            cli = r.get("cli")
            out["op"] = cli_op.get(cli) or txt(r.get("op") or SEM_OPERADOR)
            out["uf"] = txt(r.get("uf") or SEM_UF)
        return out

    for r in liq:
        r.update(dims(r))
    for k in ("liqv", "liqo", "arv", "entv"):
        for r in extras[k] or []:
            r.update(dims(r))
    for r in extras["ran"] or []:
        r.update(dims(r, geo=False))
    for r in extras["liqv"] or []:
        for c in ("dp", "dr"):
            r[c] = date.fromisoformat(r[c]) if r[c] else None

    log.info("Base montada em %.1fs · aberto=%d lojas=%d liq=%d liqv=%s arv=%s rank=%s avisos=%d",
             time.time() - inicio, len(tit), len(lojas), len(liq),
             len(extras["liqv"] or []), len(extras["arv"] or []), len(extras["ran"] or []), len(avisos))
    return {
        "dt": dt, "comp": comp, "tit": tit, "liq": liq, "lojas": lojas, **extras,
        "cod_por_nome": cod_por_nome, "nome_por_cod": nome_por_cod, "avisos": avisos,
        "montado_em": datetime.now(FUSO),
        "opcoes": opcoes_de(tit),
    }


def opcoes_de(tit):
    canais = sorted({t["canal"] for t in tit})
    return {
        "canal": [[c, c] for c in canais],
        "marca": sorted({t["marca"] for t in tit}),
        "cobrador": sorted({t["cob"] for t in tit}, key=ordem_cob),
        "uf": sorted({t["uf"] for t in tit}, key=lambda n: (n == SEM_UF, n)),
        "operador": sorted({t["op"] for t in tit}, key=lambda n: (n == SEM_OPERADOR, n)),
    }


# =====================================================================
# Filtros
# =====================================================================
def passa(r, F, ignora=()):
    for k in FK:
        s = F.get(k)
        if s and k not in ignora and r[CAMPO[k]] not in s:
            return False
    return True


def filtra(rows, F, ignora=()):
    return [r for r in (rows or []) if passa(r, F, ignora)]


SEM_GEO = ("uf", "operador")
fr = lambda a, b: (a / b) if b else None
r2 = lambda x: round(x or 0.0, 2)


# =====================================================================
# Blocos do JSON (contrato v3)
# =====================================================================
def bloco_resumo(B, F):
    T = filtra(B["tit"], F)
    car = sum(t["v"] for t in T)
    ven = sum(t["v"] for t in T if t["d"] >= 0)
    ag = [0.0] * 7
    mk = defaultdict(lambda: [0.0, 0.0])
    for t in T:
        mk[t["marca"]][1] += t["v"]
        if t["d"] >= 0:
            ag[faixa(t["d"])] += t["v"]
            mk[t["marca"]][0] += t["v"]
    # mapa: todas as UFs (ignora o filtro de UF)
    mes = B["dt"]["pos"].strftime("%Y-%m")
    uf = defaultdict(lambda: {"cart": 0.0, "venc": 0.0, "cadastro": 0.0, "liq": 0.0})
    for t in filtra(B["tit"], F, ("uf",)):
        o = uf[t["uf"]]
        o["cart"] += t["v"]
        if t["d"] >= 0:
            o["venc"] += t["v"]
        if t["ufo"] == "Cadastro da loja":
            o["cadastro"] += t["v"]
    for r in filtra(B["liq"], F, ("uf",)):
        if r["mes"] == mes:
            uf[r["uf"]]["liq"] += r["v"]
    anterior = None
    if B["arv"] is not None:
        A = [r for r in filtra(B["arv"], F) if r["j"] == "mes"]
        anterior = {"carteira": r2(sum(r["c0"] for r in A)), "vencido": r2(sum(r["v0"] for r in A))}
    return {
        "carteira": r2(car), "vencido": r2(ven), "aVencer": r2(car - ven),
        "anterior": anterior,
        "aging": [r2(x) for x in ag],
        "marcas": [{"n": n, "venc": r2(v), "cart": r2(c)}
                   for n, (v, c) in sorted(mk.items(), key=lambda kv: -kv[1][0]) if c > 0],
        "ufs": [{"uf": u, "cart": r2(o["cart"]), "venc": r2(o["venc"]), "liq": r2(o["liq"]),
                 "fat": None, "cadastro": r2(o["cadastro"])}
                for u, o in sorted(uf.items()) if u != SEM_UF and o["cart"] > 0],
    }


def bloco_liquidacao(B, F):
    if B["liqv"] is None:
        return None
    dt = B["dt"]
    pos, ma = dt["pos"], dt["mes_ant"]
    R = filtra(B["liqv"], F)
    at = [r for r in R if r["jan"] == "Mês atual"]
    an = [r for r in R if r["jan"] == "Mês anterior"]

    def por_dia(rows, dias, ate=None):
        i = {d: k for k, d in enumerate(dias)}
        p, rc, fo = [0.0] * len(dias), [0.0] * len(dias), [0.0] * len(dias)
        for r in rows:
            if r["dp"] in i:
                p[i[r["dp"]]] += r["prev"] or 0
            if r["dr"] in i:
                rc[i[r["dr"]]] += r["rec"] or 0
                fo[i[r["dr"]]] += r["fora"] or 0
        return [{"prev": r2(p[k]), "rec": r2(rc[k]) if (ate is None or d <= ate) else None,
                 "fora": r2(fo[k]) if (ate is None or d <= ate) else None} for k, d in enumerate(dias)]

    dias = por_dia(at, dt["dias_mes"], pos)
    for x, d in zip(dias, dt["dias_mes"]):
        x["data"] = str(d)
    S = lambda rows, k: sum(r[k] or 0 for r in rows)
    ofs = filtra(B["liqo"], F)
    ufs = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])   # prev até hoje, rec do previsto, idem mês ant.
    for r in at:
        if r["dp"] and r["dp"] <= pos:
            ufs[r["uf"]][0] += r["prev"] or 0
        ufs[r["uf"]][1] += r["rec"] or 0
    for r in an:
        if r["dp"] and r["dp"] <= ma:
            ufs[r["uf"]][2] += r["prev"] or 0
        if r["dr"] and r["dr"] <= ma:
            ufs[r["uf"]][3] += r["rec"] or 0
    return {
        "diaUtil": dt["idx"],
        "dias": dias,
        "antecipadoPrevisto": r2(S(at, "antec")),
        "semCaixa": r2(S(at, "semcx")),
        "naoPago": r2(sum(r["v"] or 0 for r in ofs)),
        "naoPagoClientes": len({r["cli"] for r in ofs}),
        "fora": {"recuperado": r2(S(at, "f_rec")), "antecipadoMesesSeguintes": r2(S(at, "f_ant")),
                 "faturadoMes": r2(S(at, "f_fat")), "outros": r2(S(at, "f_out"))},
        "comparacoes": {
            "mes": {"rotulo": MESES_LONGO[ma.month - 1], "ref": ma.strftime("%d/%m"),
                    "dias": por_dia(an, dt["dias_ant"])},
            "ano": None,    # pendência: a view ainda não tem a janela do ano anterior
        },
        "ufs": [{"uf": u, "prevAteHoje": r2(v[0]), "recPrev": r2(v[1]), "atAnt": fr(v[3], v[2])}
                for u, v in sorted(ufs.items()) if u != SEM_UF and v[0] > 0],
    }


def bloco_devedores(B, F):
    dt = B["dt"]
    pos = dt["pos"]
    mes = pos.strftime("%Y-%m")
    fech = {mes_add(pos, k).strftime("%Y-%m") for k in (-3, -2, -1)}
    liq_m, liq_3 = defaultdict(float), defaultdict(float)
    for r in filtra(B["liq"], F):
        if r["mes"] == mes:
            liq_m[r["cli"]] += r["v"]
        elif r["mes"] in fech:
            liq_3[r["cli"]] += r["v"]
    por = defaultdict(lambda: defaultdict(list))
    for t in filtra(B["tit"], F):
        por[t["op"]][t["cli"]].append(t)
    ops = []
    for op, lj in por.items():
        lojas = []
        for cli, ts in lj.items():
            ag, mv, cv = [0.0] * 7, defaultdict(float), defaultdict(float)
            venc = av = 0.0
            for t in ts:
                mv[t["marca"]] += t["v"]
                cv[t["canal"]] += t["v"]
                if t["d"] >= 0:
                    venc += t["v"]
                    ag[faixa(t["d"])] += t["v"]
                else:
                    av += t["v"]
            L = B["lojas"].get(cli, {})
            lojas.append({
                "cod": str(cli), "fant": L.get("fant", ""), "razao": L.get("razao", str(cli)),
                "uf": L.get("uf", SEM_UF), "cidade": L.get("cidade", ""),
                "marcas": [m for m, _ in sorted(mv.items(), key=lambda kv: -kv[1])],
                "canais": [c for c, _ in sorted(cv.items(), key=lambda kv: -kv[1])],
                "venc": r2(venc), "av": r2(av),
                "liq": r2(liq_m.get(cli, 0.0)), "liq3": r2(liq_3.get(cli, 0.0) / 3),
                "antigo": min(t["venc"] for t in ts),
                "aging": [r2(x) for x in ag],
            })
        tv = sum(l["venc"] for l in lojas)
        if tv > 0:
            lojas.sort(key=lambda l: -l["venc"])
            ops.append((tv, {"op": op, "lojas": lojas}))
    ops.sort(key=lambda x: (x[1]["op"] == SEM_OPERADOR, -x[0]))
    return {"operadores": [o for _, o in ops]}


def bloco_cobradoras(B, F):
    if B["ran"] is None:
        return None
    dt = B["dt"]
    pos = dt["pos"]
    mk = {k: mes_add(pos, k).strftime("%Y-%m") for k in range(-4, 1)}
    per, cura_m = [mk[-3], mk[-2], mk[-1]], [mk[-4], mk[-3], mk[-2]]
    m12 = [mes_add(pos, k) for k in range(-11, 1)]
    acc = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    tot = defaultdict(lambda: defaultdict(float))
    for r in filtra(B["ran"], F, SEM_GEO):
        a = acc[(r["cob"], r["canal"])][r["mes"]]
        for k in ("vi", "rec", "rec_dm", "acd", "s6", "s6a", "s6b", "v9", "r9", "novm", "curp"):
            a[k] += r[k] or 0.0
            tot[r["mes"]][k] += r[k] or 0.0
    hist = defaultdict(lambda: defaultdict(float))
    for r in filtra(B["liq"], F, SEM_GEO):
        hist[(r["cob"], r["canal"])][r["mes"]] += r["v"]
    S = lambda a, ms, k: sum(a[m][k] for m in ms)
    lista = []
    for (n, c) in sorted(set(acc) | set(hist), key=lambda kc: (ordem_cob(kc[0]), kc[1])):
        a = acc[(n, c)]
        h = [r2(hist[(n, c)].get(m.strftime("%Y-%m"), 0.0)) for m in m12]
        if not (S(a, per, "vi") or S(a, cura_m, "novm") or any(h)):
            continue
        lista.append({
            "n": n, "canal": c, "cart": r2(a[per[0]]["vi"]),
            "rec": fr(S(a, per, "rec"), S(a, per, "vi")),
            "rol": fr(S(a, per, "s6a"), S(a, per, "s6") - S(a, per, "s6b")),
            "cura": fr(S(a, cura_m, "curp"), S(a, cura_m, "novm")),
            "r90": fr(S(a, per, "r9"), S(a, per, "v9")),
            "acordo": r2(S(a, per, "acd")), "hist": h,
        })
    t0, t1, t2 = tot[mk[0]], tot[mk[-1]], tot[mk[-2]]
    return {
        "periodo": (f"{MESES_LONGO[pos.month - 1]} até {pos.strftime('%d/%m')} · ranking dos últimos "
                    f"3 meses fechados ({rotulo_mes(mes_add(pos, -3), False)} a {rotulo_mes(mes_add(pos, -1), False)})"),
        "meses12": [rotulo_mes(m, False) + ("*" if i == 11 else "") for i, m in enumerate(m12)],
        "equipe": {
            "recebido": r2(t0["rec"]), "recebidoAnt": r2(t1["rec_dm"]),            # até o mesmo dia útil
            "recuperacao": fr(t0["rec"], t0["vi"]), "recuperacaoAnt": fr(t1["rec_dm"], t1["vi"]),
            "rolagem": fr(t1["s6a"], t1["s6"] - t1["s6b"]),                       # último mês fechado
            "rolagemAnt": fr(t2["s6a"], t2["s6"] - t2["s6b"]),
            "acordo": r2(sum(tot[m]["acd"] for m in per)),
        },
        "lista": lista,
    }


def arvore_de(B, F, nome, Tc):
    """Árvore da cobradora (R$). Novos e saiu fecham por diferença, para que
    V0 + novos − rec − neg = vencido de hoje e C0 + entrou − saiu = carteira de hoje."""
    if B["arv"] is None:
        return None
    V1 = sum(t["v"] for t in Tc if t["d"] >= 0)
    C1 = sum(t["v"] for t in Tc)
    A = [r for r in filtra(B["arv"], F) if r["cob"] == nome]
    E = [r for r in filtra(B["entv"], F) if r["cob"] == nome]
    out = {}
    for j in ("dia", "mes"):
        s = defaultdict(float)
        for r in A:
            if r["j"] == j:
                for k in ("c0", "v0", "rec", "neg", "novzn", "ent"):
                    s[k] += r[k] or 0.0
        ej = [r for r in E if r["j"] == j]
        out[j] = {
            "V0": r2(s["v0"]), "C0": r2(s["c0"]),
            "novos": r2(V1 - s["v0"] + s["rec"] + s["neg"]), "acordoParc": r2(s["novzn"]),
            "rec": r2(s["rec"]), "neg": r2(s["neg"]),
            "entrou": r2(s["ent"]), "entrouVenc": r2(sum(r["v"] or 0 for r in ej)),
            "entrouCli": len({r["cli"] for r in ej}),
            "saiu": r2(s["c0"] + s["ent"] - C1),
        }
    atr = {t["cli"] for t in Tc if t["d"] >= 0 and t["atr"]}
    out["estoqueNeg"] = r2(sum(t["v"] for t in Tc if t["d"] >= 0 and t["neg"]))
    out["avencerClisVencidos"] = r2(sum(t["v"] for t in Tc if t["d"] < 0 and t["cli"] in atr))
    return out


def linha_titulo(B, t):
    L = B["lojas"].get(t["cli"], {})
    return [t["op"], str(t["cli"]), L.get("fant", ""), L.get("razao", ""), t["doc"], t["item"], t["tipo"],
            "", "", "", t["emis"], t["venc"], t["d"], round(t["v"], 2), t["bl"], t["canal"], t["marca"],
            t["uf"], ORIGEM_UF.get(t["ufo"], t["ufo"]), L.get("cidade", ""), t["cob"]]


def bloco_meu(B, F, user, mcob):
    Fe = dict(F, cobrador=set())
    if user["gestor"]:
        lista = [n for n in B["opcoes"]["cobrador"] if n != SEM_COBRADOR]
        unica = next(iter(F["cobrador"])) if len(F["cobrador"]) == 1 else None
        ativa = (mcob if mcob in lista else None) or (unica if unica in lista else None) or (lista[0] if lista else None)
    else:
        lista = user["cobradoras"]
        ativa = lista[0] if lista else None            # mcob é ignorado
    Te = filtra(B["tit"], Fe)
    car_e = sum(t["v"] for t in Te)
    Tc = [t for t in Te if t["cob"] == ativa]
    mes = B["dt"]["pos"].strftime("%Y-%m")
    rec = sum(r["v"] for r in filtra(B["liq"], Fe) if r["cob"] == ativa and r["mes"] == mes)
    Tc.sort(key=lambda t: -t["d"])
    return {
        "cobradoras": lista, "cobradora": ativa,
        "equipeInad": fr(sum(t["v"] for t in Te if t["d"] >= 0), car_e),
        "recebidoMes": r2(rec), "recebidoMesAnt": None,
        "arvore": arvore_de(B, Fe, ativa, Tc),
        "titulos": {"colunas": COLUNAS_TITULOS, "linhas": [linha_titulo(B, t) for t in Tc]},
    }


def posicao_texto(dt, comp):
    p = dt["pos"]
    return (f"Posição de <b>{p.strftime('%d/%m/%Y')} ({DIAS_PT[p.weekday()]})</b> · "
            f"{dt['idx']}º de {len(dt['dias_mes'])} dias úteis · "
            f"pagamentos compensados até <b>{comp.strftime('%d/%m')}</b>")


def montar_json(B, F, user, mcob):
    dt = B["dt"]
    if user["gestor"]:
        opcoes = B["opcoes"]
        nomes = opcoes["cobrador"]
    else:  # listas só da própria carteira (não mostra clientes de outra cobradora)
        opcoes = dict(opcoes_de([t for t in B["tit"] if t["cob"] in user["cobradoras"]]),
                      cobrador=user["cobradoras"])
        nomes = user["cobradoras"]
    dados = {
        "versao": 3,
        "posicao": str(dt["pos"]),
        "posicaoTexto": posicao_texto(dt, B["comp"]),
        "refAnterior": dt["d0mes"].strftime("%d/%m"),
        "atualizadoEm": B["montado_em"].strftime("%d/%m/%Y %H:%M"),
        "usuario": {"nome": user["nome"], "gestor": user["gestor"],
                    "cobradora": None if user["gestor"] else (user["cobradoras"] or [None])[0]},
        "codCobrador": {n: c for n, c in B["cod_por_nome"].items() if n in nomes and n != SEM_COBRADOR},
        "filtros": {"opcoes": opcoes, "ativos": {k: sorted(F[k]) for k in FK}},
        "meuPainel": bloco_meu(B, F, user, mcob),
    }
    if user["gestor"]:
        dados["resumo"] = bloco_resumo(B, F)
        dados["devedores"] = bloco_devedores(B, F)
        for chave, bloco in (("liquidacao", bloco_liquidacao(B, F)), ("cobradoras", bloco_cobradoras(B, F))):
            if bloco is not None:
                dados[chave] = bloco
    return dados


def _json_script(obj):
    txt_ = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return txt_.replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


# =====================================================================
# Cache
# =====================================================================
_cache = {"t": 0.0, "base": None}
_cache_json = {}
_estado = {"montando": False, "erro": None, "inicio": None}
_lock = threading.Lock()


def _montar_em_segundo_plano():
    try:
        base = montar_base()
        with _lock:
            _cache.update(t=time.time(), base=base)
            _cache_json.clear()
            _cache_bytes["total"] = 0
            _estado["erro"] = None
    except Exception as e:
        log.exception("Falha ao montar a base")
        with _lock:
            _estado["erro"] = str(e)[:500]
    finally:
        with _lock:
            _estado["montando"] = False


def iniciar_montagem():
    with _lock:
        if _estado["montando"]:
            return
        _estado.update(montando=True, inicio=time.time())
    threading.Thread(target=_montar_em_segundo_plano, daemon=True).start()


def base_atual():
    b = _cache["base"]
    if b is None or time.time() - _cache["t"] >= CACHE_SEGUNDOS:
        iniciar_montagem()
    return b


CACHE_MAX_BYTES = int(os.getenv("CACHE_MAX_MB", "300")) * 1024 * 1024
_cache_bytes = {"total": 0}


def pagina_gz(B, F, user, mcob):
    """Página pronta e comprimida (gzip) por combinação de filtros e usuário."""
    chave = (id(B), _tpl["mtime"], tuple(tuple(sorted(F[k])) for k in FK), mcob or "",
             user["gestor"], tuple(user["cobradoras"]))
    gz = _cache_json.get(chave)
    if gz is None:
        t0 = time.time()
        js = _json_script(montar_json(B, F, user, mcob))
        html = com_portal(template()).replace(MARCADOR_DADOS, js, 1)
        gz = gzip.compress(html.encode("utf-8"), compresslevel=5)
        log.info("JSON montado em %.2fs (%d KB; página comprimida %d KB) · filtros=%s",
                 time.time() - t0, len(js) // 1024, len(gz) // 1024, chave[2:4])
        with _lock:
            while _cache_json and _cache_bytes["total"] + len(gz) > CACHE_MAX_BYTES:
                _cache_bytes["total"] -= len(_cache_json.pop(next(iter(_cache_json))))
            _cache_json[chave] = gz
            _cache_bytes["total"] += len(gz)
    return gz


def resposta_gz(gz):
    resp = Response(status=200, mimetype="text/html")
    if "gzip" in request.headers.get("Accept-Encoding", "").lower():
        resp.set_data(gz)
        resp.headers["Content-Encoding"] = "gzip"
    else:
        resp.set_data(gzip.decompress(gz))
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Vary"] = "Accept-Encoding"
    return resp


# =====================================================================
# Usuário e filtros da URL
# =====================================================================
def usuario_email():
    email = request.headers.get("X-Forwarded-Email") or os.getenv("DEV_EMAIL", "")
    return email.strip().lower()


def primeiro_nome(email):
    p = [x for x in email.split("@")[0].replace("_", ".").split(".") if x]
    return p[0].capitalize() if p else ""


def usuario(B):
    email = usuario_email()
    gestor = email in GESTORES
    npc = B["nome_por_cod"]
    sem_zero = {c.lstrip("0"): n for c, n in npc.items()}
    cobs = sorted({npc.get(c) or sem_zero.get(c.lstrip("0")) for c in ACESSOS.get(email, [])} - {None})
    return {"email": email, "nome": primeiro_nome(email), "gestor": gestor, "cobradoras": [] if gestor else cobs}


def filtros_da_url():
    F = {k: {v.strip() for v in request.args.getlist(k) if v.strip()} for k in FK}
    if not F["canal"]:
        F["canal"] = set(CANAL_PADRAO)
    return F


# =====================================================================
# Páginas
# =====================================================================
_tpl = {"mtime": None, "html": None}


def template():
    mtime = ARQUIVO_HTML.stat().st_mtime
    if _tpl["mtime"] != mtime:
        _tpl["html"] = ARQUIVO_HTML.read_text(encoding="utf-8")
        _tpl["mtime"] = mtime
    return _tpl["html"]


def resposta_html(html, status=200):
    corpo = html.encode("utf-8")
    resp = Response(status=status, mimetype="text/html")
    if "gzip" in request.headers.get("Accept-Encoding", "").lower():
        corpo = gzip.compress(corpo, compresslevel=5)
        resp.headers["Content-Encoding"] = "gzip"
    resp.set_data(corpo)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Vary"] = "Accept-Encoding"
    return resp


def pagina_aviso(titulo, texto, refresh=None):
    meta = f"<meta http-equiv='refresh' content='{refresh}'>" if refresh else ""
    return (f"<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'>{meta}"
            f"<title>Contas a Receber</title></head>"
            f"<body style=\"font-family:Inter,'Segoe UI',Arial,sans-serif;background:#0A0C0F;color:#F3F4F6;margin:0\">"
            f"<div style='padding:18px 28px;border-bottom:1px solid #22272E;font-weight:700'>Contas a Receber</div>"
            f"<div style='padding:40px 28px;max-width:720px;color:#B6BCC6'><h2 style='color:#F3F4F6'>{titulo}</h2>"
            f"{texto}</div></body></html>")


def pagina_espera(email):
    erro = _estado["erro"]
    seg = int(time.time() - _estado["inicio"]) if _estado["inicio"] else 0
    if erro:
        detalhe = (f"<p style='color:#F87171;font-size:12px'>{erro.replace('<', '&lt;')}</p>"
                   if email in GESTORES else "")
        return pagina_aviso("Não foi possível carregar os dados do Databricks.",
                            "<p>Uma nova tentativa é feita a cada atualização desta página. "
                            "Se persistir, avise o responsável pelo painel.</p>" + detalhe, 30)
    return pagina_aviso("Carregando os dados do Contas a Receber…",
                        f"<p>Isso leva de 1 a 3 minutos na primeira vez. A página atualiza sozinha. ({seg} s)</p>", 10)


def com_portal(html):
    if config.PORTAL_URL:
        html = html.replace(PORTAL_PADRAO_TEMPLATE, config.PORTAL_URL, 1)
    return html


@app.route("/")
def inicio():
    if not ARQUIVO_HTML.exists():
        log.error("Arquivo não encontrado: %s", ARQUIVO_HTML)
        return resposta_html("<p>Painel indisponível: template.html não encontrado na pasta do app.</p>", 500)
    email = usuario_email()

    # ?demo=1 (só gestor): layout com os dados fictícios do próprio template
    if "demo" in request.args and email in GESTORES:
        return resposta_html(com_portal(template()))

    B = base_atual()
    if B is None:
        if _estado["erro"]:
            iniciar_montagem()
        return resposta_html(pagina_espera(email), 503 if _estado["erro"] else 200)

    user = usuario(B)
    if not user["gestor"] and not user["cobradoras"]:
        return resposta_html(pagina_aviso(
            "Você ainda não tem acesso a uma carteira.",
            f"<p>Peça ao responsável pelo painel para liberar o seu e-mail "
            f"(<b>{(user['email'] or 'não identificado').replace('<', '&lt;')}</b>).</p>"), 403)
    F = filtros_da_url()
    if not user["gestor"]:
        F["cobrador"] = set()                 # cobradora não escolhe carteira
    mcob = (request.args.get("mcob") or "").strip() if user["gestor"] else ""
    try:
        template()                            # recarrega se o template.html mudou
        return resposta_gz(pagina_gz(B, F, user, mcob))
    except Exception:
        log.exception("Erro ao montar o JSON do painel")
        return resposta_html("<p style='font-family:Arial'>Erro ao montar o painel. Veja os Registros do app.</p>", 500)


@app.route("/atualizar", methods=["GET", "POST"])
def atualizar():
    if usuario_email() not in GESTORES:
        return jsonify({"erro": "Só gestores podem atualizar os dados."}), 403
    with _lock:
        _cache_json.clear()
        _cache_bytes["total"] = 0
    iniciar_montagem()
    return jsonify({"ok": True, "mensagem": "Atualização iniciada. Recarregue a página em 1 a 3 minutos."})


@app.route("/saude")
def saude():
    b = _cache["base"]
    return jsonify({
        "ok": True,
        "versao_app": "4.0",
        "warehouse_configurado": bool(WAREHOUSE_ID),
        "template_encontrado": ARQUIVO_HTML.exists(),
        "marcador_no_template": ARQUIVO_HTML.exists() and MARCADOR_DADOS in template(),
        "dados_em_cache": b is not None,
        "posicao": str(b["dt"]["pos"]) if b else None,
        "dia_util": f"{b['dt']['idx']}º de {len(b['dt']['dias_mes'])}" if b else None,
        "calendario_da_tabela": bool(CAL),
        "montado_em": b["montado_em"].isoformat(timespec="seconds") if b else None,
        "titulos_em_aberto": len(b["tit"]) if b else None,
        "lojas": len(b["lojas"]) if b else None,
        "blocos_sem_dados": [k for k in ("liqv", "arv", "ran") if b and b[k] is None],
        "avisos": b["avisos"] if b else [],
        "paginas_em_cache": len(_cache_json),
        "cache_mb": round(_cache_bytes["total"] / 1048576, 1),
        "montando": _estado["montando"],
        "erro": _estado["erro"],
    })


if __name__ == "__main__":
    iniciar_montagem()
    app.run(host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8000")))
