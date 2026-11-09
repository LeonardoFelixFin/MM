"""
APP:       portal-fin-contas-receber (Databricks App · Python/Flask)
O QUE FAZ: Painel oficial de Contas a Receber (modelo da Vera, 4 abas: Resumo,
           Devedores, Cobradoras, Meu painel). Lê a view, monta o JSON do contrato
           (claude/contrato_dados_contas_receber.md) e troca o marcador
           /*__DADOS_JSON__*/null do template.html.
           - Base em memória (renovada de hora em hora, em segundo plano):
             títulos em aberto, liquidado 12 meses, vence no mês, fotos e fluxos
             reconstruídos por eventos e componentes do ranking.
           - Filtros (?canal=&grupo=&marca=&cobrador=&sv=&neg=) aplicados na base
             antes de agregar, a cada pedido (com cache por combinação).
           - Recebido = só status_titulo 'Liquidado'. Baixa sem Caixa = acordo.
           - Cobradora (não gestor) recebe só o Meu painel com a própria carteira.
FONTES:    data_analytics.contas_receber.vw_fato_financeiro_contas_receber
QUEM USA:  gestores e cobradoras do Contas a Receber (link no hub portal-financeiro).
HISTÓRICO: 05/11/2026 · v2.0–v2.2 · app sobre o template v11.
           06/11/2026 · v3.0 · modelo oficial: contrato de dados novo, filtros na
                        URL, nome_cobrador, Meu painel com árvore e títulos.
           06/11/2026 · v3.1 · segurança: SQL em texto fixo, datas por parâmetro
                        nomeado (sem f-string); filtros da URL nunca vão ao SQL.
           06/11/2026 · v3.2 · carteira com 1,1 mi de títulos: a página leva os
                        títulos detalhados só onde a tela usa; o resto vira uma
                        linha "+N títulos" por loja (totais continuam exatos).
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
FILTROS = ["canal", "grupo", "marca", "cobrador", "sv", "neg"]
FAIXAS = ["Até 10", "10–30", "31–60", "61–90", "91–180", "181–360", "Acima de 360"]
MESES_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
DIAS_PT = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]

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
# Datas · dias úteis = segunda a sexta, menos feriados nacionais (bancários)
# e os FERIADOS_EXTRAS do config.py (até a tb_calendario_recebimento do Codd)
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
    mes = (h + l - 7 * m + 114) // 31
    dia = (h + l - 7 * m + 114) % 31 + 1
    return date(ano, mes, dia)


def _feriados(ano):
    fixos = [(1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (11, 20), (12, 25)]
    p = _pascoa(ano)
    moveis = [p - timedelta(days=48), p - timedelta(days=47), p - timedelta(days=2), p + timedelta(days=60)]
    return {date(ano, m, d) for m, d in fixos} | set(moveis)


FERIADOS = set().union(*(_feriados(a) for a in range(2023, 2031))) | {
    date.fromisoformat(x) for x in getattr(config, "FERIADOS_EXTRAS", [])
}


def eh_util(d):
    return d.weekday() < 5 and d not in FERIADOS


def util_anterior(d):
    x = d - timedelta(days=1)
    while not eh_util(x):
        x -= timedelta(days=1)
    return x


def indice_util(d):
    return sum(1 for k in range(1, d.day + 1) if eh_util(date(d.year, d.month, k)))


def ultimo_dia(ano, mes):
    return (date(ano + mes // 12, mes % 12 + 1, 1) - timedelta(days=1))


def mesmo_dia_util(ano, mes, idx):
    """Dia útil de número idx no mês; se o mês tiver menos, o último dia útil."""
    alvo, n = None, 0
    for k in range(1, ultimo_dia(ano, mes).day + 1):
        d = date(ano, mes, k)
        if eh_util(d):
            n += 1
            alvo = d
            if n == idx:
                return d
    return alvo


def mes_add(d, n):
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def ultimo_util_mes_anterior(d):
    x = date(d.year, d.month, 1) - timedelta(days=1)
    while not eh_util(x):
        x -= timedelta(days=1)
    return x


def datas_da_posicao(pos):
    idx = indice_util(pos)
    m_ant = mes_add(pos, -1)
    d0dia = util_anterior(pos)
    return {
        "pos": pos,
        "mes_ant": mesmo_dia_util(m_ant.year, m_ant.month, idx),
        "ano_ant": mesmo_dia_util(pos.year - 1, pos.month, idx),
        "d0dia": d0dia,                                  # árvore "no dia"
        "d0mes": ultimo_util_mes_anterior(pos),          # árvore "no mês"
        "entrou_de": util_anterior(d0dia),               # entrou em atraso: (entrou_de, d0dia]
        "ini_mes": date(pos.year, pos.month, 1),
        "fim_mes": ultimo_dia(pos.year, pos.month),
    }


def rotulo_mes(d, ano4=True):
    return f"{MESES_PT[d.month - 1]}/{d.year if ano4 else str(d.year)[2:]}"


def dbr(iso):
    return f"{iso[8:10]}/{iso[5:7]}/{iso[0:4]}" if iso else ""


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

SQL_COBRADORES = """
SELECT
  COALESCE(NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), ''), '-') AS cob,
  MAX(NULLIF(TRIM(nome_cobrador), '')) AS nome
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
GROUP BY 1
"""

# 1) Títulos em aberto da posição (nível título)
SQL_ABERTO = """
SELECT
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  nome,
  cnpj_cliente AS cnpj,
  COALESCE(NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), ''), '-') AS cob,
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  CAST(nr_documento AS STRING) AS doc,
  tipo_documento AS tipo,
  data_documento AS emis,
  data_vencimento_liquido AS venc,
  data_vencimento_original AS orig,
  DATEDIFF(CAST(data_posicao AS DATE), data_vencimento_liquido) AS dias,
  CAST(valor_moeda_local AS DOUBLE) AS v,
  COALESCE(TRIM(bloqueio_pgto), '') AS bloq
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Em Aberto'
  AND tipo_documento IN ('RV', 'RF', 'RR', 'DR', 'RM', 'RP', 'ZN', 'ZV', 'EC')
  AND valor_moeda_local > 0
  AND data_vencimento_liquido IS NOT NULL
"""

# 2) Liquidado (só dinheiro) por loja, cobrador, canal, org e mês · parâmetros :ini, :pos
SQL_LIQ12 = """
SELECT
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  COALESCE(NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), ''), '-') AS cob,
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  DATE_FORMAT(data_compensacao, 'yyyy-MM') AS mes,
  ROUND(SUM(valor_moeda_local), 2) AS v
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Liquidado'
  AND data_compensacao BETWEEN CAST(:ini AS DATE) AND CAST(:pos AS DATE)
GROUP BY ALL
"""

# 3) Parte já paga do "vence no mês" · parâmetros :ini_mes, :fim_mes, :pos
SQL_VM_LIQ = """
SELECT
  COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
  COALESCE(NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), ''), '-') AS cob,
  canal_ajustado AS canal,
  LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
  CASE WHEN tipo_documento = 'ZN' OR UPPER(TRIM(COALESCE(bloqueio_pgto, ''))) = 'N' THEN 'S' ELSE 'N' END AS neg,
  YEAR(data_vencimento_liquido) AS ano_venc,
  ROUND(SUM(valor_moeda_local), 2) AS v
FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
WHERE status_titulo = 'Liquidado'
  AND data_vencimento_liquido BETWEEN CAST(:ini_mes AS DATE) AND CAST(:fim_mes AS DATE)
  AND data_compensacao <= CAST(:pos AS DATE)
GROUP BY ALL
"""

# 4) Fotos reconstruídas (mês e ano anteriores, início do dia e do mês) e fluxos
#    do dia e do mês · parâmetros :corte, :pos, :mes_ant, :ano_ant, :d0dia, :d0mes
SQL_HIST = """
WITH t AS (
  SELECT
    COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
    COALESCE(NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), ''), '-') AS cob,
    canal_ajustado AS canal,
    LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
    CASE WHEN tipo_documento = 'ZN' OR UPPER(TRIM(COALESCE(bloqueio_pgto, ''))) = 'N' THEN 'S' ELSE 'N' END AS neg,
    tipo_documento AS tipo,
    data_vencimento_liquido AS ve,
    COALESCE(data_documento, data_vencimento_liquido) AS e,
    CASE WHEN status_titulo = 'Em Aberto' THEN NULL ELSE data_compensacao END AS s,
    status_titulo AS st,
    CAST(valor_moeda_local AS DOUBLE) AS x
  FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
  WHERE tipo_documento IN ('RV', 'RF', 'RR', 'DR', 'RM', 'RP', 'ZN', 'ZV', 'EC')
    AND valor_moeda_local > 0
    AND data_vencimento_liquido IS NOT NULL
    AND (   status_titulo = 'Em Aberto'
         OR (status_titulo IN ('Liquidado', 'Baixa sem Caixa')
             AND data_compensacao > CAST(:corte AS DATE)))
),
d AS (
  SELECT CAST(:pos AS DATE) AS pos, CAST(:mes_ant AS DATE) AS dm, CAST(:ano_ant AS DATE) AS da,
         CAST(:d0dia AS DATE) AS d0d, CAST(:d0mes AS DATE) AS d0m
)
SELECT
  cli, cob, canal, org, neg, YEAR(ve) AS ano_venc,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) THEN x ELSE 0 END) AS car_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 0 AND DATEDIFF(d.dm, ve) < 10 THEN x ELSE 0 END) AS ag1_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 10 AND DATEDIFF(d.dm, ve) < 30 THEN x ELSE 0 END) AS ag2_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 30 AND DATEDIFF(d.dm, ve) < 60 THEN x ELSE 0 END) AS ag3_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 60 AND DATEDIFF(d.dm, ve) < 90 THEN x ELSE 0 END) AS ag4_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 90 AND DATEDIFF(d.dm, ve) < 180 THEN x ELSE 0 END) AS ag5_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 180 AND DATEDIFF(d.dm, ve) < 360 THEN x ELSE 0 END) AS ag6_mes,
  SUM(CASE WHEN e <= d.dm AND (s IS NULL OR s > d.dm) AND ve <= d.dm AND DATEDIFF(d.dm, ve) >= 360 THEN x ELSE 0 END) AS ag7_mes,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) THEN x ELSE 0 END) AS car_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 0 AND DATEDIFF(d.da, ve) < 10 THEN x ELSE 0 END) AS ag1_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 10 AND DATEDIFF(d.da, ve) < 30 THEN x ELSE 0 END) AS ag2_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 30 AND DATEDIFF(d.da, ve) < 60 THEN x ELSE 0 END) AS ag3_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 60 AND DATEDIFF(d.da, ve) < 90 THEN x ELSE 0 END) AS ag4_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 90 AND DATEDIFF(d.da, ve) < 180 THEN x ELSE 0 END) AS ag5_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 180 AND DATEDIFF(d.da, ve) < 360 THEN x ELSE 0 END) AS ag6_ano,
  SUM(CASE WHEN e <= d.da AND (s IS NULL OR s > d.da) AND ve <= d.da AND DATEDIFF(d.da, ve) >= 360 THEN x ELSE 0 END) AS ag7_ano,
  SUM(CASE WHEN e <= d.d0d AND (s IS NULL OR s > d.d0d) THEN x ELSE 0 END) AS c0_dia,
  SUM(CASE WHEN e <= d.d0d AND (s IS NULL OR s > d.d0d) AND ve <= d.d0d THEN x ELSE 0 END) AS v0_dia,
  SUM(CASE WHEN ve > d.d0d AND ve <= d.pos AND e <= ve AND (s IS NULL OR s > ve) THEN x ELSE 0 END) AS nov_dia,
  SUM(CASE WHEN ve > d.d0d AND ve <= d.pos AND e <= ve AND (s IS NULL OR s > ve) AND tipo = 'ZN' THEN x ELSE 0 END) AS novzn_dia,
  SUM(CASE WHEN st = 'Liquidado' AND s > d.d0d AND s <= d.pos AND GREATEST(e, ve) < s THEN x ELSE 0 END) AS rec_dia,
  SUM(CASE WHEN st = 'Baixa sem Caixa' AND s > d.d0d AND s <= d.pos AND GREATEST(e, ve) < s THEN x ELSE 0 END) AS neg_dia,
  SUM(CASE WHEN e > d.d0d AND e <= d.pos THEN x ELSE 0 END) AS ent_dia,
  SUM(CASE WHEN e <= d.d0m AND (s IS NULL OR s > d.d0m) THEN x ELSE 0 END) AS c0_mes,
  SUM(CASE WHEN e <= d.d0m AND (s IS NULL OR s > d.d0m) AND ve <= d.d0m THEN x ELSE 0 END) AS v0_mes,
  SUM(CASE WHEN ve > d.d0m AND ve <= d.pos AND e <= ve AND (s IS NULL OR s > ve) THEN x ELSE 0 END) AS nov_mes,
  SUM(CASE WHEN ve > d.d0m AND ve <= d.pos AND e <= ve AND (s IS NULL OR s > ve) AND tipo = 'ZN' THEN x ELSE 0 END) AS novzn_mes,
  SUM(CASE WHEN st = 'Liquidado' AND s > d.d0m AND s <= d.pos AND GREATEST(e, ve) < s THEN x ELSE 0 END) AS rec_mes,
  SUM(CASE WHEN st = 'Baixa sem Caixa' AND s > d.d0m AND s <= d.pos AND GREATEST(e, ve) < s THEN x ELSE 0 END) AS neg_mes,
  SUM(CASE WHEN e > d.d0m AND e <= d.pos THEN x ELSE 0 END) AS ent_mes
FROM t CROSS JOIN d
GROUP BY cli, cob, canal, org, neg, YEAR(ve)
"""

# 5) Ranking: 3 meses fechados + mês atual · parâmetros :corte, :pos
SQL_RANK = """
WITH t AS (
  SELECT
    COALESCE(TRY_CAST(conta_cliente AS BIGINT), 0) AS cli,
    COALESCE(NULLIF(TRIM(CAST(cobrador_codigo AS STRING)), ''), '-') AS cob,
    canal_ajustado AS canal,
    LTRIM('0', TRIM(CAST(organizacao_vendas AS STRING))) AS org,
    CASE WHEN tipo_documento = 'ZN' OR UPPER(TRIM(COALESCE(bloqueio_pgto, ''))) = 'N' THEN 'S' ELSE 'N' END AS neg,
    tipo_documento AS tipo,
    data_vencimento_liquido AS ve,
    COALESCE(data_documento, data_vencimento_liquido) AS e,
    CASE WHEN status_titulo = 'Em Aberto' THEN NULL ELSE data_compensacao END AS s,
    status_titulo AS st,
    CAST(valor_moeda_local AS DOUBLE) AS x
  FROM data_analytics.contas_receber.vw_fato_financeiro_contas_receber
  WHERE tipo_documento IN ('RV', 'RF', 'RR', 'DR', 'RM', 'RP', 'ZN', 'ZV', 'EC')
    AND valor_moeda_local > 0
    AND data_vencimento_liquido IS NOT NULL
    AND (   status_titulo = 'Em Aberto'
         OR (status_titulo IN ('Liquidado', 'Baixa sem Caixa')
             AND data_compensacao > CAST(:corte AS DATE)))
),
p AS (
  SELECT CAST(:pos AS DATE) AS pos
),
seq AS (
  SELECT EXPLODE(SEQUENCE(-3, 0)) AS n
),
meses AS (
  SELECT
    ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n)                         AS mi,
    DATE_SUB(ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n), 1)            AS s0,
    LEAST(LAST_DAY(ADD_MONTHS(TRUNC(p.pos, 'MM'), seq.n)), p.pos) AS s1
  FROM seq CROSS JOIN p
),
x AS (
  SELECT
    m.mi, t.cob, t.canal, t.org, t.st, t.x, t.e, t.ve, t.s,
    (t.e <= m.s0 AND t.ve <= m.s0)                                 AS venc_ini,
    DATEDIFF(m.s0, t.ve)                                           AS dias_ini,
    (t.s IS NOT NULL AND t.s <= m.s1)                              AS saiu_mes,
    (t.ve BETWEEN m.mi AND m.s1 AND t.e <= t.ve
       AND (t.s IS NULL OR t.s > t.ve))                            AS novo_mes
  FROM t CROSS JOIN meses m
  WHERE (t.s IS NULL OR t.s > m.s0)
    AND t.e <= m.s1
)
SELECT
  DATE_FORMAT(mi, 'yyyy-MM') AS mes, cob, canal, org,
  SUM(CASE WHEN venc_ini THEN x ELSE 0 END) AS vi,
  SUM(CASE WHEN venc_ini AND saiu_mes AND st = 'Liquidado' THEN x ELSE 0 END) AS rec,
  SUM(CASE WHEN venc_ini AND dias_ini BETWEEN 60 AND 89
            AND NOT (saiu_mes AND st = 'Baixa sem Caixa') THEN x ELSE 0 END) AS f6,
  SUM(CASE WHEN venc_ini AND dias_ini BETWEEN 60 AND 89 AND NOT saiu_mes THEN x ELSE 0 END) AS rol,
  SUM(CASE WHEN venc_ini AND dias_ini >= 90 THEN x ELSE 0 END) AS v9,
  SUM(CASE WHEN venc_ini AND dias_ini >= 90 AND saiu_mes AND st = 'Liquidado' THEN x ELSE 0 END) AS r9,
  SUM(CASE WHEN novo_mes THEN x ELSE 0 END) AS novm,
  SUM(CASE WHEN novo_mes AND st = 'Liquidado' AND s <= DATE_ADD(ve, 30) THEN x ELSE 0 END) AS curp,
  SUM(CASE WHEN st = 'Baixa sem Caixa' AND saiu_mes AND GREATEST(e, ve) < s THEN x ELSE 0 END) AS acd
FROM x
GROUP BY ALL
"""


def params_liq12(dt):
    return {"ini": str(mes_add(dt["pos"], -11)), "pos": str(dt["pos"])}


def params_vm(dt):
    return {"ini_mes": str(dt["ini_mes"]), "fim_mes": str(dt["fim_mes"]), "pos": str(dt["pos"])}


def params_hist(dt):
    return {"corte": str(min(dt["ano_ant"], dt["d0mes"])), "pos": str(dt["pos"]),
            "mes_ant": str(dt["mes_ant"]), "ano_ant": str(dt["ano_ant"]),
            "d0dia": str(dt["d0dia"]), "d0mes": str(dt["d0mes"])}


def params_rank(dt):
    return {"corte": str(mes_add(dt["pos"], -3) - timedelta(days=1)), "pos": str(dt["pos"])}


# =====================================================================
# Dimensões
# =====================================================================
def marca_de(org):
    return config.MARCAS.get(str(org or ""), config.MARCA_PADRAO)


def grupo_de(org):
    return "AR&CO" if str(org or "") in config.ORGS_ARCO else "Marcas Core"


def canal_de(c):
    return config.CANAIS.get(str(c), f"Canal {c}")


def sv_de(ano_venc, ano_pos):
    if ano_venc is None:
        return f"Em {ano_pos}"
    a = int(ano_venc)
    return f"Antes {ano_pos}" if a < ano_pos else (f"Em {ano_pos}" if a == ano_pos else f"Após {ano_pos}")


def nome_cob(cod, mapa):
    n = mapa.get(cod)
    if cod == "-" or not cod:
        return n or SEM_COBRADOR
    return n or f"SEM NOME ({cod})"


def ordem(dias):
    if dias < 0:
        return 0
    for i, lim in enumerate((10, 30, 60, 90, 180, 360), 1):
        if dias < lim:
            return i
    return 7


def fmt_cnpj(c):
    d = re.sub(r"\D", "", str(c or ""))
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return str(c or "")


# =====================================================================
# Base (consultas pesadas, renovadas de hora em hora)
# =====================================================================
def montar_base():
    inicio = time.time()
    meta = consultar_dicts(SQL_META)[0]
    if not meta["pos"]:
        raise RuntimeError("A view não retornou títulos em aberto.")
    pos = date.fromisoformat(meta["pos"])
    comp = date.fromisoformat(meta["comp"]) if meta["comp"] else pos
    comp = min(comp, pos)
    dt = datas_da_posicao(pos)

    with ThreadPoolExecutor(max_workers=6) as ex:
        f_cob = ex.submit(consultar_dicts, SQL_COBRADORES)
        f_ab = ex.submit(consultar, SQL_ABERTO)
        f_liq = ex.submit(consultar_dicts, SQL_LIQ12, params_liq12(dt))
        f_vm = ex.submit(consultar_dicts, SQL_VM_LIQ, params_vm(dt))
        f_his = ex.submit(consultar_dicts, SQL_HIST, params_hist(dt))
        f_ran = ex.submit(consultar_dicts, SQL_RANK, params_rank(dt))
        mapa = {r["cob"]: r["nome"] for r in f_cob.result()}
        cols, ab = f_ab.result()
        liq, vm, his, ran = f_liq.result(), f_vm.result(), f_his.result(), f_ran.result()

    ano = pos.year
    codigo_por_nome = {}
    nn = lambda c: nome_cob(c, mapa)

    # títulos em aberto, já com as dimensões resolvidas
    ix = {c: i for i, c in enumerate(cols)}
    tit = []
    for r in ab:
        cob = r[ix["cob"]]
        n = nn(cob)
        if cob != "-":
            codigo_por_nome.setdefault(n, cob)
        org = r[ix["org"]]
        venc = r[ix["venc"]]
        orig = r[ix["orig"]]
        tit.append({
            "cli": r[ix["cli"]], "nome": r[ix["nome"]] or "", "cnpj": r[ix["cnpj"]],
            "cob": n, "canal": canal_de(r[ix["canal"]]), "marca": marca_de(org), "grupo": grupo_de(org),
            "sv": sv_de(venc[:4] if venc else None, ano),
            "neg": "S" if (r[ix["tipo"]] == "ZN" or (r[ix["bloq"]] or "").upper() == "N") else "N",
            "doc": r[ix["doc"]], "tipo": r[ix["tipo"]], "emis": r[ix["emis"]], "venc": venc,
            "orig": orig if (orig and venc and venc > orig) else None,
            "d": int(r[ix["dias"]]), "v": float(r[ix["v"]]), "bl": r[ix["bloq"]] or "",
        })

    def dims(r, com_sv_neg=True):
        org = r["org"]
        out = {"cli": r.get("cli"), "cob": nn(r["cob"]), "canal": canal_de(r["canal"]),
               "marca": marca_de(org), "grupo": grupo_de(org)}
        if com_sv_neg:
            out["sv"] = sv_de(r.get("ano_venc"), ano)
            out["neg"] = r.get("neg") or "N"
        return out

    for lista, sv_neg in ((liq, False), (vm, True), (his, True), (ran, False)):
        for r in lista:
            r.update(dims(r, sv_neg))
    for c in mapa:
        if c != "-":
            codigo_por_nome.setdefault(nn(c), c)

    log.info("Base montada em %.1fs · aberto=%d liq=%d vm=%d hist=%d rank=%d",
             time.time() - inicio, len(tit), len(liq), len(vm), len(his), len(ran))
    return {
        "dt": dt, "comp": comp, "tit": tit, "liq": liq, "vm": vm, "his": his, "ran": ran,
        "cod_cob": codigo_por_nome, "montado_em": datetime.now(FUSO).isoformat(timespec="seconds"),
        "opcoes": {
            "canal": sorted({t["canal"] for t in tit}),
            "grupo": sorted({t["grupo"] for t in tit}),
            "marca": sorted({t["marca"] for t in tit}),
            "cobrador": sorted({t["cob"] for t in tit}, key=lambda n: (n == SEM_COBRADOR, n)),
            "sv": sorted({t["sv"] for t in tit}),
            "neg": [["S", "Sim"], ["N", "Não"]],
        },
    }


# =====================================================================
# Montagem do JSON do contrato (a cada combinação de filtros)
# =====================================================================
def passa(r, F, com_sv_neg=True):
    if F.get("canal") and r["canal"] != F["canal"]:
        return False
    if F.get("grupo") and r["grupo"] != F["grupo"]:
        return False
    if F.get("marca") and r["marca"] != F["marca"]:
        return False
    if F.get("cobrador") and r["cob"] != F["cobrador"]:
        return False
    if com_sv_neg:
        if F.get("sv") and r["sv"] != F["sv"]:
            return False
        if F.get("neg") and r["neg"] != F["neg"]:
            return False
    return True


MIL, MI = 1000.0, 1_000_000.0
r1 = lambda x: round(x, 1)
r3 = lambda x: round(x, 3)
fr = lambda a, b: round(a / b, 4) if b else 0.0


def posicao_texto(dt, comp):
    p = dt["pos"]
    return (f"Posição: <b>{p.strftime('%d/%m/%Y')} ({DIAS_PT[p.weekday()]})</b> · "
            f"pagamentos compensados até <b>{comp.strftime('%d/%m')}</b>")


def bloco_resumo(B, T, H, clientes):
    dt = B["dt"]
    car = sum(t["v"] for t in T)
    ven = sum(t["v"] for t in T if t["d"] >= 0)
    ag_hoje = [0.0] * 7
    for t in T:
        if t["d"] >= 0:
            ag_hoje[ordem(t["d"]) - 1] += t["v"]
    ag = {"mes": [0.0] * 7, "ano": [0.0] * 7}
    car_ant = {"mes": 0.0, "ano": 0.0}
    for h in H:
        for k in ("mes", "ano"):
            car_ant[k] += h[f"car_{k}"]
            for i in range(7):
                ag[k][i] += h[f"ag{i + 1}_{k}"]
    mk = defaultdict(lambda: [0.0, 0.0])
    for t in T:
        mk[t["marca"]][1] += t["v"]
        if t["d"] >= 0:
            mk[t["marca"]][0] += t["v"]
    top = sorted(clientes, key=lambda c: -c["v"])[:10]
    return {
        "posicao": str(dt["pos"]), "mesAnt": str(dt["mes_ant"]), "anoAnt": str(dt["ano_ant"]),
        "carteira": {"hoje": r3(car / MI), "mes": r3(car_ant["mes"] / MI), "ano": r3(car_ant["ano"] / MI)},
        "vencido": {"hoje": r3(ven / MI), "mes": r3(sum(ag["mes"]) / MI), "ano": r3(sum(ag["ano"]) / MI)},
        "compensacao": r3(sum(t["v"] for t in T if t["d"] == 0) / MI),
        "faixas": FAIXAS,
        "aging": {"hoje": [r3(x / MI) for x in ag_hoje],
                  "mes": [r3(x / MI) for x in ag["mes"]],
                  "ano": [r3(x / MI) for x in ag["ano"]]},
        "marcas": [{"n": n, "v": r3(v / MI), "c": r3(c / MI)}
                   for n, (v, c) in sorted(mk.items(), key=lambda kv: -kv[1][0]) if c > 0],
        "top": [{"op": c["nome"], "lj": c["lj"], "marcas": c["marcas"], "canal": c["canal"],
                 "cobs": c["cobs"], "v": c["v"], "v90": c["v90"], "ant": c["ant"], "vm": c["vm"],
                 "liq": c["liq"], "av": c["av"]} for c in top],
    }


def montar_clientes(B, T, H, L, VM):
    """Um 'cliente' por loja (conta) enquanto o operador (SORTL) não estiver no Databricks."""
    dt = B["dt"]
    mes_atual = dt["pos"].strftime("%Y-%m")
    ant = defaultdict(float)
    for h in H:
        ant[h["cli"]] += sum(h[f"ag{i}_mes"] for i in range(1, 8))
    liq_mes, liq12 = defaultdict(float), defaultdict(lambda: defaultdict(float))
    for r in L:
        liq12[r["cli"]][r["mes"]] += r["v"]
        if r["mes"] == mes_atual:
            liq_mes[r["cli"]] += r["v"]
    vm = defaultdict(float)
    for r in VM:
        vm[r["cli"]] += r["v"]
    ini, fim = str(dt["ini_mes"]), str(dt["fim_mes"])
    por = defaultdict(list)
    for t in T:
        por[t["cli"]].append(t)
        if ini <= (t["venc"] or "") <= fim:
            vm[t["cli"]] += t["v"]
    out = []
    for cli, ts in por.items():
        venc = [t for t in ts if t["d"] >= 0]
        mv = defaultdict(float)
        for t in venc:
            mv[t["marca"]] += t["v"]
        for t in ts:
            mv.setdefault(t["marca"], 0.0)
        cv = defaultdict(float)
        for t in ts:
            cv[t["canal"]] += t["v"]
        cb = defaultdict(float)
        for t in ts:
            cb[t["cob"]] += t["v"]
        out.append({
            "cod": str(cli), "cnpj": fmt_cnpj(ts[0]["cnpj"]), "nome": ts[0]["nome"] or str(cli), "lj": 1,
            "marcas": [m for m, _ in sorted(mv.items(), key=lambda kv: -kv[1])],
            "canal": max(cv.items(), key=lambda kv: kv[1])[0],
            "cobs": [c for c, _ in sorted(cb.items(), key=lambda kv: -kv[1])],
            "neg": any(t["neg"] == "S" for t in venc),
            "v": r1(sum(t["v"] for t in venc) / MIL),
            "v90": r1(sum(t["v"] for t in venc if t["d"] >= 90) / MIL),
            "ant": r1(ant.get(cli, 0.0) / MIL),
            "vm": r1(vm.get(cli, 0.0) / MIL),
            "liq": r1(liq_mes.get(cli, 0.0) / MIL),
            "av": r1(sum(t["v"] for t in ts if t["d"] < 0) / MIL),
            "_ts": ts, "_liq12": liq12.get(cli, {}),
        })
    return out


# Limites de títulos detalhados por loja (o excedente vira uma linha de total)
TIT_FICHA = 15          # ficha do devedor: os mais atrasados
TIT_VENC_PAINEL = 150   # Meu painel: vencidos por loja
DIAS_AV_PAINEL = 7      # Meu painel: a vencer detalhado até N dias


def _linha_total(ts, d, venc=None):
    """Uma linha que soma vários títulos (mantém o total da loja)."""
    return {"n": len(ts), "v": sum(t["v"] for t in ts), "d": d, "venc": venc}


def bloco_devedores(B, clientes):
    pos = B["dt"]["pos"]
    meses = [mes_add(pos, k).strftime("%Y-%m") for k in range(-11, 1)]
    cl = [c for c in clientes if c["v"] > 0]
    cl.sort(key=lambda c: -c["v"])
    fichas = {}
    for c in cl:
        venc = sorted((t for t in c["_ts"] if t["d"] >= 0), key=lambda t: -t["d"])
        ag = [0.0] * 7
        for t in venc:
            ag[ordem(t["d"]) - 1] += t["v"]
        tit = [{"doc": t["doc"], "tipo": t["tipo"], "venc": dbr(t["venc"]),
                "dias": t["d"], "val": round(t["v"] / MIL, 2)} for t in venc[:TIT_FICHA]]
        resto = venc[TIT_FICHA:]
        if resto:
            tot = _linha_total(resto, max(t["d"] for t in resto))
            tit.append({"doc": f"+{tot['n']} títulos", "tipo": "", "venc": "",
                        "dias": tot["d"], "val": round(tot["v"] / MIL, 2)})
        fichas[c["cod"]] = {
            "aging": [r1(x / MIL) for x in ag],
            "liq12": [r1(c["_liq12"].get(m, 0.0) / MIL) for m in meses],
            "lojas": [{"nome": c["nome"], "cod": c["cod"], "tit": tit}],
        }
    lim = lambda c: {k: v for k, v in c.items() if not k.startswith("_")}
    return {"clientes": [lim(c) for c in cl], "fichas": fichas}


def agrega_rank(R):
    acc = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for r in R:
        a = acc[r["cob"]][r["mes"]]
        for k in ("vi", "rec", "f6", "rol", "v9", "r9", "novm", "curp", "acd"):
            a[k] += r[k] or 0.0
    return acc


def metricas_mes(a):
    return {"rec": fr(a["rec"], a["vi"]), "rol": fr(a["rol"], a["f6"]),
            "cura": fr(a["curp"], a["novm"]), "r90": fr(a["r9"], a["v9"]),
            "acd": r1(a["acd"] / MIL)}


def bloco_cobradoras(B, T, L, R):
    pos = B["dt"]["pos"]
    fechados = [mes_add(pos, k) for k in (-3, -2, -1)]
    meses = [[m.strftime("%Y-%m"), rotulo_mes(m)] for m in fechados]
    m12 = [mes_add(pos, k) for k in range(-11, 1)]
    acc = agrega_rank(R)
    rec12 = defaultdict(lambda: defaultdict(float))
    for r in L:
        rec12[r["cob"]][r["mes"]] += r["v"]
    por = defaultdict(list)
    for t in T:
        por[t["cob"]].append(t)
    lista, sem = [], None
    for n in sorted(set(por) | set(acc)):
        ts = por.get(n, [])
        cart = sum(t["v"] for t in ts)
        venc = sum(t["v"] for t in ts if t["d"] >= 0)
        v90 = sum(t["v"] for t in ts if t["d"] >= 90)
        item = {
            "n": n, "cart": r3(cart / MI), "venc": r3(venc / MI), "v90": r3(v90 / MI),
            "clis": len({t["cli"] for t in ts}),
            "mes": {k: metricas_mes(acc[n][k]) for k, _ in meses},
            "rec12": [r1(rec12[n].get(m.strftime("%Y-%m"), 0.0) / MIL) for m in m12],
        }
        if n == SEM_COBRADOR:
            sem = {"cart": item["cart"], "venc": item["venc"], "v90": item["v90"], "clis": item["clis"],
                   "acd": r1(sum(acc[n][k]["acd"] for k, _ in meses) / MIL)}
        elif cart > 0:
            lista.append(item)
    return {"meses": meses, "rotulos12": [rotulo_mes(m, False) for m in m12],
            "lista": lista, "semCobrador": sem or {"cart": 0, "venc": 0, "v90": 0, "clis": 0, "acd": 0}}


def arvore_de(H_cob, T_cob):
    """Árvore da cobradora (R$ mil). Novos e saiu fecham por diferença, para que
    V0 + novos − rec − neg = vencido de hoje e C0 + entrou − saiu = carteira de hoje."""
    V1 = sum(t["v"] for t in T_cob if t["d"] >= 0)
    C1 = sum(t["v"] for t in T_cob)
    v0cli = {"dia": defaultdict(float), "mes": defaultdict(float)}
    s = {p: defaultdict(float) for p in ("dia", "mes")}
    entcli = {p: defaultdict(float) for p in ("dia", "mes")}
    for h in H_cob:
        for p in ("dia", "mes"):
            for k in ("c0", "v0", "novzn", "rec", "neg", "ent"):
                s[p][k] += h[f"{k}_{p}"]
            v0cli[p][h["cli"]] += h[f"v0_{p}"]
            entcli[p][h["cli"]] += h[f"ent_{p}"]
    out = {}
    for p in ("dia", "mes"):
        V0, C0 = s[p]["v0"], s[p]["c0"]
        rec, neg, ent = s[p]["rec"], s[p]["neg"], s[p]["ent"]
        ev = {c: v for c, v in entcli[p].items() if v > 0 and v0cli[p].get(c, 0) > 0}
        out[p] = {
            "V0": r1(V0 / MIL), "C0": r1(C0 / MIL),
            "novos": r1((V1 - V0 + rec + neg) / MIL),
            "acordoParc": r1(s[p]["novzn"] / MIL),
            "rec": r1(rec / MIL), "neg": r1(neg / MIL),
            "entrou": r1(ent / MIL), "entrouVenc": r1(sum(ev.values()) / MIL), "entrouCli": len(ev),
            "saiu": r1((C0 + ent - C1) / MIL),
        }
    clis_venc = {t["cli"] for t in T_cob if t["d"] >= 0}
    out["estoqueNeg"] = r1(sum(t["v"] for t in T_cob if t["d"] >= 0 and t["neg"] == "S") / MIL)
    out["avencerClisVencidos"] = r1(sum(t["v"] for t in T_cob if t["d"] < 0 and t["cli"] in clis_venc) / MIL)
    return out


def carteira_de(B, nome, T, H, L, R):
    dt = B["dt"]
    mes_atual = dt["pos"].strftime("%Y-%m")
    T_c = [t for t in T if t["cob"] == nome]
    H_c = [h for h in H if h["cob"] == nome]
    a = agrega_rank([r for r in R if r["cob"] == nome])[nome][mes_atual]
    liq = defaultdict(float)
    for r in L:
        if r["cob"] == nome and r["mes"] == mes_atual:
            liq[r["cli"]] += r["v"]
    de, ate = str(dt["entrou_de"]), str(dt["d0dia"])
    por = defaultdict(list)
    for t in T_c:
        por[t["cli"]].append(t)

    def tit_json(t):
        return {"doc": t["doc"], "tipo": t["tipo"], "emis": t["emis"], "venc": t["venc"],
                "orig": t["orig"], "d": t["d"], "val": round(t["v"] / MIL, 2), "bl": t["bl"],
                "entrou": bool(t["d"] >= 1 and de < (t["venc"] or "") <= ate)}

    def total_json(tot, rotulo):
        return {"doc": f"+{tot['n']} {rotulo}", "tipo": "", "emis": tot["venc"], "venc": tot["venc"],
                "orig": None, "d": tot["d"], "val": round(tot["v"] / MIL, 2), "bl": "", "entrou": False}

    ops = []
    for cli, ts in sorted(por.items(), key=lambda kv: -sum(t["v"] for t in kv[1] if t["d"] >= 0)):
        mv = defaultdict(float)
        for t in ts:
            mv[t["marca"]] += t["v"]
        # detalha o que as listas usam: vencidos (até o limite, mais atrasados e
        # os que entraram em atraso primeiro) e a vencer em até 7 dias
        venc = sorted((t for t in ts if t["d"] >= 0),
                      key=lambda t: (not tit_json(t)["entrou"], -t["d"]))
        perto = [t for t in ts if -DIAS_AV_PAINEL <= t["d"] < 0]
        longe = sorted((t for t in ts if t["d"] < -DIAS_AV_PAINEL), key=lambda t: -t["d"])
        tit = [tit_json(t) for t in venc[:TIT_VENC_PAINEL]] + [tit_json(t) for t in perto]
        if venc[TIT_VENC_PAINEL:]:
            r = venc[TIT_VENC_PAINEL:]
            tit.append(total_json(_linha_total(r, min(t["d"] for t in r)), "títulos vencidos"))
        if longe:
            tit.append(total_json(_linha_total(longe, longe[0]["d"], longe[0]["venc"]),
                                  f"títulos a vencer após {DIAS_AV_PAINEL} dias"))
        ops.append({
            "nome": ts[0]["nome"] or str(cli), "cod": str(cli), "canal": ts[0]["canal"],
            "ms": [m for m, _ in sorted(mv.items(), key=lambda kv: -kv[1])],
            "liq": r1(liq.get(cli, 0.0) / MIL),
            "lojas": [{"nome": ts[0]["nome"] or str(cli), "cod": str(cli), "tit": tit}],
        })
    return {"rec": fr(a["rec"], a["vi"]), "arvore": arvore_de(H_c, T_c), "ops": ops}


def montar_json(B, F, user):
    dt = B["dt"]
    T = [t for t in B["tit"] if passa(t, F)]
    H = [h for h in B["his"] if passa(h, F)]
    L = [r for r in B["liq"] if passa(r, F, False)]
    VM = [r for r in B["vm"] if passa(r, F)]
    R = [r for r in B["ran"] if passa(r, F, False)]

    # equipe (sem o filtro de cobrador, para servir de régua)
    Fe = {k: v for k, v in F.items() if k != "cobrador"}
    Te = [t for t in B["tit"] if passa(t, Fe)]
    car_e = sum(t["v"] for t in Te)
    fx = [0.0, 0.0, 0.0]
    for t in Te:
        if t["d"] >= 0:
            fx[0 if t["d"] <= 30 else 1 if t["d"] <= 90 else 2] += t["v"]
    ae = agrega_rank([dict(r, cob="*") for r in B["ran"] if passa(r, Fe, False)])["*"][dt["pos"].strftime("%Y-%m")]
    equipe = {"inad": fr(sum(fx), car_e), "rec": fr(ae["rec"], ae["vi"]),
              "f": [fr(x, car_e) for x in fx]}

    # Meu painel: gestor vê a cobradora do filtro (ou a primeira da lista);
    # cobradora vê só a(s) própria(s) carteira(s). Só a carteira ativa leva títulos.
    todas = B["opcoes"]["cobrador"]
    if user["gestor"]:
        nomes = todas
        ativa = F.get("cobrador") or next((n for n in todas if n != SEM_COBRADOR), todas[0] if todas else None)
        cart_nomes = [ativa] if ativa else []
    else:
        nomes = user["cobradoras"]
        ativa = nomes[0] if nomes else None
        cart_nomes = nomes
    Hm = [h for h in B["his"] if passa(h, Fe)]
    Lm = [r for r in B["liq"] if passa(r, Fe, False)]
    Rm = [r for r in B["ran"] if passa(r, Fe, False)]

    dados = {
        "posicao": str(dt["pos"]), "mesAnt": str(dt["mes_ant"]),
        "posicaoTexto": posicao_texto(dt, B["comp"]),
        "codCobrador": {n: c for n, c in B["cod_cob"].items() if n in nomes},
        "filtros": dict(B["opcoes"], cobrador=todas if user["gestor"] else nomes),
        "meuPainel": {
            "usuario": {"nome": user["nome"], "gestor": user["gestor"], "cobradora": ativa},
            "cobradoras": nomes,
            "equipe": equipe,
            "carteiras": {n: carteira_de(B, n, Te, Hm, Lm, Rm) for n in cart_nomes},
        },
    }
    if user["gestor"]:
        clientes = montar_clientes(B, T, H, L, VM)
        dados["resumo"] = bloco_resumo(B, T, H, clientes)
        dados["devedores"] = bloco_devedores(B, clientes)
        dados["cobradoras"] = bloco_cobradoras(B, T, L, R)
    return dados


def _json_script(obj):
    txt = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return txt.replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


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


def json_para(B, F, user):
    chave = (id(B), tuple(F.get(k) or "" for k in FILTROS), user["gestor"], tuple(user["cobradoras"]))
    j = _cache_json.get(chave)
    if j is None:
        t0 = time.time()
        j = _json_script(montar_json(B, F, user))
        log.info("JSON montado em %.2fs (%d KB) · filtros=%s", time.time() - t0, len(j) // 1024, chave[1])
        if len(_cache_json) > 64:
            _cache_json.clear()
        _cache_json[chave] = j
    return j


# =====================================================================
# Usuário
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
    mapa = {c: n for n, c in B["cod_cob"].items()}
    cobs = sorted({mapa[c] for c in ACESSOS.get(email, []) if c in mapa})
    return {"email": email, "nome": primeiro_nome(email), "gestor": gestor, "cobradoras": [] if gestor else cobs}


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


def pagina_espera(email):
    erro = _estado["erro"]
    seg = int(time.time() - _estado["inicio"]) if _estado["inicio"] else 0
    if erro:
        detalhe = (f"<p style='color:#C0452B;font-size:12px'>{erro.replace('<', '&lt;')}</p>"
                   if email in GESTORES else "")
        msg = ("<h2>Não foi possível carregar os dados do Databricks.</h2>"
               "<p>Uma nova tentativa é feita a cada atualização desta página. "
               "Se persistir, avise o responsável pelo painel.</p>" + detalhe)
        refresh = 30
    else:
        msg = (f"<h2>Carregando os dados do Contas a Receber…</h2>"
               f"<p>Isso leva de 1 a 3 minutos na primeira vez. "
               f"A página atualiza sozinha. ({seg} s)</p>")
        refresh = 10
    return (f"<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'>"
            f"<meta http-equiv='refresh' content='{refresh}'><title>Contas a Receber</title></head>"
            f"<body style=\"font-family:Raleway,'Segoe UI',Arial,sans-serif;background:#fff;color:#1D2B3F;margin:0\">"
            f"<div style='background:#1D2B3F;color:#F4F1EC;padding:18px 28px;letter-spacing:3px;font-size:13px'>"
            f"CONTAS A RECEBER</div><div style='padding:40px 28px;max-width:720px'>{msg}</div></body></html>")


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
    F = {k: (request.args.get(k) or "").strip() or None for k in FILTROS}
    if not user["gestor"]:
        F["cobrador"] = None  # cobradora não escolhe carteira
    try:
        js = json_para(B, F, user)
    except Exception:
        log.exception("Erro ao montar o JSON do painel")
        return resposta_html("<p style='font-family:Arial'>Erro ao montar o painel. Veja os Registros do app.</p>", 500)
    html = com_portal(template()).replace(MARCADOR_DADOS, js, 1)
    return resposta_html(html)


@app.route("/atualizar", methods=["GET", "POST"])
def atualizar():
    if usuario_email() not in GESTORES:
        return jsonify({"erro": "Só gestores podem atualizar os dados."}), 403
    iniciar_montagem()
    return jsonify({"ok": True, "mensagem": "Atualização iniciada. Recarregue a página em 1 a 3 minutos."})


@app.route("/saude")
def saude():
    b = _cache["base"]
    return jsonify({
        "ok": True,
        "warehouse_configurado": bool(WAREHOUSE_ID),
        "template_encontrado": ARQUIVO_HTML.exists(),
        "marcador_no_template": ARQUIVO_HTML.exists() and MARCADOR_DADOS in template(),
        "dados_em_cache": b is not None,
        "posicao": str(b["dt"]["pos"]) if b else None,
        "montado_em": b["montado_em"] if b else None,
        "titulos_em_aberto": len(b["tit"]) if b else None,
        "montando": _estado["montando"],
        "erro": _estado["erro"],
    })


if __name__ == "__main__":
    iniciar_montagem()
    app.run(host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8000")))