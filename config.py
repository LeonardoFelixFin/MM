"""
CONFIG:    portal-fin-contas-receber · configurações editáveis
O QUE FAZ: Acessos, marcas e link do hub.
           Para mudar qualquer item, edite só este arquivo e clique em Implementar.
HISTÓRICO: 05/11/2026 · v2.0 · criação (app refeito com o template v11 da Vera).
           05/11/2026 · v2.2 · sai COBRADORES: o nome vem da coluna nome_cobrador da view.
           06/11/2026 · v3.0 · modelo oficial (4 abas). Saem PDD, METAS e
                        DIAS_A_VENCER_TITULOS, que o modelo novo não usa.
           06/11/2026 · v3.1 · sai TIPOS_DOCUMENTO_EM_ABERTO: os tipos ficam
                        fixos no SQL (K1), sem montar consulta com variáveis.
           06/11/2026 · Ritiele (rdantunes) entra em GESTORES.
           10/11/2026 · v4.0 · layout v3: sai AGRUPAR_SEM_OPERADOR (SEM OPERADOR
                        vira uma linha única no fim da lista, como no layout).
           10/11/2026 · v4.1 · canais agrupados (CANAL_GRUPO): 40 Franquia;
                        50 e 55 Multimarcas; 70, 76 e vazio Outros.
"""

# Arquivo da página (contas_receber_oficial.html da Vera, sem alteração)
ARQUIVO_HTML = "template.html"

# Endereço do hub portal-financeiro (botão "← Portal Financeiro")
PORTAL_URL = "https://portal-financeiro-3879198595418316.aws.databricksapps.com"

# E-mails (minúsculas) que veem todas as abas e todas as carteiras
GESTORES = {
    "lfelsantos@arezzo.com.br",
    "leonardo.santos@arezzo.com.br",
    "rdantunes@arezzo.com.br",
    "ritiele.antunes@arezzo.com.br",
    "atmotta@arezzo.com.br",
}

# E-mail (minúsculas) -> códigos de cobrador (como aparecem em cobrador_codigo).
# Quem está aqui e não é gestor vê só a aba Meu painel, com a própria carteira.
ACESSOS = {
    # "fulana@arezzo.com.br": ["001"],
}

# Canal ajustado (exato da view) -> grupo usado no painel e nos filtros.
# Qualquer canal fora desta lista (70, 76, vazio, outros) cai em CANAL_OUTROS.
CANAL_GRUPO = {
    "40": "40",   # Franquia
    "50": "50",   # Multimarcas
    "55": "50",   # Multimarcas
}
CANAL_OUTROS = "70"

# Grupo -> nome exibido no filtro
CANAIS = {
    "40": "Franquia",
    "50": "Multimarcas",
    "70": "Outros",
}

# Organização de vendas -> marca (K1)
MARCAS = {
    "1101": "Arezzo",
    "1102": "Schutz",
    "1103": "Anacapri",
    "1104": "Alexandre Birman",
    "1107": "Alme",
    "1110": "Corporativo",
    "1111": "Vans",
    "1121": "Reserva GO",
    "1122": "Reserva Mini",
    "1123": "Oficina Reserva",
    "1124": "Baw",
    "1125": "My Shoes",
    "1126": "Reserva",
    "1127": "Ink",
    "1128": "Reversa",
    "1131": "Brizza Arezzo",
    "1132": "Vicenza",
    "1136": "Hering",
    "1137": "Farm",
    "1165": "Arezzo Star",
}
MARCA_PADRAO = "Corporativo"

# Organizações do grupo AR&CO (as demais são "Marcas Core")
ORGS_ARCO = ["1121", "1122", "1123", "1124", "1126", "1127", "1128"]

# Feriados além dos nacionais (que o app já calcula, inclusive Carnaval,
# Sexta-feira Santa e Corpus Christi). Formato "AAAA-MM-DD".
# Campo Bom não tem feriado municipal (K1, 05/11/2026).
FERIADOS_EXTRAS = [
    # "2026-xx-xx",
]
