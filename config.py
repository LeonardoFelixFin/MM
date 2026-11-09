"""
CONFIG:    portal-fin-contas-receber · configurações editáveis
O QUE FAZ: Acessos, canais, marcas e link do hub.
           Para mudar qualquer item, edite só este arquivo e clique em Implementar.
HISTÓRICO: 05/11/2026 · v2.0 · criação (app refeito com o template v11 da Vera).
           05/11/2026 · v2.2 · sai COBRADORES: o nome vem da coluna nome_cobrador da view.
           06/11/2026 · v3.0 · modelo oficial (4 abas). Saem PDD, METAS e
                        DIAS_A_VENCER_TITULOS, que o modelo novo não usa.
           06/11/2026 · v3.1 · sai TIPOS_DOCUMENTO_EM_ABERTO: os tipos ficam
                        fixos no SQL (K1), sem montar consulta com variáveis.
           06/11/2026 · v3.1 · Ritiele entra em GESTORES (login rdantunes).
"""

# Arquivo da página (contas_receber_oficial.html da Vera, sem alteração)
ARQUIVO_HTML = "template.html"

# Endereço do hub portal-financeiro (botão "← Portal Financeiro")
PORTAL_URL = "https://portal-financeiro-3879198595418316.aws.databricksapps.com"

# E-mails (minúsculas) que veem todas as abas e todas as carteiras.
# Use o e-mail do LOGIN no Databricks (pode ser diferente do e-mail de uso).
GESTORES = {
    "lfelsantos@arezzo.com.br",
    "leonardo.santos@arezzo.com.br",
    "rdantunes@arezzo.com.br",          # Ritiele
    "ritiele.antunes@arezzo.com.br",    # Ritiele (e-mail de uso)
}

# E-mail (minúsculas) -> códigos de cobrador (como aparecem em cobrador_codigo).
# Quem está aqui e não é gestor vê só a aba Meu painel, com a própria carteira.
ACESSOS = {
    # "fulana@arezzo.com.br": ["001"],
}

# Canal ajustado -> nome exibido
CANAIS = {
    "40": "Franquias",
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
# Sexta-feira Santa e Corpus Christi). Use para os feriados de Campo Bom.
# Formato "AAAA-MM-DD". Some quando a tb_calendario_recebimento do Codd entrar.
FERIADOS_EXTRAS = [
    # "2026-xx-xx",
]