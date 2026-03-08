import os, tempfile, uuid
from datetime import datetime

CTK_APPEARANCE = "light"
CTK_COLOR_THEME = "blue"

PASTA_BASE = r"C:\Users\ana.oliveira\Downloads\Codigos\leitor.xml"
os.makedirs(PASTA_BASE, exist_ok=True)

TEMP_DIR = os.path.join(tempfile.gettempdir(), "leitor_xml_multiusuario")
os.makedirs(TEMP_DIR, exist_ok=True)

SESSAO_ID  = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
USUARIO_ID = os.environ.get("USERNAME", "usuario_desconhecido")

CSV_PRINCIPAL   = os.path.join(PASTA_BASE, "produtos_nfe.csv")
EXCEL_PRINCIPAL = os.path.join(PASTA_BASE, "produtos_nfe.xlsx")
LOG_PRINCIPAL   = os.path.join(PASTA_BASE, "log_processamento.txt")

CSV_TEMP   = os.path.join(TEMP_DIR, f"temp_produtos_{USUARIO_ID}_{SESSAO_ID}.csv")
EXCEL_TEMP = os.path.join(TEMP_DIR, f"temp_excel_{USUARIO_ID}_{SESSAO_ID}.xlsx")
LOG_TEMP   = os.path.join(TEMP_DIR, f"temp_log_{USUARIO_ID}_{SESSAO_ID}.txt")
LOCK_FILE  = os.path.join(TEMP_DIR, f"lock_{USUARIO_ID}_{SESSAO_ID}.lock")

# ── Cabeçalho NF-e (produto) ──────────────────────────────────────────────────
CABECALHO_NFE = [
    # Nota
    "Tipo_Nota","Chave_NFe","Numero_NFe","Serie_NFe","Mod_NFe",
    "NatOp","Tp_NF","Data_Emissao",
    # Emitente
    "CNPJ_Emitente","Nome_Emitente","NomeFantasia_Emit","IE_Emitente",
    "UF_Emitente","Mun_Emitente",
    # Destinatário
    "CNPJ_Destinatario","CPF_Destinatario","Nome_Destinatario",
    "IE_Destinatario","UF_Destinatario","Mun_Destinatario",
    # Produto
    "Item","cProd","cEAN","xProd","NCM","CEST","CFOP",
    "uCom","qCom","vUnCom","vProd","indEscala","nFCI",
    # ICMS
    "ICMS_orig","ICMS_CST","ICMS_modBC","ICMS_vBC","ICMS_pICMS","ICMS_vICMS",
    "ICMS_vBCSTRet","ICMS_pST","ICMS_vICMSSubstituto","ICMS_vICMSSTRet",
    "ICMS_pRedBCEfet","ICMS_vBCEfet","ICMS_pICMSEfet","ICMS_vICMSEfet",
    # IPI
    "IPI_cEnq","IPI_CST","IPI_vBC","IPI_pIPI","IPI_vIPI",
    # PIS
    "PIS_CST","PIS_vBC","PIS_pPIS","PIS_vPIS",
    # COFINS
    "COFINS_CST","COFINS_vBC","COFINS_pCOFINS","COFINS_vCOFINS",
    # IBS/CBS (Reforma Tributária)
    "IBS_CST","cClassTrib","IBS_vBC",
    "pIBSUF","vIBSUF","pIBSMun","vIBSMun",
    "IBS_vIBS","pCBS","CBS_vCBS",
    # Meta
    "Arquivo_Origem",
]
CABECALHO_CSV = CABECALHO_NFE  # alias

# ── Cabeçalho NFS-e (serviço) ─────────────────────────────────────────────────
CABECALHO_NFSE = [
    "Tipo_Nota","Formato","Chave_NFSe","Numero_NFSe","Serie_RPS",
    "Data_Emissao","Data_Competencia","Municipio_Prestacao",
    "cTribNac","xDescServ","cNBS_DPS",
    "Cod_Servico_Mun","Desc_Servico","Cod_Item_Lei116","Cod_NBS",
    # ISS
    "ISS_Retido","BC_ISS","Aliq_ISS","Valor_ISS","pAliq_ISS","tpRetISSQN",
    # CSRF (DPS)
    "BC_CSRF",
    "Valor_PIS","Valor_COFINS","Valor_CSLL",
    "BC_IRRF","Valor_IRRF",
    "BC_INSS","Valor_INSS",
    # Simples
    "pTotTribSN",
    # IBS/CBS
    "IBS_vBC","IBS_pIBSUF","IBS_pIBSMun","CBS_pCBS",
    # Valores
    "Valor_Bruto","Valor_Liquido","Discriminacao",
    # Prestador
    "CNPJ_Prestador","IM_Prestador","Nome_Prestador","NomeFantasia_Prestador",
    "UF_Prestador","Mun_Prestador","Email_Prestador","Simples_Nacional",
    # Tomador
    "CNPJ_Tomador","IM_Tomador","Nome_Tomador",
    "Mun_Tomador","UF_Tomador","Email_Tomador",
    # Meta
    "Arquivo_Origem",
]

LOCK_TTL_SECONDS = 300
TEMP_TTL_SECONDS = 3600

# Arquivos NFS-e
CSV_NFSE_TEMP        = CSV_TEMP.replace("temp_produtos_", "temp_nfse_")
EXCEL_NFSE_TEMP      = EXCEL_TEMP.replace("temp_excel_", "temp_excel_nfse_")
CSV_NFSE_PRINCIPAL   = os.path.join(PASTA_BASE, "servicos_nfse.csv")
EXCEL_NFSE_PRINCIPAL = os.path.join(PASTA_BASE, "servicos_nfse.xlsx")
