# GCON/SIAN — Sistema de Extração NF-e / NFS-e

Aplicação desktop para leitura em lote de XMLs de **Notas Fiscais Eletrônicas (NF-e)** e **Notas Fiscais de Serviço Eletrônicas (NFS-e)**, com extração de impostos, exportação para CSV/Excel e interface gráfica multiusuário.

---

## Funcionalidades

- Importação em lote de XMLs (NF-e modelo 55 e NFS-e)
- Detecção automática do tipo de nota (NF-e × NFS-e)
- Suporte a dois formatos de NFS-e:
  - **CompNFe** — padrão municipal legado (MeuResíduo, Vogel, Carraro, Thiago...)
  - **NFSe Nacional** — padrão SPED/Fazenda (MJOTA, LEXPLAN, Ruber, Stylus, Onfly novo...)
- Extração de impostos por produto (NF-e): ICMS, IPI, PIS, COFINS, IBS/CBS
- Extração de impostos por nota (NFS-e): ISS, PIS, COFINS, CSLL, IRRF, INSS, IBS/CBS
- Deduplicação automática — nunca duplica notas já importadas
- Planilha interativa com filtro, soma de seleção e exportação CSV
- Dashboard com gráficos de barras e pizza (sem matplotlib)
- Múltiplos usuários simultâneos via arquivos temporários isolados por sessão
- Excel principal atualizado automaticamente após cada importação

---

## Estrutura do projeto

```
XmlProcessor/
│
├── main.py                    ← ponto de entrada — execute este
│
├── config/
│   └── settings.py            ← caminhos, sessão, cabeçalhos (71 campos NF-e, 52 NFS-e)
│
├── extract/
│   ├── xml_reader.py          ← NF-e modelo 55: ICMS, IPI, PIS, COFINS, IBS/CBS por produto
│   └── nfse_reader.py         ← NFS-e: CompNFe + NFSe Nacional, detecção automática
│
├── transform/
│   └── validator.py           ← normalização e deduplicação de registros
│
├── load/
│   └── storage.py             ← CSV/Excel temporário e principal, sincronização, backup
│
├── ui/
│   └── main_window.py         ← interface: sidebar, planilhas, dashboard, log
│
└── diagnostico.py             ← script de diagnóstico para resolução de problemas
```

---

## Pré-requisitos

- Python 3.10 ou superior
- Dependências:

```bash
pip install pandas openpyxl customtkinter
```

---

## Como rodar

```powershell
# Ativar ambiente virtual
.venv\Scripts\activate

# Executar
python main.py
```

---

## Arquivos gerados

Criados automaticamente na **pasta do projeto**:

| Arquivo | Conteúdo |
|---|---|
| `produtos_nfe.csv` | Todos os produtos NF-e extraídos (acumulado) |
| `produtos_nfe.xlsx` | Excel formatado NF-e |
| `servicos_nfse.csv` | Todas as notas NFS-e extraídas (acumulado) |
| `servicos_nfse.xlsx` | Excel formatado NFS-e |
| `*_backup_*.csv` | Backup automático antes de cada sincronização |

Arquivos temporários ficam em `%TEMP%\leitor_xml_multiusuario\` e são limpos ao fechar.

---

## Fluxo ETL

```
XMLs selecionados
       │
       ▼  extrair_produtos() / extrair_servicos()
       │  detecta CompNFe ou NFSe Nacional pela tag raiz
       │
       ▼  salvar_produtos_csv() / salvar_nfse_csv()
       │  append no CSV temporário da sessão
       │
       ▼  salvar_tudo()
       │  temp → principal (deduplicado por chave)
       │  CSV principal → Excel principal (formatado)
       │
       ▼  planilha / dashboard
          lê sempre o CSV mais recente (temp > principal)
```

---

## Como funciona a detecção de formato NFS-e

```python
tag raiz = "CompNFe"  →  _extrair_compnfe()        # municipal legado
tag raiz = "NFSe"     →  _extrair_nfse_nacional()   # padrão SPED/Fazenda
```

Os dois formatos têm estruturas XML completamente diferentes. Cada função sabe onde buscar cada campo. Ambas retornam o mesmo dicionário de 52 campos — o resto do sistema não precisa saber qual formato veio.

---

## Campos extraídos — NF-e (71 campos)

| Grupo | Campos |
|---|---|
| Nota | Tipo_Nota, Chave_NFe, Numero_NFe, Serie_NFe, Mod_NFe, NatOp, Tp_NF, Data_Emissao |
| Emitente | CNPJ, Nome, NomeFantasia, IE, UF, Municipio |
| Destinatário | CNPJ/CPF, Nome, IE, UF, Municipio |
| Produto | Item, cProd, cEAN, xProd, NCM, CEST, CFOP, uCom, qCom, vUnCom, vProd, indEscala, nFCI |
| ICMS | orig, CST, modBC, vBC, pICMS, vICMS, ST, Efetivo |
| IPI | cEnq, CST, vBC, pIPI, vIPI |
| PIS | CST, vBC, pPIS, vPIS |
| COFINS | CST, vBC, pCOFINS, vCOFINS |
| IBS/CBS | CST, cClassTrib, vBC, pIBSUF, vIBSUF, pIBSMun, vIBSMun, vIBS, pCBS, vCBS |
| Meta | Arquivo_Origem |

## Campos extraídos — NFS-e (52 campos)

| Grupo | Campos |
|---|---|
| Nota | Tipo_Nota, Formato, Chave_NFSe, Numero_NFSe, Serie_RPS, Data_Emissao, Data_Competencia, Municipio |
| Serviço | cTribNac, xDescServ, cNBS_DPS, Cod_Servico_Mun, Desc_Servico, Cod_Item_Lei116, Cod_NBS |
| ISS | ISS_Retido, BC_ISS, Aliq_ISS, Valor_ISS, pAliq_ISS, tpRetISSQN |
| CSRF | BC_CSRF, Valor_PIS, Valor_COFINS, Valor_CSLL, BC_IRRF, Valor_IRRF, BC_INSS, Valor_INSS |
| Simples/IBS/CBS | pTotTribSN, IBS_vBC, IBS_pIBSUF, IBS_pIBSMun, CBS_pCBS |
| Valores | Valor_Bruto, Valor_Liquido, Discriminacao |
| Prestador | CNPJ, IM, Nome, NomeFantasia, UF, Municipio, Email, Simples_Nacional |
| Tomador | CNPJ, IM, Nome, Municipio, UF, Email |
| Meta | Arquivo_Origem |

---

## Capacidade estimada

| Quantidade | Tempo estimado |
|---|---|
| 100 XMLs | ~0,2s |
| 1.000 XMLs | ~2s |
| 5.000 XMLs | ~10s |
| 10.000 XMLs | ~21s |

Memória não é limitante — processamento em streaming, um arquivo por vez.

---

## Diagnóstico

```bash
python diagnostico.py
```

Verifica: versão dos arquivos, dados existentes, dependências e teste de extração.

```powershell
# Limpar cache Python após atualizar arquivos
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```
