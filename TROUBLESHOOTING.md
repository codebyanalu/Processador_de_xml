# TROUBLESHOOTING — GCON/SIAN

Guia de diagnóstico e resolução dos problemas mais comuns.

---

## Diagnóstico rápido

Antes de qualquer coisa, execute o script de diagnóstico:

```bash
python diagnostico.py
```

Ele verifica automaticamente versão dos arquivos, dados existentes e dependências.

---

## Índice de problemas

1. [Erro ao iniciar — ModuleNotFoundError](#1-erro-ao-iniciar--modulenotfounderror)
2. [Planilha NF-e ou NFS-e abre vazia](#2-planilha-nf-e-ou-nfs-e-abre-vazia)
3. [Excel não atualiza após importar XMLs](#3-excel-não-atualiza-após-importar-xmls)
4. [Nota não aparece após importar](#4-nota-não-aparece-após-importar)
5. [Campos de impostos vazios (IBS/CBS, ISS...)](#5-campos-de-impostos-vazios-ibscbs-iss)
6. [Tela em branco ao abrir janela](#6-tela-em-branco-ao-abrir-janela)
7. [Arquivo desatualizado — sistema com comportamento antigo](#7-arquivo-desatualizado--sistema-com-comportamento-antigo)
8. [Edição manual no código não tem efeito](#8-edição-manual-no-código-não-tem-efeito)
9. [Erro de permissão ao executar .ps1](#9-erro-de-permissão-ao-executar-ps1)
10. [NFS-e Nacional não extrai impostos](#10-nfs-e-nacional-não-extrai-impostos)
11. [TypeError: got multiple values for keyword argument 'fg'](#11-typeerror-got-multiple-values-for-keyword-argument-fg)

---

## 1. Erro ao iniciar — ModuleNotFoundError

### Sintoma
```
ModuleNotFoundError: No module named 'ui.storage'
ModuleNotFoundError: No module named 'pandas'
```

### Causa A — `ui/__init__.py` com conteúdo errado
O arquivo `ui/__init__.py` deve conter **apenas**:
```python
from .main_window import AplicacaoLeitorXML
```

Verifique o conteúdo:
```powershell
Get-Content "ui\__init__.py"
```

Se estiver errado, corrija:
```powershell
Set-Content "ui\__init__.py" "from .main_window import AplicacaoLeitorXML"
```

### Causa B — cache `.pyc` antigo
```powershell
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

### Causa C — dependência não instalada
```bash
pip install pandas openpyxl customtkinter
```

---

## 2. Planilha NF-e ou NFS-e abre vazia

### Causa A — nenhum XML foi importado ainda
A planilha só tem dados após importar XMLs na sessão atual **ou** se já existirem os arquivos `produtos_nfe.csv` / `servicos_nfse.csv` na pasta do projeto.

**Solução:** importe XMLs primeiro clicando em "Selecionar XMLs".

### Causa B — encoding do CSV (acentos quebrando leitura)
CSVs criados em Windows podem ter encoding `latin-1`. O sistema tenta `utf-8 → utf-8-sig → latin-1` automaticamente. Se ainda estiver vazio, verifique se o CSV tem conteúdo:

```powershell
(Get-Content "servicos_nfse.csv").Count
```

Se retornar 1 (só cabeçalho), o CSV está vazio — importe os XMLs novamente.

### Causa C — CSV temporário da sessão vazio
Os CSVs temporários ficam em `%TEMP%\leitor_xml_multiusuario\`. Ao abrir o sistema, ele copia o CSV principal para o temporário. Se o principal não existia, o temporário começa vazio.

**Solução:** processe XMLs — o sistema criará e preencherá os arquivos automaticamente.

### Causa D — CSV com cabeçalho de versão anterior *(causa mais comum após atualizar o sistema)*
Quando o sistema é atualizado e o `servicos_nfse.csv` foi gerado por uma versão mais antiga, o arquivo tem menos colunas. O sistema atual consegue ler e exibir esses dados normalmente — colunas novas aparecem vazias e os dados antigos são preservados.

Se mesmo assim a planilha abrir vazia após atualizar os arquivos:

1. Confirme que o `main_window.py` é o mais recente (deve conter `_csv_nfse` com contagem de linhas)
2. Limpe o cache:
```powershell
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```
3. Execute novamente. Se os dados ainda não aparecerem, reimporte os XMLs — o novo CSV será gerado já com o cabeçalho atualizado de 56 campos.

---

## 3. Excel não atualiza após importar XMLs

### Comportamento esperado
A partir da versão atual, o Excel é atualizado **automaticamente** ao final de cada importação. O log mostra:
```
excel_nfe    : OK — Excel NF-e atualizado (X registros)
excel_nfse   : OK — Excel NFS-e atualizado (X registros)
```

### Se ainda não atualiza
1. Verifique se o arquivo Excel está **aberto** — o Excel bloqueia escrita enquanto está aberto. Feche o arquivo e reimporte.
2. Clique em **Sincronizar Tudo** na sidebar para forçar atualização manual.
3. Verifique se o `main_window.py` está atualizado (o log deve mostrar as 4 linhas de sincronização após processar).

---

## 4. Nota não aparece após importar

### Causa — deduplicação por chave
O sistema nunca duplica uma nota já importada. A chave de deduplicação é:

- **NF-e:** `Chave_NFe + Item + cProd`
- **NFS-e:** `Chave_NFSe + Numero_NFSe`

Se a nota já estava no CSV principal de uma sessão anterior, ela será ignorada silenciosamente.

**Como verificar:** procure a chave da nota no CSV principal:
```powershell
Select-String -Path "servicos_nfse.csv" -Pattern "CHAVE_DA_NOTA"
```

**Para reimportar uma nota já existente:** delete o CSV principal (`servicos_nfse.csv` ou `produtos_nfe.csv`) e reimporte todos os XMLs. Os backups ficam com sufixo `_backup_DATA.csv`.

---

## 5. Campos de impostos vazios (IBS/CBS, ISS...)

### NFS-e — IBS/CBS vazio
IBS e CBS são impostos da **Reforma Tributária** (vigência gradual a partir de 2026). A maioria das notas de 2025 não tem esses campos. É comportamento normal — os campos ficam vazios.

Notas que **devem ter** IBS/CBS: emitidas por prestadores já enquadrados na reforma (ex: Onfly a partir de março/2026).

### NFS-e Nacional — ISS, PIS, COFINS vazios
Algumas notas do padrão nacional não informam PIS/COFINS/CSLL/IRRF/INSS quando o prestador é optante do Simples Nacional (campo `Simples_Nacional = 1`). Nesse caso só o ISS é obrigatório.

### NF-e — IPI vazio
IPI só existe em notas de produtos industrializados. Para notas de revenda de peças ou materiais de uso e consumo, o campo IPI fica vazio — comportamento correto.

### NF-e — ICMS vazio
Pode ocorrer em notas com CFOP de devolução ou notas de simples remessa. Verifique o `ICMS_CST` — alguns CSTs (ex: 40, 41, 50) indicam isenção e não têm base de cálculo.

---

## 6. Tela em branco ao abrir janela

### Causa — mistura de `pack()` e `grid()` no mesmo container tkinter
O tkinter cancela a renderização silenciosamente quando um container usa `pack` e `grid` ao mesmo tempo.

### Diagnóstico
Se uma janela (planilha, dashboard) abre mas fica totalmente branca, verifique se o `main_window.py` está atualizado. A versão correta usa **pack puro** em todas as janelas.

### Solução
Substitua o `ui/main_window.py` pela versão mais recente e limpe o cache:
```powershell
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

---

## 7. Arquivo desatualizado — sistema com comportamento antigo

### Como identificar
Execute `diagnostico.py` — ele verifica o conteúdo funcional de cada arquivo (não só o hash):
```
✓ ui/main_window.py     leitura CSV=OK  gráficos=OK
✗ extract/xml_reader.py IPI=FALTA  IBS/CBS=FALTA
```

### Solução
Substitua apenas os arquivos marcados com `✗`. Depois limpe o cache:
```powershell
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

---

## 8. Edição manual no código não tem efeito

Três motivos possíveis:

**A — Cache `.pyc` antigo**
O Python usa o `.pyc` compilado se a data de modificação do `.py` não mudou corretamente.
```powershell
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

**B — Encoding errado ao salvar**
O Bloco de Notas salva por padrão em `UTF-16` ou `ANSI` dependendo da versão. Python exige `UTF-8`.
Use o VS Code ou salve via PowerShell:
```powershell
Set-Content -Path "caminho\arquivo.py" -Value (Get-Content "caminho\arquivo.py") -Encoding UTF8
```

**C — Editou o arquivo errado**
Confirme o conteúdo real do arquivo que o Python está lendo:
```powershell
Get-Content "ui\__init__.py"
```

---

## 9. Erro de permissão ao executar .ps1

### Sintoma
```
execution of scripts is disabled on this system
```

### Solução
```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

Depois execute o script normalmente:
```powershell
& "C:\Users\ana.oliveira\Downloads\atualizar.ps1"
```

---

## 10. NFS-e Nacional não extrai impostos

### Sintoma
Notas do padrão NFSe Nacional (SPED/Fazenda) aparecem na planilha mas com IBS/CBS e outros impostos vazios.

### Causa
O bloco `<IBSCBS>` no formato Nacional fica dentro de `<infNFSe>`, não dentro de `<DPS>`. Versões antigas do `nfse_reader.py` não buscavam nesse local.

### Verificação
```bash
python diagnostico.py
```
O arquivo `extract/nfse_reader.py` deve mostrar `IPI=OK  IBS/CBS=OK` — mas para NFS-e o relevante é que a função `_extrair_nfse_nacional` tenha o bloco de extração IBS/CBS buscando em `inf.find(".//IBSCBS")`.

### Solução
Substitua o `extract/nfse_reader.py` pela versão mais recente.

---

## 11. TypeError: got multiple values for keyword argument 'fg'

### Sintoma
```
TypeError: tkinter.Button() got multiple values for keyword argument 'fg'
```
O erro ocorre ao clicar em "Ver NFS-e (Planilha)" — a janela não abre.

### Causa
A função `_btn` define `fg="white"` internamente, mas a chamada do botão **✕** do filtro passava `fg=C_TEXTO` como parâmetro extra. O Python não aceita o mesmo argumento duas vezes.

### Solução
Substitua o `ui/main_window.py` pela versão mais recente. A correção foi fazer `_btn` aceitar `fg` como parâmetro opcional:

```python
def _btn(parent, texto, cmd, bg, hv, **kw):
    fg = kw.pop("fg", "white")   # usa fg passado ou branco como padrão
    return tk.Button(parent, ..., fg=fg, ...)
```

Após substituir o arquivo, limpe o cache:
```powershell
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

---

## Referência rápida de comandos

```powershell
# Limpar cache Python
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force

# Verificar conteúdo de um arquivo
Get-Content "ui\__init__.py"

# Corrigir ui/__init__.py
Set-Content "ui\__init__.py" "from .main_window import AplicacaoLeitorXML"

# Verificar quantas linhas tem um CSV
(Get-Content "servicos_nfse.csv").Count

# Buscar uma chave num CSV
Select-String -Path "servicos_nfse.csv" -Pattern "CHAVE_AQUI"

# Instalar dependências
pip install pandas openpyxl customtkinter

# Executar diagnóstico
python diagnostico.py

# Executar sistema
python main.py
```
