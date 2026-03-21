"""
ui/main_window.py  —  GCON/SIAN  v3.0
Correções definitivas:
  - Leitura via CSV direto (não Excel) para visualização — mais confiável
  - NUNCA mistura pack() e grid() no mesmo container
  - Dashboard com gráficos canvas (barras + pizza) — sem dependências externas
  - Log completo por arquivo
  - NFS-e planilha com todos os campos e valores
"""

import importlib, math, os, sys, threading, tkinter as tk
import multiprocessing as mp
from tkinter import filedialog, messagebox, scrolledtext, ttk
from datetime import datetime
import customtkinter as ctk
import pandas as pd

import config.settings as cfg
from extract import extrair_produtos, extrair_servicos
from transform import filtrar_novos, carregar_chaves_existentes
from load import (
    inicializar_sessao, verificar_locks_ativos,
    salvar_produtos_csv, salvar_nfse_csv,
    sincronizar_excel_temp, sincronizar_excel_nfse_temp,
    sincronizar_com_principal, sincronizar_nfse_com_principal,
    atualizar_excel_principal, atualizar_excel_nfse_principal,
    limpar_temporarios, total_registros, carregar_chaves_nfse,
    salvar_tudo, salvar_excel_sessao,
)
from config.settings import CABECALHO_CSV, CABECALHO_NFSE

# ─── Paleta ───────────────────────────────────────────────────────────────────
C_PRIM  = "#1a5276"; C_SEC   = "#2980b9"; C_ACENT = "#e67e22"
C_FUNDO = "#f0f3f7"; C_F2    = "#ffffff"; C_SIDE  = "#1c2833"
C_SIDE2 = "#2c3e50"; C_TEXTO = "#1a252f"; C_TEX2  = "#5d6d7e"
C_BORDA = "#d5d8dc"; C_OK    = "#1e8449"; C_WARN  = "#d68910"
C_ERR   = "#c0392b"; C_INFO  = "#2471a3"
PALETA  = ["#2980b9","#e67e22","#27ae60","#8e44ad",
           "#c0392b","#16a085","#d35400","#1a5276","#7d6608","#117a65"]
FONTE_LOG = ("Consolas", 10)
LOTE_MAX  = 500

# ─── Worker de processamento (fora da classe — obrigatório para multiprocessing) ─

def _worker_processar(arquivos, csv_temp, csv_nfse_temp, cabecalho_csv, cabecalho_nfse,
                      chaves_nfe, chaves_nfse, fila):
    """Roda em processo separado. Envia eventos para a fila para a UI consumir."""
    import os, csv as csvmod
    from extract import extrair_produtos, extrair_servicos
    from transform import filtrar_novos

    def _tipo(caminho):
        try:
            with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                t = f.read(600)
            return "nfse" if any(x in t for x in ["CompNFe","<NFSe","infNFSe","nNFSe"]) else "nfe"
        except Exception:
            return "nfe"

    def _salvar(regs, caminho, cabecalho):
        if not regs: return
        existe = os.path.exists(caminho) and os.path.getsize(caminho) > 0
        with open(caminho, "a", newline="", encoding="utf-8") as f:
            w = csvmod.DictWriter(f, fieldnames=cabecalho, extrasaction="ignore")
            if not existe: w.writeheader()
            for r in regs:
                w.writerow({k: r.get(k, "") for k in cabecalho})

    total = len(arquivos)
    lote_nfe = []; lote_nfse = []
    cnt_nfe = cnt_nfse = add_nfe = add_nfse = err_nfe = err_nfse = 0

    def _chave_nfse(r):
        cnpj_raiz = (r.get('CNPJ_Prestador','') or '').replace('.','').replace('/','').replace('-','')[:8].zfill(8)
        return f"{r.get('Numero_NFSe','')}_{cnpj_raiz}"

    for i, arq in enumerate(arquivos, 1):
        nome = os.path.basename(arq)
        tipo = _tipo(arq)

        if tipo == "nfse":
            cnt_nfse += 1
            regs, msg = extrair_servicos(arq)
            if msg.startswith("ERRO"):
                err_nfse += 1
                fila.put(("log", "err", f"  [{i:>4}/{total}] [NFS-e] ⚠  {nome[:48]}  →  {msg[:50]}"))
            else:
                novos = [r for r in regs if _chave_nfse(r) not in chaves_nfse]
                for r in novos:
                    chaves_nfse.add(_chave_nfse(r))
                lote_nfse.extend(novos); add_nfse += len(novos)
                fila.put(("log", "nfse", f"  [{i:>4}/{total}] [NFS-e] {nome[:48]:<50}  {len(novos):>3} novo(s)  [{msg[:30]}]"))
        else:
            cnt_nfe += 1
            regs, msg = extrair_produtos(arq)
            if msg.startswith("ERRO"):
                err_nfe += 1
                fila.put(("log", "err", f"  [{i:>4}/{total}] [NF-e]  ⚠  {nome[:48]}  →  {msg[:50]}"))
            else:
                novos, _ = filtrar_novos(regs, chaves_nfe)
                for r in novos:
                    chaves_nfe.add(f"{r.get('Chave_NFe','')}_{r.get('Item','')}_{r.get('cProd','')}")
                lote_nfe.extend(novos); add_nfe += len(novos)
                fila.put(("log", "nfe", f"  [{i:>4}/{total}] [NF-e]  {nome[:48]:<50}  {len(novos):>3} novo(s)  {len(regs)} itens"))

        # Salva lotes
        if len(lote_nfe)  >= LOTE_MAX:
            _salvar(lote_nfe, csv_temp, cabecalho_csv); lote_nfe = []
        if len(lote_nfse) >= LOTE_MAX:
            _salvar(lote_nfse, csv_nfse_temp, cabecalho_nfse); lote_nfse = []

        fila.put(("progresso", i, total, cnt_nfe, cnt_nfse))

    # Salva restos
    _salvar(lote_nfe,  csv_temp,      cabecalho_csv)
    _salvar(lote_nfse, csv_nfse_temp, cabecalho_nfse)
    fila.put(("fim", cnt_nfe, cnt_nfse, add_nfe, add_nfse, err_nfe, err_nfse))



def _detectar_tipo(caminho):
    try:
        with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
            t = f.read(600)
        return "nfse" if any(x in t for x in ["CompNFe","<NFSe","infNFSe","nNFSe"]) else "nfe"
    except Exception:
        return "nfe"

def _f(v):
    """Converte para float com segurança."""
    try:
        s = str(v).strip().replace(",","")
        return float(s) if s and s not in ("nan","None","") else 0.0
    except:
        return 0.0

def _moeda(v):
    x = _f(v); return f"R$ {x:,.2f}" if x else ""

def _vl(v):
    """Valor de célula limpo."""
    s = str(v).strip()
    return "" if s in ("nan","None","") else s

def _btn(parent, texto, cmd, bg, hv, **kw):
    fg = kw.pop("fg", "white")
    return tk.Button(parent, text=texto, command=cmd, bg=bg, fg=fg,
                     font=("Segoe UI",10,"bold"), relief="flat",
                     padx=12, pady=5, cursor="hand2",
                     activebackground=hv, activeforeground="white", bd=0, **kw)

def _ler_csv(caminho, cabecalho):
    """Lê CSV → DataFrame com colunas garantidas.
    Robusto a cabeçalho antigo: preserva colunas existentes e preenche
    colunas novas com ''. Retorna None se vazio/inexistente/inválido."""
    if not os.path.exists(caminho):
        return None
    df = None
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            df = pd.read_csv(caminho, dtype=str, encoding=enc, on_bad_lines="skip")
            break
        except UnicodeDecodeError:
            continue
        except Exception:
            return None
    if df is None or df.empty:
        return None
    # Precisa ter pelo menos uma coluna reconhecida (CSV válido para este tipo)
    if not (set(df.columns) & set(cabecalho)):
        return None
    return df.reindex(columns=cabecalho, fill_value="")

# ─── Widgets: Treeview ────────────────────────────────────────────────────────

def _estilo_tree(nome, cor_hdr=None):
    cor_hdr = cor_hdr or C_PRIM
    s = ttk.Style(); s.theme_use("default")
    s.configure(f"{nome}.Treeview", background=C_F2, foreground=C_TEXTO,
                rowheight=22, fieldbackground=C_F2, font=("Segoe UI",9))
    s.configure(f"{nome}.Treeview.Heading", background=cor_hdr,
                foreground="white", font=("Segoe UI",9,"bold"), relief="flat")
    s.map(f"{nome}.Treeview",
          background=[("selected","#d6eaf8")], foreground=[("selected",C_PRIM)])

def _make_tree(parent, colunas, larguras, estilo):
    """Cria Treeview + scrollbars dentro de parent (usa grid internamente)."""
    _estilo_tree(estilo)
    tree = ttk.Treeview(parent, columns=colunas, show="headings",
                        style=f"{estilo}.Treeview")
    for c in colunas:
        tree.heading(c, text=c)
        tree.column(c, width=larguras.get(c, 90), anchor=tk.W, minwidth=30)
    tree.tag_configure("par",   background="#eaf2fb")
    tree.tag_configure("impar", background=C_F2)
    sb_y = ttk.Scrollbar(parent, orient="vertical",   command=tree.yview)
    sb_x = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
    parent.grid_rowconfigure(0, weight=1)
    parent.grid_columnconfigure(0, weight=1)
    tree.grid(row=0, column=0, sticky="nsew")
    sb_y.grid(row=0, column=1, sticky="ns")
    sb_x.grid(row=1, column=0, sticky="ew")
    return tree

# ─── Widgets: Gráficos (Canvas puro — sem matplotlib) ─────────────────────────

class BarChart(tk.Frame):
    """Gráfico de barras simples com Canvas tkinter."""

    def __init__(self, parent, dados, titulo, fmt=None, cor=C_PRIM, **kw):
        super().__init__(parent, bg=C_F2, **kw)
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)
        tk.Label(self, text=titulo, bg=C_F2, fg=C_TEXTO,
                 font=("Segoe UI",9,"bold")).grid(row=0, column=0, pady=(6,0))
        self._dados = [(str(k)[:14], _f(v)) for k, v in dados] if dados else []
        self._fmt   = fmt or (lambda v: f"{v:,.0f}")
        self._cor   = cor
        if not self._dados:
            tk.Label(self, text="Sem dados", bg=C_F2, fg=C_TEX2,
                     font=("Segoe UI",9)).grid(row=1, column=0)
            return
        self._cv = tk.Canvas(self, bg=C_F2, highlightthickness=0)
        self._cv.grid(row=1, column=0, sticky="nsew", padx=6, pady=4)
        self._cv.bind("<Configure>", lambda e: self._draw())

    def _draw(self):
        cv = self._cv; cv.delete("all")
        W = cv.winfo_width(); H = cv.winfo_height()
        if W < 30 or H < 30: return
        dados = self._dados[:12]
        n = len(dados)
        PL, PR, PT, PB = 52, 8, 10, 50
        wa = W - PL - PR; ha = H - PT - PB
        if wa <= 0 or ha <= 0: return
        bg2 = int(wa * 0.12 / n)
        bw  = max(6, (wa - bg2*(n+1)) // n)
        mx  = max(v for _, v in dados) or 1
        for i, (lbl, val) in enumerate(dados):
            x0 = PL + bg2 + i*(bw+bg2); x1 = x0+bw
            hb = int((val/mx)*ha)
            y0 = PT + ha - hb; y1 = PT + ha
            cor = PALETA[i % len(PALETA)]
            cv.create_rectangle(x0, y0, x1, y1, fill=cor, outline="")
            # valor
            cv.create_text((x0+x1)//2, y0-2, text=self._fmt(val),
                           font=("Segoe UI",7), fill=C_TEXTO, anchor="s")
            # rótulo girado (aproximado com ângulo)
            cv.create_text((x0+x1)//2, y1+3, text=lbl,
                           font=("Segoe UI",7), fill=C_TEX2, anchor="n", angle=35)
        # eixos
        cv.create_line(PL, PT, PL, PT+ha, fill=C_BORDA)
        cv.create_line(PL, PT+ha, W-PR, PT+ha, fill=C_BORDA)
        # marcas eixo Y
        for k in range(0, 5):
            y = PT + ha - int(k/4*ha)
            v = mx * k/4
            cv.create_line(PL-3, y, PL, y, fill=C_TEX2)
            cv.create_text(PL-5, y, text=self._fmt(v),
                           font=("Segoe UI",7), fill=C_TEX2, anchor="e")


class PieChart(tk.Frame):
    """Gráfico de pizza com legenda."""

    def __init__(self, parent, dados, titulo, **kw):
        super().__init__(parent, bg=C_F2, **kw)
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)
        tk.Label(self, text=titulo, bg=C_F2, fg=C_TEXTO,
                 font=("Segoe UI",9,"bold")).grid(row=0, column=0, pady=(6,0))
        self._dados = [(str(k), _f(v)) for k, v in dados] if dados else []
        if not self._dados:
            tk.Label(self, text="Sem dados", bg=C_F2, fg=C_TEX2,
                     font=("Segoe UI",9)).grid(row=1, column=0)
            return
        self._cv = tk.Canvas(self, bg=C_F2, highlightthickness=0)
        self._cv.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
        self._cv.bind("<Configure>", lambda e: self._draw())

    def _draw(self):
        cv = self._cv; cv.delete("all")
        W = cv.winfo_width(); H = cv.winfo_height()
        if W < 30 or H < 30: return
        total = sum(v for _, v in self._dados) or 1
        leg_h = 18 * len(self._dados)
        raio  = min(W//2 - 10, (H - leg_h - 20)//2)
        raio  = max(raio, 20)
        cx, cy = W//2, raio + 10
        ang = -90.0
        for i, (lbl, val) in enumerate(self._dados):
            ext = (val/total)*360
            cor = PALETA[i % len(PALETA)]
            cv.create_arc(cx-raio, cy-raio, cx+raio, cy+raio,
                          start=ang, extent=ext, fill=cor, outline="white", width=2)
            pct = val/total*100
            if pct > 5:
                mid = math.radians(ang + ext/2)
                lx = cx + raio*0.6*math.cos(mid)
                ly = cy + raio*0.6*math.sin(mid)
                cv.create_text(lx, ly, text=f"{pct:.0f}%",
                               font=("Segoe UI",8,"bold"), fill="white")
            ang += ext
        # Legenda embaixo
        y0 = cy + raio + 10
        for i, (lbl, val) in enumerate(self._dados):
            cor = PALETA[i % len(PALETA)]
            y = y0 + i*18
            cv.create_rectangle(6, y, 16, y+12, fill=cor, outline="")
            cv.create_text(22, y+6, text=f"{lbl}: {int(val)}",
                           font=("Segoe UI",8), fill=C_TEXTO, anchor="w")


# ═══════════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

class JanelaDashboard(tk.Toplevel):

    def __init__(self, master, df_nfse, df_nfe=None):
        super().__init__(master)
        self.title("GCON/SIAN — Dashboard Geral")
        self.geometry("1280x880"); self.resizable(True, True)
        self.configure(bg=C_FUNDO)

        # Topbar (pack)
        top = tk.Frame(self, bg=C_PRIM, height=48); top.pack(fill="x"); top.pack_propagate(False)
        tk.Label(top, text="  DASHBOARD  —  NF-e / NFS-e", bg=C_PRIM,
                 fg="white", font=("Segoe UI",13,"bold")).pack(side="left", padx=14, pady=12)
        tk.Label(top, text=datetime.now().strftime("Atualizado em %d/%m/%Y %H:%M"),
                 bg=C_PRIM, fg="#aed6f1", font=("Segoe UI",9)).pack(side="right", padx=14)
        tk.Frame(self, bg=C_ACENT, height=3).pack(fill="x")

        # Notebook de abas (pack)
        nb = ttk.Notebook(self); nb.pack(fill="both", expand=True, padx=6, pady=6)

        a1 = tk.Frame(nb, bg=C_FUNDO); nb.add(a1, text="  Visão Geral  ")
        a2 = tk.Frame(nb, bg=C_FUNDO); nb.add(a2, text="  NFS-e  ")
        a3 = tk.Frame(nb, bg=C_FUNDO); nb.add(a3, text="  Evolução Mensal  ")

        self._aba_geral(a1, df_nfse, df_nfe)
        self._aba_nfse(a2, df_nfse)
        self._aba_mensal(a3, df_nfse, df_nfe)

    # ── helpers internos ──────────────────────────────────────────────────────

    def _kpi(self, parent, row, col, titulo, valor, cor, sub=""):
        f = tk.Frame(parent, bg=C_F2, highlightbackground=C_BORDA, highlightthickness=1)
        f.grid(row=row, column=col, padx=4, pady=4, sticky="nsew", ipady=4)
        tk.Frame(f, bg=cor, height=5).pack(fill="x")
        tk.Label(f, text=titulo, bg=C_F2, fg=C_TEX2,
                 font=("Segoe UI",8,"bold")).pack(anchor="w", padx=10, pady=(5,0))
        tk.Label(f, text=valor, bg=C_F2, fg=C_TEXTO,
                 font=("Segoe UI",16,"bold")).pack(anchor="w", padx=10)
        if sub:
            tk.Label(f, text=sub, bg=C_F2, fg=C_TEX2,
                     font=("Segoe UI",8)).pack(anchor="w", padx=10, pady=(0,4))

    def _grafico_frame(self, parent, row, col, cspan=1, rspan=1):
        f = tk.Frame(parent, bg=C_F2, highlightbackground=C_BORDA, highlightthickness=1)
        f.grid(row=row, column=col, columnspan=cspan, rowspan=rspan,
               padx=4, pady=4, sticky="nsew")
        f.grid_rowconfigure(0, weight=1); f.grid_columnconfigure(0, weight=1)
        return f

    # ── Aba Visão Geral ───────────────────────────────────────────────────────

    def _aba_geral(self, parent, df_nfse, df_nfe):
        for c in range(3): parent.grid_columnconfigure(c, weight=1)
        for r in range(3): parent.grid_rowconfigure(r + 1, weight=1)

        # Métricas
        qtd_nfse   = len(df_nfse) if df_nfse is not None else 0
        qtd_nfe    = len(df_nfe)  if df_nfe  is not None else 0
        val_nfse   = df_nfse["Valor_Bruto"].apply(_f).sum() if df_nfse is not None else 0
        val_nfe    = df_nfe["vProd"].apply(_f).sum()        if df_nfe  is not None else 0
        total_iss  = df_nfse["Valor_ISS"].apply(_f).sum()   if df_nfse is not None else 0
        qtd_prest  = df_nfse["CNPJ_Prestador"].nunique()    if df_nfse is not None else 0

        self._kpi(parent, 0, 0, "TOTAL DOCUMENTOS",    str(qtd_nfse + qtd_nfe), C_PRIM,
                  f"NFS-e: {qtd_nfse}  |  NF-e itens: {qtd_nfe}")
        self._kpi(parent, 0, 1, "VALOR SERVIÇOS (NFS-e)", f"R$ {val_nfse:,.2f}", C_OK,
                  f"ISS total: R$ {total_iss:,.2f}")
        self._kpi(parent, 0, 2, "VALOR PRODUTOS (NF-e)", f"R$ {val_nfe:,.2f}", C_SEC,
                  f"Prestadores distintos: {qtd_prest}")

        # Pizza: NFS-e x NF-e quantidade
        dados_doc = []
        if qtd_nfse: dados_doc.append(("NFS-e serviços", qtd_nfse))
        if qtd_nfe:  dados_doc.append(("NF-e itens",     qtd_nfe))
        f1 = self._grafico_frame(parent, 1, 0)
        PieChart(f1, dados_doc, "Documentos por Tipo (Qtd)").grid(sticky="nsew")

        # Pizza: ISS retido x não
        if df_nfse is not None and len(df_nfse):
            mapa = {"1": "ISS Próprio", "2": "Retido Tomador", "": "Não informado"}
            cnt  = df_nfse["tpRetISSQN"].fillna("").apply(
                lambda x: mapa.get(str(x).strip(), "Outro")).value_counts()
            dados_ret = [(k, int(v)) for k, v in cnt.items()]
        else:
            dados_ret = []
        f2 = self._grafico_frame(parent, 1, 1)
        PieChart(f2, dados_ret, "ISS — Tipo Retenção (NFS-e)").grid(sticky="nsew")

        # Pizza: por formato NFS-e
        if df_nfse is not None and len(df_nfse):
            cnt2 = df_nfse["Formato"].value_counts()
            dados_fmt = [(k, int(v)) for k, v in cnt2.items()]
        else:
            dados_fmt = []
        f3 = self._grafico_frame(parent, 1, 2)
        PieChart(f3, dados_fmt, "NFS-e por Formato").grid(sticky="nsew")

        # Barras: top prestadores por QUANTIDADE
        if df_nfse is not None and len(df_nfse):
            top_q = (df_nfse.groupby("Nome_Prestador")
                     .size().sort_values(ascending=False).head(10))
            dados_q = [(str(k)[:18], int(v)) for k, v in top_q.items()]
        else:
            dados_q = []
        f4 = self._grafico_frame(parent, 2, 0, cspan=2)
        BarChart(f4, dados_q, "Top Prestadores — Quantidade de Notas",
                 fmt=lambda v: str(int(v)), cor=C_SEC).grid(sticky="nsew", padx=4, pady=4)

        # Barras: top prestadores por VALOR
        if df_nfse is not None and len(df_nfse):
            top_v = (df_nfse.groupby("Nome_Prestador")["Valor_Bruto"]
                     .apply(lambda x: x.apply(_f).sum())
                     .sort_values(ascending=False).head(10))
            dados_v = [(str(k)[:18], v) for k, v in top_v.items()]
        else:
            dados_v = []
        f5 = self._grafico_frame(parent, 2, 2)
        BarChart(f5, dados_v, "Top Prestadores — Valor",
                 fmt=lambda v: f"R${v/1000:.0f}k", cor=C_PRIM).grid(sticky="nsew", padx=4, pady=4)

    # ── Aba NFS-e ─────────────────────────────────────────────────────────────

    def _aba_nfse(self, parent, df):
        if df is None or df.empty:
            tk.Label(parent, text="Sem dados NFS-e", bg=C_FUNDO,
                     font=("Segoe UI",12)).pack(expand=True); return

        for c in range(2): parent.grid_columnconfigure(c, weight=1)
        for r in range(3): parent.grid_rowconfigure(r, weight=1)

        # Barra de totais
        bruto = df["Valor_Bruto"].apply(_f).sum()
        liq   = df["Valor_Liquido"].apply(_f).sum()
        iss   = df["Valor_ISS"].apply(_f).sum()
        pis   = df["Valor_PIS"].apply(_f).sum()
        cof   = df["Valor_COFINS"].apply(_f).sum()
        irrf  = df["Valor_IRRF"].apply(_f).sum()
        inss  = df["Valor_INSS"].apply(_f).sum()

        barra = tk.Frame(parent, bg=C_PRIM)
        barra.grid(row=0, column=0, columnspan=2, sticky="ew", padx=4, pady=(4,2))
        resumo = (f"  Notas: {len(df)}  |  Bruto: R$ {bruto:,.2f}  |  "
                  f"Líquido: R$ {liq:,.2f}  |  ISS: R$ {iss:,.2f}  |  "
                  f"PIS: R$ {pis:,.2f}  |  COFINS: R$ {cof:,.2f}  |  "
                  f"IRRF: R$ {irrf:,.2f}  |  INSS: R$ {inss:,.2f}")
        tk.Label(barra, text=resumo, bg=C_PRIM, fg="white",
                 font=("Segoe UI",8,"bold"), pady=5).pack(anchor="w")

        # Barras: ISS por prestador
        top_iss = (df.groupby("Nome_Prestador")["Valor_ISS"]
                   .apply(lambda x: x.apply(_f).sum())
                   .sort_values(ascending=False).head(8))
        dados_iss = [(str(k)[:16], v) for k, v in top_iss.items() if v > 0]
        f1 = self._grafico_frame(parent, 1, 0)
        BarChart(f1, dados_iss, "ISS por Prestador",
                 fmt=lambda v: f"R${v:.0f}", cor=C_ERR).grid(sticky="nsew", padx=4, pady=4)

        # Barras: notas por prestador (quantidade)
        top_qtd = df.groupby("Nome_Prestador").size().sort_values(ascending=False).head(8)
        dados_qtd = [(str(k)[:16], int(v)) for k, v in top_qtd.items()]
        f2 = self._grafico_frame(parent, 1, 1)
        BarChart(f2, dados_qtd, "Notas por Prestador (Qtd)",
                 fmt=lambda v: str(int(v)), cor=C_INFO).grid(sticky="nsew", padx=4, pady=4)

        # Barras: por código de serviço
        top_cod = (df.groupby("cTribNac")["Valor_Bruto"]
                   .apply(lambda x: x.apply(_f).sum())
                   .sort_values(ascending=False).head(8))
        dados_cod = [(str(k), v) for k, v in top_cod.items()]
        f3 = self._grafico_frame(parent, 2, 0)
        BarChart(f3, dados_cod, "Valor por Código de Serviço",
                 fmt=lambda v: f"R${v/1000:.0f}k", cor=C_ACENT).grid(sticky="nsew", padx=4, pady=4)

        # Pizza: UF dos prestadores
        uf_cnt = df["UF_Prestador"].fillna("").replace("","N/D").value_counts().head(6)
        dados_uf = [(k, int(v)) for k, v in uf_cnt.items()]
        f4 = self._grafico_frame(parent, 2, 1)
        PieChart(f4, dados_uf, "Prestadores por UF").grid(sticky="nsew")

    # ── Aba Mensal ────────────────────────────────────────────────────────────

    def _aba_mensal(self, parent, df_nfse, df_nfe):
        for c in range(2): parent.grid_columnconfigure(c, weight=1)
        for r in range(2): parent.grid_rowconfigure(r, weight=1)

        def mes_col(df, col_data, col_val):
            if df is None or df.empty: return []
            df2 = df.copy()
            df2["_m"] = df2[col_data].apply(lambda x: str(x)[:7]
                                            if x and str(x) not in ("nan","") else "????-??")
            g = (df2.groupby("_m")[col_val]
                 .apply(lambda x: x.apply(_f).sum()).sort_index())
            return [(k, v) for k, v in g.items()]

        def mes_qtd(df, col_data):
            if df is None or df.empty: return []
            df2 = df.copy()
            df2["_m"] = df2[col_data].apply(lambda x: str(x)[:7]
                                            if x and str(x) not in ("nan","") else "????-??")
            g = df2.groupby("_m").size().sort_index()
            return [(k, int(v)) for k, v in g.items()]

        f1 = self._grafico_frame(parent, 0, 0)
        BarChart(f1, mes_col(df_nfse, "Data_Emissao", "Valor_Bruto"),
                 "Valor NFS-e por Mês",
                 fmt=lambda v: f"R${v/1000:.0f}k", cor=C_OK).grid(sticky="nsew", padx=4, pady=4)

        f2 = self._grafico_frame(parent, 0, 1)
        BarChart(f2, mes_qtd(df_nfse, "Data_Emissao"),
                 "Qtd NFS-e por Mês",
                 fmt=lambda v: str(int(v)), cor=C_ACENT).grid(sticky="nsew", padx=4, pady=4)

        f3 = self._grafico_frame(parent, 1, 0)
        BarChart(f3, mes_col(df_nfse, "Data_Emissao", "Valor_ISS"),
                 "ISS por Mês",
                 fmt=lambda v: f"R${v:.0f}", cor=C_ERR).grid(sticky="nsew", padx=4, pady=4)

        # NF-e por mês (se tiver)
        dados_nfe_mes = mes_qtd(df_nfe, "Data_Emissao") if df_nfe is not None else []
        f4 = self._grafico_frame(parent, 1, 1)
        BarChart(f4, dados_nfe_mes,
                 "Qtd Itens NF-e por Mês",
                 fmt=lambda v: str(int(v)), cor=C_SEC).grid(sticky="nsew", padx=4, pady=4)


# ═══════════════════════════════════════════════════════════════════════════════
# APLICAÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

class AplicacaoLeitorXML:

    def __init__(self):
        ctk.set_appearance_mode("light")
        self.janela = ctk.CTk()
        self.janela.title("GCON/SIAN  —  NF-e / NFS-e")
        self.janela.geometry("1400x900"); self.janela.minsize(1200, 750)
        self.janela.configure(fg_color=C_FUNDO)
        self.janela.protocol("WM_DELETE_WINDOW", self._fechar)
        self.janela.grid_columnconfigure(1, weight=1)
        self.janela.grid_rowconfigure(0, weight=1)
        self.processando  = False
        self.cancelar     = False
        self.arquivos     = []
        self._win_dash    = None   # referência ao dashboard aberto

        locks = verificar_locks_ativos()
        if locks and not messagebox.askyesno("Sessões Ativas",
                                              f"{len(locks)} sessão(ões) ativa(s).\nContinuar mesmo assim?"):
            sys.exit(0)
        if not inicializar_sessao():
            messagebox.showerror("Erro", "Não foi possível inicializar sessão!"); return

        self._build_sidebar()
        self._build_area_principal()
        self._log_inicial()
        self._iniciar_watcher()

    # ─── Sidebar ──────────────────────────────────────────────────────────────

    def _build_sidebar(self):
        sb = tk.Frame(self.janela, bg=C_SIDE, width=300)
        sb.grid(row=0, column=0, sticky="nsew"); sb.grid_propagate(False)

        # Logo
        logo = tk.Frame(sb, bg=C_PRIM, height=90); logo.pack(fill="x"); logo.pack_propagate(False)
        tk.Label(logo, text="GCON / SIAN", bg=C_PRIM, fg="white",
                 font=("Segoe UI",17,"bold")).pack(pady=(18,2))
        tk.Label(logo, text="NF-e  |  NFS-e", bg=C_PRIM, fg="#aed6f1",
                 font=("Segoe UI",10)).pack()
        tk.Frame(sb, bg=C_ACENT, height=3).pack(fill="x")

        # Usuário
        usr = tk.Frame(sb, bg=C_SIDE, pady=12); usr.pack(fill="x", padx=16)
        tk.Label(usr, text="USUÁRIO ATIVO", bg=C_SIDE, fg="#7f8c8d",
                 font=("Segoe UI",8,"bold")).pack(anchor="w")
        tk.Label(usr, text=cfg.USUARIO_ID, bg=C_SIDE, fg="white",
                 font=("Segoe UI",12,"bold")).pack(anchor="w")
        tk.Label(usr, text=f"Sessão: {cfg.SESSAO_ID[:14]}…", bg=C_SIDE, fg="#7f8c8d",
                 font=("Segoe UI",9)).pack(anchor="w")
        tk.Frame(sb, bg=C_SIDE2, height=1).pack(fill="x", padx=16)

        def sbtn(texto, icone, cmd):
            f = tk.Frame(sb, bg=C_SIDE, cursor="hand2"); f.pack(fill="x")
            lbl = tk.Label(f, text=f"   {icone}   {texto}", bg=C_SIDE, fg="#bdc3c7",
                           font=("Segoe UI",11), anchor="w", padx=10, pady=11); lbl.pack(fill="x")
            for w in (lbl, f):
                w.bind("<Enter>",   lambda e, l=lbl, fr=f: [l.configure(bg=C_SIDE2, fg="white"),  fr.configure(bg=C_SIDE2)])
                w.bind("<Leave>",   lambda e, l=lbl, fr=f: [l.configure(bg=C_SIDE,  fg="#bdc3c7"), fr.configure(bg=C_SIDE)])
                w.bind("<Button-1>",lambda e, c=cmd: c())

        def sec(t):
            tk.Label(sb, text=f"  {t}", bg=C_SIDE, fg="#7f8c8d",
                     font=("Segoe UI",8,"bold")).pack(anchor="w", padx=16, pady=(12,2))

        sec("IMPORTAR")
        sbtn("Selecionar 1 XML",       "▶",  self._sel_um)
        sbtn("Selecionar Vários XMLs", "▶▶", self._sel_varios)
        tk.Frame(sb, bg=C_SIDE2, height=1).pack(fill="x", padx=16, pady=3)
        sec("VISUALIZAR")
        sbtn("Dashboard Geral",       "◉",  self._ver_dashboard)
        sbtn("Abrir Excel NF-e",      "⊞",  self._excel_nfe)
        sbtn("Abrir Excel NFS-e",     "⊡",  self._excel_nfse)
        tk.Frame(sb, bg=C_SIDE2, height=1).pack(fill="x", padx=16, pady=3)
        sec("SINCRONIZAR")
        sbtn("Sincronizar Tudo", "⟳", self._sincronizar)
        sbtn("Limpar Sessão",    "⌫", self._limpar_sessao)
        tk.Frame(sb, bg=C_SIDE2, height=1).pack(fill="x", padx=16, pady=3)
        sec("LOG")
        sbtn("Limpar Log", "✕", lambda: [self.txt_log.delete(1.0, tk.END), self.log("Log limpo.", "info")])
        sbtn("Salvar Log",  "↓", self._salvar_log)

        tk.Frame(sb, bg=C_SIDE).pack(fill="both", expand=True)
        rod = tk.Frame(sb, bg=C_ERR, cursor="hand2"); rod.pack(fill="x", side="bottom")
        lf  = tk.Label(rod, text="   ✕   FECHAR SISTEMA", bg=C_ERR, fg="white",
                        font=("Segoe UI",11,"bold"), pady=16); lf.pack(fill="x")
        for w in (rod, lf):
            w.bind("<Button-1>", lambda e: self._fechar())
            w.bind("<Enter>",  lambda e: [rod.configure(bg="#a93226"), lf.configure(bg="#a93226")])
            w.bind("<Leave>",  lambda e: [rod.configure(bg=C_ERR),    lf.configure(bg=C_ERR)])

    # ─── Área principal ────────────────────────────────────────────────────────

    def _build_area_principal(self):
        area = tk.Frame(self.janela, bg=C_FUNDO)
        area.grid(row=0, column=1, sticky="nsew")
        area.grid_columnconfigure(0, weight=1)
        area.grid_rowconfigure(2, weight=1)

        # Topbar
        top = tk.Frame(area, bg=C_PRIM, height=52)
        top.grid(row=0, column=0, sticky="ew"); top.grid_propagate(False)
        top.grid_columnconfigure(0, weight=1)
        tk.Label(top, text="  SISTEMA DE EXTRAÇÃO  —  NF-e / NFS-e  (MULTIUSUÁRIO)",
                 bg=C_PRIM, fg="white",
                 font=("Segoe UI",13,"bold")).grid(row=0, column=0, sticky="w", padx=16, pady=14)
        self._lbl_hora = tk.Label(top, text="", bg=C_PRIM, fg="#aed6f1", font=("Segoe UI",9))
        self._lbl_hora.grid(row=0, column=1, sticky="e", padx=16)
        tk.Frame(area, bg=C_ACENT, height=3).grid(row=0, column=0, sticky="sew")
        self._tick()

        # Cards
        cards = tk.Frame(area, bg=C_FUNDO)
        cards.grid(row=1, column=0, sticky="ew", padx=14, pady=10)
        for i in range(6): cards.grid_columnconfigure(i, weight=1)

        def card(col, titulo, valor, cor):
            f = tk.Frame(cards, bg=C_F2, highlightbackground=C_BORDA, highlightthickness=1)
            f.grid(row=0, column=col, padx=4, sticky="nsew", ipady=4)
            tk.Frame(f, bg=cor, height=4).pack(fill="x")
            tk.Label(f, text=titulo, bg=C_F2, fg=C_TEX2,
                     font=("Segoe UI",8,"bold")).pack(anchor="w", padx=10, pady=(5,0))
            lbl = tk.Label(f, text=valor, bg=C_F2, fg=C_TEXTO, font=("Segoe UI",15,"bold"))
            lbl.pack(anchor="w", padx=10, pady=(0,5))
            return lbl

        self._c_status = card(0, "STATUS",         "PRONTO",               C_OK)
        self._c_arqs   = card(1, "SELECIONADOS",   "0",                    C_SEC)
        self._c_proc   = card(2, "PROCESSADOS",    "0",                    C_INFO)
        self._c_nfe    = card(3, "NF-e PRODUTOS",  str(total_registros()), C_PRIM)
        self._c_nfse   = card(4, "NFS-e SERVIÇOS", "0",                    C_ACENT)
        self._c_prog   = card(5, "PROGRESSO",      "0%",                   C_WARN)

        # Progressbar
        pf = tk.Frame(area, bg=C_FUNDO)
        pf.grid(row=1, column=0, sticky="sew", padx=14, pady=(0,2))
        pf.grid_columnconfigure(0, weight=1)
        self._pv = tk.DoubleVar()
        ctk.CTkProgressBar(pf, variable=self._pv, height=7, corner_radius=4,
                           fg_color=C_BORDA, progress_color=C_SEC).grid(row=0, column=0, sticky="ew")
        self._pv.set(0)
        self._btn_parar = tk.Button(pf, text="⏹ Parar", font=("Segoe UI",8,"bold"),
                                    bg=C_ERR, fg="white", relief="flat", bd=0,
                                    cursor="hand2", padx=8, pady=2,
                                    command=self._parar_processamento)
        self._btn_parar.grid(row=0, column=1, padx=(8,0))
        self._btn_parar.grid_remove()  # oculto por padrão

        # Log
        lo = tk.Frame(area, bg=C_F2, highlightbackground=C_BORDA, highlightthickness=1)
        lo.grid(row=2, column=0, sticky="nsew", padx=14, pady=(0,12))
        lo.grid_columnconfigure(0, weight=1); lo.grid_rowconfigure(1, weight=1)

        lhdr = tk.Frame(lo, bg=C_PRIM, height=34); lhdr.grid(row=0, column=0, sticky="ew")
        tk.Label(lhdr, text="  LOG DE PROCESSAMENTO", bg=C_PRIM, fg="white",
                 font=("Segoe UI",9,"bold")).pack(side="left", padx=10, pady=7)
        self._badge_nfe  = tk.Label(lhdr, text="  NF-e: 0  ",  bg=C_PRIM,  fg="white", font=("Segoe UI",8,"bold"))
        self._badge_nfse = tk.Label(lhdr, text="  NFS-e: 0  ", bg=C_ACENT, fg="white", font=("Segoe UI",8,"bold"))
        self._badge_nfse.pack(side="right", padx=4, pady=6)
        self._badge_nfe.pack(side="right",  padx=4, pady=6)

        self.txt_log = scrolledtext.ScrolledText(lo, wrap=tk.WORD, font=FONTE_LOG,
                                                  bg="#1c2833", fg="#d5d8dc",
                                                  insertbackground="white", relief="flat")
        self.txt_log.grid(row=1, column=0, sticky="nsew", padx=1, pady=1)
        for tag, fg_cor, bold in [
            ("ok",   "#2ecc71", True),  ("err",  "#e74c3c", True),
            ("warn", "#f39c12", True),  ("info", "#5dade2", False),
            ("ts",   "#566573", False), ("brd",  "#2c3e50", False),
            ("nfe",  "#5dade2", True),  ("nfse", "#f5b942", True),
        ]:
            self.txt_log.tag_config(tag, foreground=fg_cor,
                font=(FONTE_LOG[0], FONTE_LOG[1], "bold" if bold else "normal"))

    def _tick(self):
        self._lbl_hora.configure(text=datetime.now().strftime("%d/%m/%Y  %H:%M:%S"))
        self.janela.after(1000, self._tick)

    # ─── Log ──────────────────────────────────────────────────────────────────

    def log(self, msg, tag="info"):
        self.txt_log.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] ", "ts")
        self.txt_log.insert(tk.END, f"{msg}\n", tag)
        self.txt_log.see(tk.END); self.janela.update_idletasks()

    def _div(self, c="─"):
        self.txt_log.insert(tk.END, f"{c*98}\n", "brd"); self.txt_log.see(tk.END)

    def _ctr(self, msg, tag="info"):
        p = max(0, (96 - len(msg)) // 2)
        self.txt_log.insert(tk.END, f"{' '*p}{msg}\n", tag); self.txt_log.see(tk.END)

    def _log_inicial(self):
        self.txt_log.delete(1.0, tk.END)
        self._div("="); self._ctr("GCON/SIAN  —  NF-e / NFS-e  —  MULTIUSUÁRIO")
        self._ctr(f"Sessão: {cfg.SESSAO_ID}  |  Usuário: {cfg.USUARIO_ID}")
        self._ctr(datetime.now().strftime("%d/%m/%Y  %H:%M:%S")); self._div("=")
        self.log("Sistema iniciado. Selecione XMLs para processar.", "ok")
        self.log(f"Pasta compartilhada : {cfg.PASTA_BASE}", "info")
        # Mostra contagem: temp (sessão atual) ou principal (sessões anteriores)
        nfe_n  = total_registros(cfg.CSV_TEMP)      or total_registros(cfg.CSV_PRINCIPAL)
        nfse_n = total_registros(cfg.CSV_NFSE_TEMP) or total_registros(cfg.CSV_NFSE_PRINCIPAL)
        orig_nfe  = "sessão" if total_registros(cfg.CSV_TEMP)      else "base"
        orig_nfse = "sessão" if total_registros(cfg.CSV_NFSE_TEMP) else "base"
        self.log(f"NF-e  [{orig_nfe}]  : {nfe_n} registros", "nfe")
        self.log(f"NFS-e [{orig_nfse}] : {nfse_n} registros", "nfse")
        if nfe_n:  self._c_nfe.configure(text=str(nfe_n))
        if nfse_n: self._c_nfse.configure(text=str(nfse_n))
        self.log("")

    # ─── Seleção e Pipeline ───────────────────────────────────────────────────

    def _iniciar_watcher(self):
        """Inicia o file watcher em background — recarrega módulos ao salvar."""
        try:
            from core.watcher import FileWatcher

            def _on_reload(nome, arq, hora, sucesso, erro):
                # Callback roda na thread do watcher — usa .after para tocar na UI
                import os
                nome_curto = os.path.basename(arq)
                if sucesso:
                    self.janela.after(0, lambda: self.log(
                        f"🔄 [{hora}] {nome_curto} recarregado ({nome})", "ok"))
                else:
                    self.janela.after(0, lambda: self.log(
                        f"⚠ [{hora}] Erro ao recarregar {nome_curto}: {erro}", "err"))

            self._watcher = FileWatcher(callback=_on_reload)
            self._watcher.start()
            self.log("👁 File watcher ativo — edite e salve qualquer .py para recarregar.", "info")
        except Exception as e:
            self.log(f"Watcher não iniciado: {e}", "warn")

    def _parar_processamento(self):
        if not self.processando: return
        if not messagebox.askyesno("Parar", "Parar imediatamente e descartar tudo da sessão atual?"):
            return
        self.cancelar = True
        if hasattr(self, "_proc") and self._proc and self._proc.is_alive():
            self._proc.terminate()
            self._proc.join(timeout=2)
        # Zera o temp
        from load.storage import _criar_csv_vazio
        _criar_csv_vazio(cfg.CSV_TEMP,      cfg.CABECALHO_CSV)
        _criar_csv_vazio(cfg.CSV_NFSE_TEMP, cfg.CABECALHO_NFSE)
        self.processando = False; self.arquivos = []; self.cancelar = False
        self._c_nfe.configure(text="0"); self._c_nfse.configure(text="0")
        self._c_status.configure(text="CANCELADO", fg=C_ERR)
        self._btn_parar.configure(text="⏹ Parar", state="normal")
        self.janela.after(0, self._btn_parar.grid_remove)
        self._pv.set(0); self._c_prog.configure(text="0%"); self._c_arqs.configure(text="0")
        self.log("⏹ Processo encerrado. Sessão zerada — pronto para nova importação.", "warn")

    def _processar(self):
        self.processando = True
        self.cancelar    = False
        self.janela.after(0, self._btn_parar.grid)
        self._c_status.configure(text="PROCESSANDO...", fg=C_WARN)
        total = len(self.arquivos)

        self.log(""); self._div("="); self._ctr("INÍCIO DO PROCESSAMENTO"); self._div("=")
        self.log(f"Total de arquivos   : {total}", "info")
        self.log(f"CSV NF-e  temp      : {os.path.basename(cfg.CSV_TEMP)}", "info")
        self.log(f"CSV NFS-e temp      : {os.path.basename(cfg.CSV_NFSE_TEMP)}", "info")
        self.log("")

        chaves_nfe  = carregar_chaves_existentes(cfg.CSV_TEMP)
        chaves_nfse = carregar_chaves_nfse()

        fila = mp.Queue()
        self._proc = mp.Process(
            target=_worker_processar,
            args=(self.arquivos, cfg.CSV_TEMP, cfg.CSV_NFSE_TEMP,
                  cfg.CABECALHO_CSV, cfg.CABECALHO_NFSE,
                  chaves_nfe, chaves_nfse, fila),
            daemon=True
        )
        self._proc.start()

        cnt_nfe = cnt_nfse = add_nfe = add_nfse = err_nfe = err_nfse = 0

        while True:
            if self.cancelar:
                break
            try:
                evento = fila.get(timeout=0.1)
            except Exception:
                # fila vazia — verifica se processo terminou
                if not self._proc.is_alive():
                    break
                continue

            tipo = evento[0]
            if tipo == "log":
                _, nivel, msg = evento
                self.log(msg, nivel)
            elif tipo == "progresso":
                _, i, tot, cnfe, cnfse = evento
                cnt_nfe = cnfe; cnt_nfse = cnfse
                p = i / tot
                self._pv.set(p); self._c_prog.configure(text=f"{p*100:.0f}%")
                self._c_proc.configure(text=str(i))
                self._badge_nfe.configure(text=f"  NF-e: {cnt_nfe}  ")
                self._badge_nfse.configure(text=f"  NFS-e: {cnt_nfse}  ")
                self.janela.update_idletasks()
            elif tipo == "fim":
                _, cnt_nfe, cnt_nfse, add_nfe, add_nfse, err_nfe, err_nfse = evento
                break

        if self.cancelar:
            return  # _parar_processamento já fez a limpeza

        self._proc.join()

        # Excel da sessão
        resultado = salvar_excel_sessao()
        for chave, (ok, msg) in resultado.items():
            self.log(f"{chave:12} : {msg}", "ok" if ok else "warn")

        # Atualiza cards
        nfe_total  = total_registros(cfg.CSV_TEMP)
        nfse_total = total_registros(cfg.CSV_NFSE_TEMP)
        self._c_nfe.configure(text=str(nfe_total))
        self._c_nfse.configure(text=str(nfse_total))

        # Resumo
        self.log(""); self._div("="); self._ctr("RESUMO FINAL"); self._div("=")
        self.log(f"Arquivos: {total}  (NF-e: {cnt_nfe}  NFS-e: {cnt_nfse})", "info")
        if err_nfe or err_nfse:
            self.log(f"Erros: NF-e={err_nfe}  NFS-e={err_nfse}", "err")
        self.log(f"NF-e adicionados  : {add_nfe} produtos  (total sessão: {nfe_total})", "nfe")
        self.log(f"NFS-e adicionadas : {add_nfse} notas    (total sessão: {nfse_total})", "nfse")
        self._div("=")

        self._c_status.configure(text="CONCLUÍDO", fg=C_OK)
        messagebox.showinfo("Concluído",
                            f"NF-e : {cnt_nfe} arqs  →  {add_nfe} produtos novos\n"
                            f"NFS-e: {cnt_nfse} arqs  →  {add_nfse} notas novas\n"
                            + (f"Erros: {err_nfe+err_nfse}" if err_nfe+err_nfse else "Sem erros!"))
        if self._win_dash and self._win_dash.winfo_exists():
            self.log("Atualizando Dashboard…", "info"); self._ver_dashboard()

        self.processando = False; self.arquivos = []; self.cancelar = False
        self.janela.after(0, self._btn_parar.grid_remove)
        self._btn_parar.configure(text="⏹ Parar", state="normal")
        self._c_arqs.configure(text="0"); self._pv.set(0); self._c_prog.configure(text="0%")
        """Se já houver dados na sessão, pergunta se quer substituir ou adicionar.
        Retorna True para continuar, False para cancelar."""
        nfe  = total_registros(cfg.CSV_TEMP)
        nfse = total_registros(cfg.CSV_NFSE_TEMP)
        if nfe == 0 and nfse == 0:
            return True  # sessão vazia — pode importar direto
        resp = messagebox.askyesnocancel(
            "Sessão com dados",
            f"A sessão atual já tem dados:\n\n"
            f"  NF-e : {nfe} registro(s)\n"
            f"  NFS-e: {nfse} registro(s)\n\n"
            f"Sim    → Substituir (zera a sessão e importa os novos)\n"
            f"Não    → Adicionar (mantém os existentes e acrescenta)\n"
            f"Cancelar → Voltar"
        )
        if resp is None:
            return False  # Cancelar
        if resp:  # Sim → zera o temp antes de importar
            from load.storage import _criar_csv_vazio
            _criar_csv_vazio(cfg.CSV_TEMP,      cfg.CABECALHO_CSV)
            _criar_csv_vazio(cfg.CSV_NFSE_TEMP, cfg.CABECALHO_NFSE)
            self._c_nfe.configure(text="0")
            self._c_nfse.configure(text="0")
            self.log("Sessão substituída — dados anteriores removidos.", "warn")
        return True  # Não → adiciona normalmente

    def _confirmar_sessao(self):
        """Se já houver dados na sessão, pergunta se quer substituir ou adicionar.
        Retorna True para continuar, False para cancelar."""
        nfe  = total_registros(cfg.CSV_TEMP)
        nfse = total_registros(cfg.CSV_NFSE_TEMP)
        if nfe == 0 and nfse == 0:
            return True  # sessão vazia — pode importar direto
        resp = messagebox.askyesnocancel(
            "Sessão com dados",
            f"A sessão atual já tem dados:\n\n"
            f"  NF-e : {nfe} registro(s)\n"
            f"  NFS-e: {nfse} registro(s)\n\n"
            f"Sim    → Substituir (zera a sessão e importa os novos)\n"
            f"Não    → Adicionar (mantém os existentes e acrescenta)\n"
            f"Cancelar → Voltar"
        )
        if resp is None:
            return False
        if resp:
            from load.storage import _criar_csv_vazio
            _criar_csv_vazio(cfg.CSV_TEMP,      cfg.CABECALHO_CSV)
            _criar_csv_vazio(cfg.CSV_NFSE_TEMP, cfg.CABECALHO_NFSE)
            self._c_nfe.configure(text="0")
            self._c_nfse.configure(text="0")
            self.log("Sessão substituída — dados anteriores removidos.", "warn")
        return True

    def _sel_um(self):
        if self.processando: messagebox.showwarning("Aguarde", "Processamento em andamento!"); return
        arq = filedialog.askopenfilename(title="Selecione um XML",
                                          filetypes=[("XML","*.xml"),("Todos","*.*")])
        if arq:
            if not self._confirmar_sessao(): return
            self.arquivos = [arq]; self._c_arqs.configure(text="1")
            threading.Thread(target=self._processar, daemon=True).start()

    def _sel_varios(self):
        if self.processando: messagebox.showwarning("Aguarde", "Processamento em andamento!"); return
        arqs = filedialog.askopenfilenames(title="Selecione XMLs",
                                            filetypes=[("XML","*.xml"),("Todos","*.*")])
        if arqs:
            if not self._confirmar_sessao(): return
            self.arquivos = list(arqs); self._c_arqs.configure(text=str(len(arqs)))
            threading.Thread(target=self._processar, daemon=True).start()

    # ─── Visualizações ────────────────────────────────────────────────────────


    def _csv_nfse(self):
        """Retorna o melhor CSV NFS-e disponível: temp (se tiver dados) > principal."""
        for caminho in (cfg.CSV_NFSE_TEMP, cfg.CSV_NFSE_PRINCIPAL):
            if not os.path.exists(caminho) or os.path.getsize(caminho) < 50:
                continue
            # Verificar se tem ao menos 2 linhas (cabeçalho + 1 dado)
            try:
                with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                    linhas = sum(1 for _ in f)
                if linhas >= 2:
                    return caminho
            except Exception:
                continue
        return None



    def _csv_nfse(self):
        """Retorna o melhor CSV NFS-e disponível: temp > principal."""
        for caminho in (cfg.CSV_NFSE_TEMP, cfg.CSV_NFSE_PRINCIPAL):
            if not os.path.exists(caminho) or os.path.getsize(caminho) < 50:
                continue
            try:
                with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                    if sum(1 for _ in f) >= 2:
                        return caminho
            except Exception:
                continue
        return None

    def _csv_nfe(self):
        """Retorna o melhor CSV NF-e disponível: temp > principal."""
        for caminho in (cfg.CSV_TEMP, cfg.CSV_PRINCIPAL):
            if not os.path.exists(caminho) or os.path.getsize(caminho) < 50:
                continue
            try:
                with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                    if sum(1 for _ in f) >= 2:
                        return caminho
            except Exception:
                continue
        return None

    def _ver_dashboard(self):
        if self._win_dash and self._win_dash.winfo_exists():
            self._win_dash.destroy()
        csv_nfse = self._csv_nfse()
        csv_nfe  = self._csv_nfe()
        if csv_nfse is None and csv_nfe is None:
            messagebox.showwarning("Aviso", "Sem dados para exibir.\nImporte XMLs primeiro."); return
        df_nfse = _ler_csv(csv_nfse, CABECALHO_NFSE) if csv_nfse else None
        df_nfe  = _ler_csv(csv_nfe,  CABECALHO_CSV)  if csv_nfe  else None
        self._win_dash = JanelaDashboard(self.janela, df_nfse, df_nfe)

    def _excel_nfe(self):
        csv = self._csv_nfe()
        if csv is None:
            messagebox.showwarning("Aviso", "Sem dados NF-e. Importe XMLs primeiro."); return
        # Gera Excel a partir do melhor CSV disponível
        from load.storage import _csv_para_df, _df_para_excel
        df = _csv_para_df(csv, CABECALHO_CSV)
        _df_para_excel(df, cfg.EXCEL_TEMP, "Produtos_NFe", "GCON/SIAN — NF-e — Produtos e Impostos")
        self.log(f"Excel NF-e gerado: {len(df)} registros", "ok")
        os.startfile(cfg.EXCEL_TEMP)

    def _excel_nfse(self):
        csv = self._csv_nfse()
        if csv is None:
            messagebox.showwarning("Aviso", "Sem dados NFS-e. Importe XMLs primeiro."); return
        from load.storage import _csv_para_df, _df_para_excel
        df = _csv_para_df(csv, CABECALHO_NFSE)
        _df_para_excel(df, cfg.EXCEL_NFSE_TEMP, "Servicos_NFSe", "GCON/SIAN — NFS-e — Notas de Serviço")
        self.log(f"Excel NFS-e gerado: {len(df)} registros", "ok")
        os.startfile(cfg.EXCEL_NFSE_TEMP)

    def _sincronizar(self):
        if self.processando: messagebox.showwarning("Aguarde", "Processamento em andamento!"); return
        ok1, m1 = sincronizar_com_principal()
        ok2, m2 = sincronizar_nfse_com_principal()
        self.log(f"Sinc NF-e  : {m1}", "ok" if ok1 else "err")
        self.log(f"Sinc NFS-e : {m2}", "ok" if ok2 else "err")
        if ok1: ok3, m3 = atualizar_excel_principal();      self.log(f"Excel NF-e : {m3}", "ok" if ok3 else "warn")
        if ok2: ok4, m4 = atualizar_excel_nfse_principal(); self.log(f"Excel NFS-e: {m4}", "ok" if ok4 else "warn")
        messagebox.showinfo("Sincronização", f"NF-e : {m1}\nNFS-e: {m2}")

    def _limpar_sessao(self):
        if self.processando:
            messagebox.showwarning("Aguarde", "Processamento em andamento!"); return
        nfe  = total_registros(cfg.CSV_TEMP)
        nfse = total_registros(cfg.CSV_NFSE_TEMP)
        if nfe == 0 and nfse == 0:
            messagebox.showinfo("Limpar Sessão", "A sessão já está vazia."); return
        resp = messagebox.askyesno(
            "Limpar Sessão",
            f"Isso apagará os dados da sessão atual:\n\n"
            f"  NF-e : {nfe} registro(s)\n"
            f"  NFS-e: {nfse} registro(s)\n\n"
            f"O histórico salvo (CSV/Excel principal) não será alterado.\n\n"
            f"Deseja continuar?"
        )
        if not resp: return
        from load.storage import _criar_csv_vazio
        _criar_csv_vazio(cfg.CSV_TEMP,      cfg.CABECALHO_CSV)
        _criar_csv_vazio(cfg.CSV_NFSE_TEMP, cfg.CABECALHO_NFSE)
        self._c_nfe.configure(text="0")
        self._c_nfse.configure(text="0")
        self.log("Sessão limpa. NF-e e NFS-e zerados.", "warn")
        # Fecha janelas abertas pois os dados foram zerados
        if self._win_dash and self._win_dash.winfo_exists(): self._win_dash.destroy()

    def _fechar(self):
        if self.processando:
            if not messagebox.askyesno("Atenção", "Processamento em andamento.\nFechar mesmo assim?"): return
        tem_dados = total_registros(cfg.CSV_TEMP) > 0 or total_registros(cfg.CSV_NFSE_TEMP) > 0
        if tem_dados:
            resp = messagebox.askyesnocancel(
                "Fechar",
                "Você tem dados importados nesta sessão.\n\n"
                "Deseja sincronizar (salvar no histórico) antes de fechar?\n\n"
                "Sim → Sincroniza e fecha\n"
                "Não → Fecha sem salvar no histórico\n"
                "Cancelar → Volta ao sistema"
            )
            if resp is None: return          # Cancelar
            if resp:                         # Sim → sincroniza
                self.log("Sincronizando antes de fechar…", "info")
                sincronizar_com_principal(); sincronizar_nfse_com_principal()
                atualizar_excel_principal(); atualizar_excel_nfse_principal()
        self._watcher_ativo = False
        if hasattr(self, "_watcher"): self._watcher.stop()
        limpar_temporarios()
        self.janela.destroy(); sys.exit(0)

    def _salvar_log(self):
        arq = filedialog.asksaveasfilename(
            defaultextension=".txt", filetypes=[("Texto","*.txt")],
            initialfile=f"log_{cfg.USUARIO_ID}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        if arq:
            with open(arq, "w", encoding="utf-8") as f: f.write(self.txt_log.get(1.0, tk.END))
            self.log(f"Log salvo: {arq}", "ok")

    def _iniciar_watcher(self):
        """Monitora arquivos .py do projeto e recarrega módulos ao detectar mudanças."""
        import extract, transform, load, config.settings as _cfg_mod

        # Módulos recarregáveis e seus arquivos correspondentes
        self._modulos_watch = {
            os.path.abspath(mod.__file__): mod
            for mod in [
                extract.xml_reader   if hasattr(extract, 'xml_reader')   else None,
                extract.nfse_reader  if hasattr(extract, 'nfse_reader')  else None,
                transform.validator  if hasattr(transform, 'validator')  else None,
                load.storage         if hasattr(load, 'storage')         else None,
                _cfg_mod,
            ]
            if mod is not None and hasattr(mod, '__file__') and mod.__file__
        }

        # Importar submódulos explicitamente para garantir referência
        import extract.xml_reader, extract.nfse_reader, transform.validator, load.storage
        self._modulos_watch = {
            os.path.abspath(m.__file__): m
            for m in [
                extract.xml_reader, extract.nfse_reader,
                transform.validator, load.storage, _cfg_mod,
            ]
        }

        # Snapshot inicial dos mtimes
        self._watch_mtimes = {
            arq: os.path.getmtime(arq)
            for arq in self._modulos_watch
            if os.path.exists(arq)
        }

        # Também monitora main_window.py (não recarrega, mas avisa)
        ui_arquivo = os.path.abspath(__file__)
        self._watch_mtimes[ui_arquivo] = os.path.getmtime(ui_arquivo) if os.path.exists(ui_arquivo) else 0

        self._watcher_ativo = True
        threading.Thread(target=self._loop_watcher, daemon=True).start()
        self.log("👁 File watcher ativo — edite e salve qualquer .py para recarregar.", "info")

    def _loop_watcher(self):
        while self._watcher_ativo:
            threading.Event().wait(1.0)  # checa a cada 1 segundo
            for arq, mtime_anterior in list(self._watch_mtimes.items()):
                try:
                    mtime_atual = os.path.getmtime(arq)
                except OSError:
                    continue
                if mtime_atual == mtime_anterior:
                    continue

                self._watch_mtimes[arq] = mtime_atual
                nome = os.path.basename(arq)

                # main_window.py — não recarrega, só avisa
                if "main_window" in arq:
                    self.janela.after(0, lambda n=nome: self.log(
                        f"⚠ {n} alterado — reinicie o sistema para aplicar.", "warn"))
                    continue

                # Demais módulos — recarrega
                mod = self._modulos_watch.get(arq)
                if mod is None:
                    continue
                try:
                    importlib.reload(mod)
                    # Recarregar também os __init__ que reexportam
                    import extract, transform, load
                    for pkg in (extract, transform, load):
                        try: importlib.reload(pkg)
                        except Exception: pass
                    self.janela.after(0, lambda n=nome: self.log(
                        f"🔄 {n} recarregado com sucesso.", "ok"))
                except Exception as e:
                    self.janela.after(0, lambda n=nome, err=str(e): self.log(
                        f"❌ Erro ao recarregar {n}: {err}", "err"))

    def run(self):
        self.janela.mainloop()
