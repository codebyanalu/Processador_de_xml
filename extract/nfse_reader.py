"""
extract/nfse_reader.py
Suporta:
  - NFSe Nacional (SPED/Fazenda) — infNFSe + DPS
  - CompNFe (municipal legado)
Corrigido: prestador lido de infNFSe/emit, tomador de DPS/toma.
"""

import os, re
import xml.etree.ElementTree as ET

def _t(el, tag, default=""):
    if el is None: return default
    e = el.find(tag)
    return e.text.strip() if e is not None and e.text else default

def _remover_ns(c):
    c = re.sub(r'xmlns(:[^=]+)?="[^"]+"', "", c)
    c = re.sub(r"<\?xml[^?>]*\?>", "", c)
    return c

def _parsear(caminho):
    with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
        c = f.read()
    s = _remover_ns(c)
    try:
        return ET.fromstring(s)
    except ET.ParseError:
        s = re.sub(r"<\?xml-stylesheet[^?>]*\?>", "", s)
        return ET.fromstring(s)


# ── NFSe Nacional ─────────────────────────────────────────────────────────────

def _extrair_nfse_nacional(root, arquivo):
    inf    = root.find(".//infNFSe")
    if inf is None:
        return None, "ERRO: infNFSe nao encontrado"

    infdps = root.find(".//infDPS")
    emit   = inf.find(".//emit")            # prestador fica em infNFSe/emit
    prest  = infdps.find(".//prest") if infdps is not None else None
    toma   = infdps.find(".//toma")  if infdps is not None else None
    serv   = infdps.find(".//serv")  if infdps is not None else None
    cserv  = serv.find(".//cServ")   if serv is not None else None
    vals_nfse = inf.find(".//valores")
    vals_dps  = infdps.find(".//valores") if infdps is not None else None
    trib      = vals_dps.find(".//trib")  if vals_dps is not None else None
    if trib is None and infdps is not None:
        trib = infdps.find(".//trib")
    trib_mun  = trib.find(".//tribMun")  if trib is not None else None
    trib_fed  = trib.find(".//tribFed")  if trib is not None else None
    reg_trib  = prest.find(".//regTrib") if prest is not None else None
    ender_emit = emit.find(".//enderNac") if emit is not None else None
    ender_toma = toma.find(".//end")       if toma is not None else None

    # Prestador: combina infNFSe/emit com DPS/prest
    cnpj_prest = _t(emit,"CNPJ") or _t(emit,"CPF") or _t(prest,"CNPJ") or _t(prest,"CPF")
    nome_prest = _t(emit,"xNome")
    fant_prest = _t(emit,"xFant")
    im_prest   = _t(emit,"IM")
    uf_prest   = _t(ender_emit,"UF")
    email_prest = _t(emit,"email") or _t(prest,"email")

    # Tomador
    cnpj_toma  = _t(toma,"CNPJ") or _t(toma,"CPF")
    nome_toma  = _t(toma,"xNome")
    im_toma    = _t(toma,"IM")
    email_toma = _t(toma,"email")
    mun_toma   = _t(inf,"xLocPrestacao")
    uf_toma    = ""
    if ender_toma is not None:
        en = ender_toma.find(".//endNac")
        uf_toma = _t(en,"UF") if en is not None else ""

    # Valores
    vbc      = _t(vals_nfse,"vBC")
    p_aliq   = _t(vals_nfse,"pAliqAplic")
    v_issqn  = _t(vals_nfse,"vISSQN")
    v_liq    = _t(vals_nfse,"vLiq")
    # Valor bruto = vServ dentro do DPS
    v_serv   = ""
    vserv_el = vals_dps.find(".//vServPrest") if vals_dps is not None else None
    if vserv_el is not None:
        v_serv = _t(vserv_el,"vServ")
    if not v_serv:
        v_serv = vbc or v_liq

    # trib federal DPS
    v_pis   = _t(trib,"vPis")
    v_cof   = _t(trib,"vCofins")
    v_irrf  = _t(trib,"vRetIRRF")
    v_csll  = _t(trib,"vRetCSLL")
    v_inss  = _t(trib,"vRetINSS")
    p_sn    = _t(trib,"pTotTribSN")

    simples = _t(reg_trib,"opSimpNac") if reg_trib is not None else ""

    # IBS/CBS — em infNFSe/IBSCBS (Nacional)
    ibs_vbc = ibs_uf = ibs_mun = cbs_p = ""
    ibscbs_inf = inf.find(".//IBSCBS")
    if ibscbs_inf is None:
        ibscbs_inf = root.find(".//IBSCBS")   # fallback raiz
    if ibscbs_inf is not None:
        vals_ib = ibscbs_inf.find(".//valores")
        if vals_ib is not None:
            ibs_vbc = _t(vals_ib,"vBC")
            uf_el   = ibscbs_inf.find(".//uf")
            mun_el  = ibscbs_inf.find(".//mun")
            fed_el  = ibscbs_inf.find(".//fed")
            ibs_uf  = _t(uf_el, "pIBSUF")  if uf_el  is not None else ""
            ibs_mun = _t(mun_el,"pIBSMun") if mun_el is not None else ""
            cbs_p   = _t(fed_el,"pCBS")    if fed_el is not None else ""

    d = {
        "Formato":              "NFSe Nacional",
        "Chave_NFSe":           inf.get("Id","").replace("NFS",""),
        "Numero_NFSe":          _t(inf,"nNFSe"),
        "Serie_RPS":            _t(infdps,"serie") if infdps is not None else "",
        "Data_Emissao":         _t(infdps,"dhEmi") if infdps is not None else _t(inf,"dhProc"),
        "Data_Competencia":     _t(infdps,"dCompet") if infdps is not None else "",
        "Municipio_Prestacao":  _t(inf,"xLocPrestacao"),
        "cTribNac":             _t(cserv,"cTribNac"),
        "xDescServ":            _t(cserv,"xDescServ")[:200],
        "cNBS_DPS":             _t(cserv,"cNBS"),
        "Cod_Servico_Mun":      "",
        "Desc_Servico":         _t(inf,"xTribNac"),
        "Cod_Item_Lei116":      "",
        "Cod_NBS":              _t(inf,"xNBS"),
        # ISS
        "ISS_Retido":           _t(trib_mun,"tpRetISSQN") if trib_mun is not None else "",
        "BC_ISS":               vbc,
        "Aliq_ISS":             p_aliq,
        "Valor_ISS":            v_issqn,
        "pAliq_ISS":            _t(trib_mun,"pAliq") if trib_mun is not None else p_aliq,
        "tpRetISSQN":           _t(trib_mun,"tpRetISSQN") if trib_mun is not None else "",
        # CSRF
        "BC_CSRF":              v_serv,
        "Valor_PIS":            v_pis,
        "Valor_COFINS":         v_cof,
        "Valor_CSLL":           v_csll,
        "BC_IRRF":              "",
        "Valor_IRRF":           v_irrf,
        "BC_INSS":              "",
        "Valor_INSS":           v_inss,
        "pTotTribSN":           p_sn,
        # IBS/CBS
        "IBS_vBC":              ibs_vbc,
        "IBS_pIBSUF":           ibs_uf,
        "IBS_pIBSMun":          ibs_mun,
        "CBS_pCBS":             cbs_p,
        # Valores
        "Valor_Bruto":          v_serv,
        "Valor_Liquido":        v_liq,
        "Discriminacao":        _t(cserv,"xDescServ")[:200],
        # Prestador
        "CNPJ_Prestador":       cnpj_prest,
        "IM_Prestador":         im_prest,
        "Nome_Prestador":       nome_prest,
        "NomeFantasia_Prestador": fant_prest,
        "UF_Prestador":         uf_prest,
        "Mun_Prestador":        _t(inf,"xLocEmi"),
        "Email_Prestador":      email_prest,
        "Simples_Nacional":     simples,
        # Tomador
        "CNPJ_Tomador":         cnpj_toma,
        "IM_Tomador":           im_toma,
        "Nome_Tomador":         nome_toma,
        "Mun_Tomador":          mun_toma,
        "UF_Tomador":           uf_toma,
        "Email_Tomador":        email_toma,
        "Arquivo_Origem":       os.path.basename(arquivo),
    }

    nome_log = nome_prest or cnpj_prest
    return d, f"NFS-e Nacional: {nome_log[:40]}"


# ── CompNFe (municipal legado) ────────────────────────────────────────────────

def _extrair_compnfe(root, arquivo):
    nfe = root.find(".//NFe")
    if nfe is None:
        return None, "ERRO: Tag NFe nao encontrada em CompNFe"

    prest   = nfe.find(".//Prestador")
    tomad   = nfe.find(".//Tomador")
    ibscbs  = nfe.find(".//IBSCBS")
    vals_ib = ibscbs.find(".//valores") if ibscbs is not None else None

    # IBSCBS detalhado
    ibs_vbc = ibs_uf = ibs_mun = cbs_p = ""
    if vals_ib is not None:
        ibs_vbc = _t(vals_ib,"vBC")
        uf_el   = ibscbs.find(".//uf")
        mun_el  = ibscbs.find(".//mun")
        fed_el  = ibscbs.find(".//fed")
        ibs_uf  = _t(uf_el, "pIBSUF")  if uf_el  is not None else ""
        ibs_mun = _t(mun_el,"pIBSMun") if mun_el is not None else ""
        cbs_p   = _t(fed_el,"pCBS")    if fed_el is not None else ""

    # DPS embutido (Onfly, Vogel)
    dps     = nfe.find(".//DPS")
    infdps  = dps.find(".//infDPS") if dps is not None else None
    cserv   = infdps.find(".//cServ") if infdps is not None else None
    trib    = infdps.find(".//trib")  if infdps is not None else None
    trib_mun = trib.find(".//tribMun") if trib is not None else None

    v_bruto = nfe.findtext(".//ValorNFe","") or ""
    v_liq   = nfe.findtext(".//ValorLiquidoNFe","") or v_bruto

    d = {
        "Tipo_Nota":            "NFS-e",
        "Formato":              "CompNFe",
        "Chave_NFSe":           _t(nfe,"CodigoVerificador"),
        "Numero_NFSe":          _t(nfe,"NumeroNFe"),
        "Serie_RPS":            _t(nfe,"SerieRPS"),
        "Data_Emissao":         _t(nfe,"DataEmissaoNFe"),
        "Data_Competencia":     _t(nfe,"DataCompetenciaNFe"),
        "Municipio_Prestacao":  _t(nfe,"MunicipioPrestacao"),
        "cTribNac":             _t(cserv,"cTribNac") if cserv is not None else "",
        "xDescServ":            (_t(cserv,"xDescServ") if cserv is not None else "")[:200],
        "cNBS_DPS":             _t(cserv,"cNBS") if cserv is not None else "",
        "Cod_Servico_Mun":      _t(nfe,"CodigoServicoMunicipal"),
        "Desc_Servico":         _t(nfe,"DescricaoServicoMunicipal"),
        "Cod_Item_Lei116":      _t(nfe,"CodigoItemLei116"),
        "Cod_NBS":              _t(nfe,"CodigoNBS"),
        # ISS
        "ISS_Retido":           _t(nfe,"ISSRetido"),
        "BC_ISS":               _t(nfe,"BaseCalculoISS"),
        "Aliq_ISS":             _t(nfe,"AliquotaIss"),
        "Valor_ISS":            _t(nfe,"ValorISS"),
        "pAliq_ISS":            _t(trib_mun,"pAliq") if trib_mun is not None else "",
        "tpRetISSQN":           _t(trib_mun,"tpRetISSQN") if trib_mun is not None else "",
        # CSRF
        "BC_CSRF":              _t(nfe,"BaseCalculoCSRF"),
        "Valor_PIS":            _t(nfe,"ValorPIS"),
        "Valor_COFINS":         _t(nfe,"ValorCOFINS"),
        "Valor_CSLL":           _t(nfe,"ValorCSLL"),
        "BC_IRRF":              _t(nfe,"BaseCalculoIRRF"),
        "Valor_IRRF":           _t(nfe,"ValorIRRF"),
        "BC_INSS":              _t(nfe,"BaseCalculoINSS"),
        "Valor_INSS":           _t(nfe,"ValorINSS"),
        "pTotTribSN":           "",
        # IBS/CBS
        "IBS_vBC":              ibs_vbc,
        "IBS_pIBSUF":           ibs_uf,
        "IBS_pIBSMun":          ibs_mun,
        "CBS_pCBS":             cbs_p,
        # Valores
        "Valor_Bruto":          v_bruto,
        "Valor_Liquido":        v_liq,
        "Discriminacao":        _t(nfe,"Discriminacao")[:200].replace("\n"," "),
        # Prestador
        "CNPJ_Prestador":       _t(prest,"CnpjCpf") if prest is not None else "",
        "IM_Prestador":         "",
        "Nome_Prestador":       _t(prest,"RazaoSocialNome") if prest is not None else "",
        "NomeFantasia_Prestador": "",
        "UF_Prestador":         "",
        "Mun_Prestador":        _t(nfe,"MunicipioPrestacao"),
        "Email_Prestador":      "",
        "Simples_Nacional":     _t(nfe,"PrestadorOptanteSimplesNacional"),
        # Tomador
        "CNPJ_Tomador":         _t(tomad,"CnpjCpf") if tomad is not None else "",
        "IM_Tomador":           _t(tomad,"InscricaoMunicipal") if tomad is not None else "",
        "Nome_Tomador":         _t(tomad,"RazaoSocialNome") if tomad is not None else "",
        "Mun_Tomador":          _t(tomad,"Municipio") if tomad is not None else "",
        "UF_Tomador":           _t(tomad,"UfSigla") if tomad is not None else "",
        "Email_Tomador":        "",
        "Arquivo_Origem":       os.path.basename(arquivo),
    }

    nome_log = d["Nome_Prestador"] or d["CNPJ_Prestador"]
    return d, f"NFS-e CompNFe: {nome_log[:40]}"


# ── Função pública ─────────────────────────────────────────────────────────────

def extrair_servicos(caminho_xml):
    try:
        root = _parsear(caminho_xml)
        tag  = root.tag.split("}")[-1] if "}" in root.tag else root.tag

        if tag == "CompNFe" or root.find(".//CompNFe") is not None:
            dados, msg = _extrair_compnfe(root, caminho_xml)
        elif tag in ("NFSe","nfseProc") or root.find(".//infNFSe") is not None:
            dados, msg = _extrair_nfse_nacional(root, caminho_xml)
        else:
            return [], f"ERRO: Formato NFS-e nao reconhecido (tag: {tag})"

        if dados is None:
            return [], msg
        return [dados], msg

    except ET.ParseError as e:
        return [], f"ERRO XML: {str(e)[:80]}"
    except Exception as e:
        return [], f"ERRO: {str(e)[:80]}"
