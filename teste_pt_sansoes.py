# -*- coding: utf-8 -*-
"""
cnpj_consulta_all.py
Consulta CNPJ em múltiplas fontes de uma só vez (exceto BrasilAPI e Portal da Transparência).
- Minha Receita (grátis)
- ReceitaWS (grátis, com rate limit)
- CNPJ.ws (paga/opcional) -> passar --cnpjws-token
- SERPRO Consulta CNPJ (paga/opcional) -> passar --serpro-token

Saídas:
- Resumo comparativo no terminal
- Arquivo JSON consolidado (--out-json)
- Arquivo CSV consolidado (--out-csv)

Uso rápido:
  python cnpj_consulta_all.py --cnpj 02558157000162 --out-json saida.json --out-csv saida.csv [--insecure]
"""

import argparse
import csv
import json
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

# -----------------------------
# Utilitários
# -----------------------------

def somente_digitos(txt: str) -> str:
    return re.sub(r"\D+", "", txt or "")

def desliga_warnings_ssl():
    try:
        requests.packages.urllib3.disable_warnings()  # type: ignore
    except Exception:
        pass

def http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 12, verify_ssl: bool = True) -> Dict[str, Any]:
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout, verify=verify_ssl)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "application/json" in ct:
            data = r.json()
        else:
            try:
                data = r.json()
            except Exception:
                data = {"raw": r.text}
        return {"ok": r.ok, "status": r.status_code, "data": data, "url": url}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "status": None, "error": str(e), "url": url}

# -----------------------------
# Fontes
# -----------------------------

def fetch_minha_receita(cnpj: str, timeout=12, verify_ssl=True) -> Dict[str, Any]:
    # Doc: https://minhareceita.org/
    url = f"https://minhareceita.org/{cnpj}"
    return {"fonte": "minha_receita", **http_get(url, timeout=timeout, verify_ssl=verify_ssl)}

def fetch_receitaws(cnpj: str, timeout=12, verify_ssl=True) -> Dict[str, Any]:
    # Doc: https://receitaws.com.br/api
    url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj}"
    return {"fonte": "receitaws", **http_get(url, timeout=timeout, verify_ssl=verify_ssl)}

def fetch_cnpjws(cnpj: str, token: str, timeout=12, verify_ssl=True) -> Dict[str, Any]:
    # Doc: https://docs.cnpj.ws/
    url = f"https://api.cnpj.ws/cnpj/{cnpj}"
    headers = {"x_api_token": token}
    return {"fonte": "cnpj_ws", **http_get(url, headers=headers, timeout=timeout, verify_ssl=verify_ssl)}

def fetch_serpro(cnpj: str, bearer_token: str, timeout=12, verify_ssl=True) -> Dict[str, Any]:
    # Doc (varia por contratação): https://apicenter.estaleiro.serpro.gov.br/
    base = "https://apicenter-api.serpro.gov.br/consulta-cnpj/v1"
    url = f"{base}/cnpj/{cnpj}"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return {"fonte": "serpro_consulta_cnpj", **http_get(url, headers=headers, timeout=timeout, verify_ssl=verify_ssl)}

# -----------------------------
# Normalização para um schema único
# -----------------------------
# Schema alvo:
# {
#   cnpj, razao_social, nome_fantasia, abertura, natureza_juridica,
#   situacao, situacao_data, capital_social,
#   cnae_principal: { codigo, descricao },
#   cnaes_secundarios: [ { codigo, descricao }, ... ],
#   endereco: { logradouro, numero, complemento, bairro, municipio, uf, cep },
#   telefones: [str], email, qsa: [ { nome, qualificacao } ]
# }

def norm_from_minhareceita(cnpj: str, d: Dict[str, Any]) -> Dict[str, Any]:
    # Campos comuns na Minha Receita
    cnae_princ_cod = d.get("cnae_fiscal")
    cnae_princ_desc = d.get("cnae_fiscal_descricao")
    cnaes_sec = []
    for item in d.get("cnaes_secundarios") or []:
        cnaes_sec.append({
            "codigo": item.get("codigo"),
            "descricao": item.get("descricao")
        })

    # Telefones
    tels = []
    for k in ("ddd_telefone_1", "ddd_telefone_2"):
        v = (d.get(k) or "").strip()
        if v:
            tels.append(v)

    # QSA
    qsa = []
    for s in d.get("qsa") or []:
        qsa.append({
            "nome": s.get("nome_socio") or s.get("nome") or "",
            "qualificacao": s.get("qualificacao_socio") or s.get("qualificacao") or ""
        })

    return {
        "fonte": "minha_receita",
        "cnpj": cnpj,
        "razao_social": d.get("razao_social") or d.get("nome_empresarial"),
        "nome_fantasia": d.get("nome_fantasia"),
        "abertura": d.get("data_inicio_atividade"),
        "natureza_juridica": d.get("natureza_juridica"),
        "situacao": str(d.get("situacao_cadastral")) if d.get("situacao_cadastral") is not None else d.get("descricao_situacao_cadastral"),
        "situacao_data": d.get("data_situacao_cadastral"),
        "capital_social": d.get("capital_social"),
        "cnae_principal": {"codigo": cnae_princ_cod, "descricao": cnae_princ_desc},
        "cnaes_secundarios": cnaes_sec,
        "endereco": {
            "logradouro": d.get("logradouro"),
            "numero": d.get("numero"),
            "complemento": d.get("complemento"),
            "bairro": d.get("bairro"),
            "municipio": d.get("municipio"),
            "uf": d.get("uf"),
            "cep": d.get("cep"),
        },
        "telefones": tels,
        "email": d.get("email"),
        "qsa": qsa
    }

def norm_from_receitaws(cnpj: str, d: Dict[str, Any]) -> Dict[str, Any]:
    # ReceitaWS estrutura
    atv_p = d.get("atividade_principal") or []
    cnae_p_cod, cnae_p_desc = (None, None)
    if atv_p:
        cnae_p_cod = atv_p[0].get("code")
        cnae_p_desc = atv_p[0].get("text")
    cnaes_sec = []
    for item in d.get("atividades_secundarias") or []:
        cnaes_sec.append({
            "codigo": item.get("code"),
            "descricao": item.get("text")
        })

    # QSA
    qsa = []
    for s in d.get("qsa") or []:
        qsa.append({
            "nome": s.get("nome") or "",
            "qualificacao": s.get("qual") or s.get("qualificacao") or ""
        })

    # Telefones
    tels = []
    tel = (d.get("telefone") or "").strip()
    if tel:
        tels.append(tel)

    return {
        "fonte": "receitaws",
        "cnpj": cnpj,
        "razao_social": d.get("nome"),
        "nome_fantasia": d.get("fantasia"),
        "abertura": d.get("abertura"),
        "natureza_juridica": d.get("natureza_juridica"),
        "situacao": d.get("situacao"),
        "situacao_data": d.get("data_situacao") or d.get("data_situacao_especial"),
        "capital_social": d.get("capital_social"),
        "cnae_principal": {"codigo": cnae_p_cod, "descricao": cnae_p_desc},
        "cnaes_secundarios": cnaes_sec,
        "endereco": {
            "logradouro": d.get("logradouro"),
            "numero": d.get("numero"),
            "complemento": d.get("complemento"),
            "bairro": d.get("bairro"),
            "municipio": d.get("municipio"),
            "uf": d.get("uf"),
            "cep": d.get("cep"),
        },
        "telefones": tels,
        "email": d.get("email"),
        "qsa": qsa
    }

def norm_from_cnpjws(cnpj: str, d: Dict[str, Any]) -> Dict[str, Any]:
    # Alguns campos mudam conforme o plano/versão
    cnae_p = d.get("cnae_fiscal") or {}
    cnaes_sec = []
    for item in d.get("cnaes_secundarias") or d.get("cnaes_secundarios") or []:
        cnaes_sec.append({
            "codigo": item.get("codigo") or item.get("code"),
            "descricao": item.get("descricao") or item.get("text"),
        })

    qsa = []
    for s in d.get("socios") or d.get("qsa") or []:
        qsa.append({
            "nome": s.get("nome") or s.get("nome_socio") or "",
            "qualificacao": s.get("qualificacao") or s.get("qualificacao_socio") or ""
        })

    tels = []
    for k in ("telefone", "telefones"):
        v = d.get(k)
        if isinstance(v, list):
            for t in v:
                if t:
                    tels.append(str(t))
        elif isinstance(v, str) and v.strip():
            tels.append(v.strip())

    end = d.get("endereco") or {}
    return {
        "fonte": "cnpj_ws",
        "cnpj": cnpj,
        "razao_social": d.get("razao_social") or d.get("nome_empresarial") or d.get("razaoSocial"),
        "nome_fantasia": d.get("nome_fantasia") or d.get("nomeFantasia"),
        "abertura": d.get("data_inicio_atividade") or d.get("abertura"),
        "natureza_juridica": d.get("natureza_juridica") or d.get("naturezaJuridica"),
        "situacao": d.get("situacao_cadastral") or d.get("situacao"),
        "situacao_data": d.get("data_situacao_cadastral") or d.get("data_situacao"),
        "capital_social": d.get("capital_social"),
        "cnae_principal": {
            "codigo": cnae_p.get("codigo") or cnae_p.get("code"),
            "descricao": cnae_p.get("descricao") or cnae_p.get("text"),
        },
        "cnaes_secundarios": cnaes_sec,
        "endereco": {
            "logradouro": end.get("logradouro") or d.get("logradouro"),
            "numero": end.get("numero") or d.get("numero"),
            "complemento": end.get("complemento") or d.get("complemento"),
            "bairro": end.get("bairro") or d.get("bairro"),
            "municipio": end.get("municipio") or d.get("municipio"),
            "uf": end.get("uf") or d.get("uf"),
            "cep": end.get("cep") or d.get("cep"),
        },
        "telefones": tels,
        "email": d.get("email"),
        "qsa": qsa
    }

def norm_from_serpro(cnpj: str, d: Dict[str, Any]) -> Dict[str, Any]:
    # A resposta pode vir com chaves diferentes por produto/versão
    empresa = d.get("estabelecimento") or d
    cnae_p = empresa.get("cnae") or d.get("cnae_principal") or {}
    cnaes_sec = []
    for item in empresa.get("cnaes_secundarias") or d.get("cnaes_secundarios") or []:
        cnaes_sec.append({
            "codigo": item.get("codigo") or item.get("code"),
            "descricao": item.get("descricao") or item.get("text"),
        })

    end = empresa.get("endereco") or {}
    qsa = []
    for s in d.get("socios") or d.get("qsa") or []:
        qsa.append({
            "nome": s.get("nome") or s.get("nome_socio") or "",
            "qualificacao": s.get("qualificacao") or s.get("qualificacao_socio") or ""
        })

    return {
        "fonte": "serpro_consulta_cnpj",
        "cnpj": cnpj,
        "razao_social": d.get("razao_social") or d.get("nome_empresarial") or d.get("razaoSocial"),
        "nome_fantasia": empresa.get("nome_fantasia") or d.get("nome_fantasia"),
        "abertura": d.get("data_inicio_atividade") or d.get("abertura"),
        "natureza_juridica": d.get("natureza_juridica"),
        "situacao": d.get("situacao_cadastral") or d.get("situacao"),
        "situacao_data": d.get("data_situacao_cadastral") or d.get("data_situacao"),
        "capital_social": d.get("capital_social"),
        "cnae_principal": {
            "codigo": (cnae_p.get("codigo") if isinstance(cnae_p, dict) else None),
            "descricao": (cnae_p.get("descricao") if isinstance(cnae_p, dict) else None),
        },
        "cnaes_secundarios": cnaes_sec,
        "endereco": {
            "logradouro": end.get("logradouro"),
            "numero": end.get("numero"),
            "complemento": end.get("complemento"),
            "bairro": end.get("bairro"),
            "municipio": end.get("municipio"),
            "uf": end.get("uf"),
            "cep": end.get("cep"),
        },
        "telefones": empresa.get("telefones") or [],
        "email": d.get("email"),
        "qsa": qsa
    }

# -----------------------------
# Consolidação e I/O
# -----------------------------

def consolidate_results(cnpj: str, raw_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Retorna um dicionário com: entradas normalizadas por fonte + visão unificada simples."""
    normalized: List[Dict[str, Any]] = []
    for rr in raw_results:
        fonte = rr.get("fonte")
        data = (rr.get("data") or {}) if rr.get("ok") else {}
        try:
            if fonte == "minha_receita" and data:
                normalized.append(norm_from_minhareceita(cnpj, data))
            elif fonte == "receitaws" and data:
                normalized.append(norm_from_receitaws(cnpj, data))
            elif fonte == "cnpj_ws" and data:
                normalized.append(norm_from_cnpjws(cnpj, data))
            elif fonte == "serpro_consulta_cnpj" and data:
                normalized.append(norm_from_serpro(cnpj, data))
        except Exception as e:
            normalized.append({"fonte": fonte, "erro_normalizacao": str(e)})

    # visão unificada simples (pega o primeiro valor válido em ordem de confiança)
    ordem = ["serpro_consulta_cnpj", "cnpj_ws", "minha_receita", "receitaws"]
    unificado: Dict[str, Any] = {
        "cnpj": cnpj,
        "razao_social": None,
        "nome_fantasia": None,
        "abertura": None,
        "natureza_juridica": None,
        "situacao": None,
        "situacao_data": None,
        "capital_social": None,
        "cnae_principal": {"codigo": None, "descricao": None},
        "cnaes_secundarios": [],
        "endereco": {"logradouro": None, "numero": None, "complemento": None, "bairro": None, "municipio": None, "uf": None, "cep": None},
        "telefones": [],
        "email": None,
        "qsa": []
    }

    def pick(key, src):
        nonlocal unificado
        if unificado.get(key) in (None, "", [], {}):
            unificado[key] = src

    # helper para adicionar listas sem duplicar
    def extend_unique(lst: List[Any], items: List[Any]) -> None:
        s = set(json.dumps(x, ensure_ascii=False, sort_keys=True) for x in lst)
        for it in items or []:
            js = json.dumps(it, ensure_ascii=False, sort_keys=True)
            if js not in s:
                lst.append(it); s.add(js)

    # aplica ordem de confiança
    for fonte in ordem:
        cand = next((n for n in normalized if n.get("fonte") == fonte), None)
        if not cand:
            continue
        for k in ["razao_social","nome_fantasia","abertura","natureza_juridica","situacao","situacao_data","capital_social","email"]:
            v = cand.get(k)
            if v not in (None, "", []):
                pick(k, v)

        # cnae principal
        cp = cand.get("cnae_principal") or {}
        if unificado["cnae_principal"]["codigo"] in (None, "") and cp.get("codigo"):
            unificado["cnae_principal"] = {"codigo": cp.get("codigo"), "descricao": cp.get("descricao")}

        # cnaes secundários
        extend_unique(unificado["cnaes_secundarios"], cand.get("cnaes_secundarios") or [])

        # endereço
        end = cand.get("endereco") or {}
        ue = unificado["endereco"]
        for ek in ue.keys():
            if ue[ek] in (None, "") and end.get(ek):
                ue[ek] = end.get(ek)

        # telefones
        extend_unique(unificado["telefones"], [{"numero": t} if isinstance(t, str) else t for t in (cand.get("telefones") or [])])

        # qsa
        extend_unique(unificado["qsa"], cand.get("qsa") or [])

    return {"unificado": unificado, "normalizados": normalized, "raw": raw_results}

def save_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_csv(path: str, unificado: Dict[str, Any]) -> None:
    # CSV de 1 linha (resumo unificado). Campos de lista serão " ; " separados.
    row = {
        "cnpj": unificado.get("cnpj"),
        "razao_social": unificado.get("razao_social"),
        "nome_fantasia": unificado.get("nome_fantasia"),
        "abertura": unificado.get("abertura"),
        "natureza_juridica": unificado.get("natureza_juridica"),
        "situacao": unificado.get("situacao"),
        "situacao_data": unificado.get("situacao_data"),
        "capital_social": unificado.get("capital_social"),
        "cnae_principal_codigo": (unificado.get("cnae_principal") or {}).get("codigo"),
        "cnae_principal_descricao": (unificado.get("cnae_principal") or {}).get("descricao"),
        "cnaes_secundarios_codigos": " ; ".join([str(x.get("codigo")) for x in (unificado.get("cnaes_secundarios") or []) if x.get("codigo")]),
        "cnaes_secundarios_descricoes": " ; ".join([str(x.get("descricao")) for x in (unificado.get("cnaes_secundarios") or []) if x.get("descricao")]),
        "logradouro": (unificado.get("endereco") or {}).get("logradouro"),
        "numero": (unificado.get("endereco") or {}).get("numero"),
        "complemento": (unificado.get("endereco") or {}).get("complemento"),
        "bairro": (unificado.get("endereco") or {}).get("bairro"),
        "municipio": (unificado.get("endereco") or {}).get("municipio"),
        "uf": (unificado.get("endereco") or {}).get("uf"),
        "cep": (unificado.get("endereco") or {}).get("cep"),
        "telefones": " ; ".join([t.get("numero") if isinstance(t, dict) else str(t) for t in (unificado.get("telefones") or [])]),
        "email": unificado.get("email"),
        "qsa_nomes": " ; ".join([m.get("nome","") for m in (unificado.get("qsa") or []) if m.get("nome")]),
        "qsa_qualificacoes": " ; ".join([m.get("qualificacao","") for m in (unificado.get("qsa") or []) if m.get("qualificacao")]),
    }
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        w.writeheader()
        w.writerow(row)

def print_resume(consolidado: Dict[str, Any]) -> None:
    uni = consolidado["unificado"]
    print("\n=== RESUMO UNIFICADO ===")
    print(f"CNPJ:              {uni.get('cnpj')}")
    print(f"Razão Social:      {uni.get('razao_social')}")
    print(f"Nome Fantasia:     {uni.get('nome_fantasia')}")
    print(f"Abertura:          {uni.get('abertura')}")
    print(f"Natureza Jurídica: {uni.get('natureza_juridica')}")
    print(f"Situação:          {uni.get('situacao')}  (data: {uni.get('situacao_data')})")
    cp = uni.get("cnae_principal") or {}
    print(f"CNAE Principal:    {cp.get('codigo')} - {cp.get('descricao')}")
    end = uni.get("endereco") or {}
    print(f"Endereço:          {end.get('logradouro')}, {end.get('numero')} - {end.get('bairro')} - {end.get('municipio')}/{end.get('uf')} - CEP {end.get('cep')}")
    print(f"Email:             {uni.get('email')}")
    tels = ", ".join([t.get("numero") if isinstance(t, dict) else str(t) for t in (uni.get("telefones") or [])])
    print(f"Telefones:         {tels}")
    print(f"QSA (só nomes):    {', '.join([m.get('nome','') for m in (uni.get('qsa') or []) if m.get('nome')])}")

    print("\n=== FONTES CONSULTADAS ===")
    for n in consolidado["normalizados"]:
        fonte = n.get("fonte")
        rs = n.get("razao_social") or n.get("nome_fantasia")
        st = n.get("situacao")
        print(f"- {fonte}: razão='{rs}', situação='{st}'")

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True, help="CNPJ com ou sem máscara")
    ap.add_argument("--timeout", type=int, default=12)
    ap.add_argument("--insecure", action="store_true", help="Desabilita verificação SSL (uso em teste local)")
    ap.add_argument("--cnpjws-token", help="Token do CNPJ.ws (opcional)")
    ap.add_argument("--serpro-token", help="Bearer token do SERPRO (opcional)")
    ap.add_argument("--out-json", help="Arquivo JSON de saída (consolidado)")
    ap.add_argument("--out-csv", help="Arquivo CSV de saída (linha única unificada)")
    args = ap.parse_args()

    cnpj = somente_digitos(args.cnpj)
    if len(cnpj) != 14:
        print("CNPJ inválido (precisa ter 14 dígitos).", file=sys.stderr)
        sys.exit(2)

    verify_ssl = not args.insecure
    if not verify_ssl:
        desliga_warnings_ssl()

    # Chama TODAS as fontes de uma vez
    resultados_raw: List[Dict[str, Any]] = []
    resultados_raw.append(fetch_minha_receita(cnpj, timeout=args.timeout, verify_ssl=verify_ssl))
    resultados_raw.append(fetch_receitaws(cnpj, timeout=args.timeout, verify_ssl=verify_ssl))
    if args.cnpjws_token:
        resultados_raw.append(fetch_cnpjws(cnpj, token=args.cnpjws_token, timeout=args.timeout, verify_ssl=verify_ssl))
    if args.serpro_token:
        resultados_raw.append(fetch_serpro(cnpj, bearer_token=args.serpro_token, timeout=args.timeout, verify_ssl=verify_ssl))

    # Mostra status HTTP de cada uma
    print("=== Status das consultas ===")
    for r in resultados_raw:
        fonte = r.get("fonte")
        st = r.get("status")
        ok = r.get("ok")
        err = r.get("error")
        print(f"{fonte:22} -> ok={ok} status={st} url={r.get('url')}{(' | erro='+err) if err else ''}")

    # Consolida e imprime resumo
    consolidado = consolidate_results(cnpj, resultados_raw)
    print_resume(consolidado)

    # Grava saídas
    if args.out_json:
        save_json(args.out_json, consolidado)
        print(f"\n✔ JSON salvo em: {args.out_json}")
    if args.out_csv:
        save_csv(args.out_csv, consolidado["unificado"])
        print(f"✔ CSV salvo em:  {args.out_csv}")

if __name__ == "__main__":
    main()
