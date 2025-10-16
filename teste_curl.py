import requests

cnpj = "02558157000162"  # só números
url = f"https://open.cnpja.com/office/{cnpj}"
resp = requests.get(url, timeout=30, verify=False)
resp.raise_for_status()
data = resp.json()

# o campo pode variar por provedor/versão; tente estas chaves comuns:
capital = (
    data.get("company", {}).get("capital", {}).get("total") or
    data.get("company", {}).get("capital_social") or
    data.get("capital_social")
)

print("Capital social:", capital)
