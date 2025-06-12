import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin
from openpyxl.utils.exceptions import IllegalCharacterError
from postal.parser import parse_address
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARCHIVO_ENTRADA = "Webs para extraer todavía.xlsx"
ARCHIVO_SALIDA = "detalles_contacto_web.xlsx"
ARCHIVO_BACKUP = "detalles_contacto_web_backup.xlsx"
BACKUP_CADA = 200

proxy = {
    "http": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823",
    "https": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823"
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

SUBPAGINAS = [
    'contacto', 'contáctanos', 'contacta', 'contactar', 'contacte', 'kontaktua', 'contact-us', 'about',
    'sobre nosotros', 'quienes somos', 'empresa', 'aviso legal', 'privacy-policy', 'data-protection'
]

df = pd.read_excel(ARCHIVO_ENTRADA)
if "Website" not in df.columns:
    raise ValueError("No se encontró la columna 'Website' en el archivo de entrada.")
urls = df["Website"].dropna().unique().tolist()

try:
    resultados = pd.read_excel(ARCHIVO_SALIDA)
    procesadas = set(resultados["Website"].dropna().tolist())
except FileNotFoundError:
    columnas = [
        "Website", "Email", "Teléfono", "Direccion_candidata", "Direccion_postal",
        "road", "house_number", "city", "postcode", "state", "country", "suburb",
        "unit", "level", "staircase", "entrance", "po_box", "category", "near", "Con_Proxy"
    ]
    resultados = pd.DataFrame(columns=columnas)
    procesadas = set()

def extraer_emails(texto):
    return list(set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", texto)))

def extraer_telefonos(texto):
    return list(set(re.findall(r"\+?\d[\d\s().-]{8,}", texto)))

def encontrar_subpaginas(soup, base_url):
    enlaces = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        for sub in SUBPAGINAS:
            if sub in href:
                full_url = urljoin(base_url, a['href'])
                enlaces.add(full_url)
    return list(enlaces)[:3]

def extraer_candidatos_direccion(soup):
    candidatos = set()
    for tag in soup.find_all(['p', 'li', 'span', 'address', 'div']):
        texto = tag.get_text(separator=' ', strip=True)
        for linea in re.split(r'[\n.;|•]', texto):
            l = linea.strip()
            if len(l) < 8:
                continue
            if any(palabra in l.lower() for palabra in [
                "calle", "avda", "avenida", "plaza", "paseo", "carrer", "via", "road", "street",
                "ronda", "camino", "edificio", "bloque", "local", "oficina", "polígono"
            ]) or (re.search(r'\d{1,4}', l) and re.search(r'[a-zA-Z]{4,}', l)):
                candidatos.add(l)
    return list(candidatos)

def extraer_direccion_postal(soup):
    candidatos = extraer_candidatos_direccion(soup)
    for cand in candidatos:
        parsed = parse_address(cand)
        labels = dict(parsed)
        if parsed and any(label in labels for label in ["road", "city", "postcode"]):
            return cand, labels
    return "", {}

def procesar_url(url):
    try:
        res = None
        con_proxy = "Sí"
        try:
            res = requests.get(url, proxies=proxy, timeout=15, verify=False, headers=headers)
        except Exception:
            con_proxy = "No"
            res = requests.get(url, timeout=15, verify=False, headers=headers)
        if res is None or res.status_code != 200:
            return None
        soup = BeautifulSoup(res.text, "html.parser")
        texto = soup.get_text(separator='\n', strip=True)
        email = "; ".join(extraer_emails(texto))
        telefono = "; ".join(extraer_telefonos(texto))
        direccion_candidata, componentes = extraer_direccion_postal(soup)
        direccion_postal = ", ".join([v for v in componentes.values()]) if componentes else ""
        return {
            "Email": email,
            "Teléfono": telefono,
            "Direccion_candidata": direccion_candidata,
            "Direccion_postal": direccion_postal,
            "road": componentes.get("road", ""),
            "house_number": componentes.get("house_number", ""),
            "city": componentes.get("city", ""),
            "postcode": componentes.get("postcode", ""),
            "state": componentes.get("state", ""),
            "country": componentes.get("country", ""),
            "suburb": componentes.get("suburb", ""),
            "unit": componentes.get("unit", ""),
            "level": componentes.get("level", ""),
            "staircase": componentes.get("staircase", ""),
            "entrance": componentes.get("entrance", ""),
            "po_box": componentes.get("po_box", ""),
            "category": componentes.get("category", ""),
            "near": componentes.get("near", ""),
            "Con_Proxy": con_proxy
        }
    except Exception as e:
        print(f"❌ Error al procesar {url}: {e}")
        return None

for i, url in enumerate(urls, 1):
    if url in procesadas:
        continue
    print(f"Procesando {url} ({i}/{len(urls)})...")

    resultados_url = []
    datos = procesar_url(url)
    if datos:
        datos['Website'] = url
        resultados_url.append(datos)

    # Buscar y procesar subpáginas relevantes
    try:
        res = requests.get(url, proxies=proxy, timeout=15, verify=False, headers=headers)
        soup = BeautifulSoup(res.text, "html.parser")
        subpaginas = encontrar_subpaginas(soup, url)
        for sub_url in subpaginas:
            print(f"  Procesando subpágina: {sub_url}")
            datos_sub = procesar_url(sub_url)
            if datos_sub:
                datos_sub['Website'] = sub_url
                resultados_url.append(datos_sub)
    except Exception as e:
        print(f"❌ Error al buscar subpáginas en {url}: {e}")

    # Guarda todos los resultados (principal + subpáginas)
    for fila in resultados_url:
        nueva_fila = pd.DataFrame([fila])
        resultados = pd.concat([resultados, nueva_fila], ignore_index=True)
        try:
            resultados.to_excel(ARCHIVO_SALIDA, index=False)
            resultados.to_excel(ARCHIVO_BACKUP, index=False)
        except IllegalCharacterError:
            print("⚠️ Error de carácter ilegal al guardar fila. Se omite esta fila.")

    if (len(resultados) % BACKUP_CADA == 0):
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        resultados.to_excel(f"backup_cada_{BACKUP_CADA}_{ts}.xlsx", index=False)

resultados.to_excel(ARCHIVO_SALIDA, index=False)
resultados.to_excel(ARCHIVO_BACKUP, index=False)
print("✅ Proceso completado.")
