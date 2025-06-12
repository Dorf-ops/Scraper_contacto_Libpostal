import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from urllib.parse import urljoin
from openpyxl.utils.exceptions import IllegalCharacterError

# === CONFIGURACIÓN ===

ARCHIVO_ENTRADA = "Webs para extraer todavía.xlsx"
ARCHIVO_SALIDA = "detalles_contacto_web.xlsx"
BACKUP_CADA = 200  # cada cuántos registros guardar backup con timestamp

proxy = {
    "http": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823",
    "https": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823"
}

SUBPAGINAS = [
    'contacto', 'contáctanos', 'contacta', 'contactar', 'contacte', 'kontaktua', 'contact-us', 'about',
    'sobre nosotros', 'quienes somos', 'empresa', 'aviso legal', 'privacy-policy', 'data-protection'
]

# === CARGAR EXCEL ===

df = pd.read_excel(ARCHIVO_ENTRADA)
if "Website" not in df.columns:
    raise ValueError("No se encontró la columna 'Website' en el archivo de entrada.")
urls = df["Website"].dropna().unique().tolist()

try:
    resultados = pd.read_excel(ARCHIVO_SALIDA)
    procesadas = set(resultados["Website"].dropna().tolist())
except FileNotFoundError:
    resultados = pd.DataFrame(columns=[
        "Website", "Email", "Teléfono", "Dirección", "Facebook", "Instagram", "Twitter", "LinkedIn", "Pinterest", "TikTok", "YouTube"
    ])
    procesadas = set()

# === FUNCIONES DE EXTRACCIÓN ===

def extraer_emails(texto):
    return list(set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", texto)))

def extraer_telefonos(texto):
    return list(set(re.findall(r"\+?\d[\d\s().-]{8,}", texto)))

def extraer_redes_sociales(soup):
    redes = {
        "Facebook": "",
        "Instagram": "",
        "Twitter": "",
        "LinkedIn": "",
        "Pinterest": "",
        "TikTok": "",
        "YouTube": ""
    }
    for a in soup.find_all('a', href=True):
        href = a['href']
        if "facebook.com" in href and not redes["Facebook"]:
            redes["Facebook"] = href
        elif "instagram.com" in href and not redes["Instagram"]:
            redes["Instagram"] = href
        elif ("twitter.com" in href or "x.com" in href) and not redes["Twitter"]:
            redes["Twitter"] = href
        elif "linkedin.com" in href and not redes["LinkedIn"]:
            redes["LinkedIn"] = href
        elif "pinterest.com" in href and not redes["Pinterest"]:
            redes["Pinterest"] = href
        elif "tiktok.com" in href and not redes["TikTok"]:
            redes["TikTok"] = href
        elif "youtube.com" in href and not redes["YouTube"]:
            redes["YouTube"] = href
    return redes

def llamar_libpostal(texto):
    try:
        response = requests.post("http://localhost:8080/parse", data={"address": texto})
        if response.status_code == 200:
            datos = response.json()
            direccion = ", ".join([item["value"] for item in datos])
            return direccion.strip()
    except Exception as e:
        print(f"Error al conectar con la API de Libpostal: {e}")
    return ""

def encontrar_subpaginas(soup, base_url):
    enlaces = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        for sub in SUBPAGINAS:
            if sub in href:
                full_url = urljoin(base_url, a['href'])
                enlaces.add(full_url)
    return list(enlaces)[:3]

# === PROCESAMIENTO PRINCIPAL ===

for i, url in enumerate(urls, 1):
    if url in procesadas:
        continue
    print(f"Procesando {url} ({i}/{len(urls)})...")

    paginas_a_procesar = [url]

    # Intentar encontrar subpáginas relevantes
    try:
        res = requests.get(url, proxies=proxy, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        subpaginas = encontrar_subpaginas(soup, url)
        paginas_a_procesar.extend(subpaginas)
    except Exception as e:
        print(f"❌ Error buscando subpáginas en {url}: {e}")

    for pagina in paginas_a_procesar:
        print(f"  Extrayendo datos de: {pagina}")
        email = telefono = direccion = ""
        facebook = instagram = twitter = linkedin = pinterest = tiktok = youtube = ""

        try:
            res = requests.get(pagina, proxies=proxy, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            texto = soup.get_text(separator=' ', strip=True)

            email = "; ".join(extraer_emails(texto))
            telefono = "; ".join(extraer_telefonos(texto))
            direccion = llamar_libpostal(texto)
            redes = extraer_redes_sociales(soup)
            facebook = redes["Facebook"]
            instagram = redes["Instagram"]
            twitter = redes["Twitter"]
            linkedin = redes["LinkedIn"]
            pinterest = redes["Pinterest"]
            tiktok = redes["TikTok"]
            youtube = redes["YouTube"]

            print(f"✓ Guardado: {email} | {telefono} | {direccion[:80]}... | FB: {facebook} | IG: {instagram} | TW: {twitter}")

            nueva_fila = pd.DataFrame([{
                "Website": pagina,
                "Email": email,
                "Teléfono": telefono,
                "Dirección": direccion,
                "Facebook": facebook,
                "Instagram": instagram,
                "Twitter": twitter,
                "LinkedIn": linkedin,
                "Pinterest": pinterest,
                "TikTok": tiktok,
                "YouTube": youtube
            }])

            resultados = pd.concat([resultados, nueva_fila], ignore_index=True)

            try:
                resultados.to_excel(ARCHIVO_SALIDA, index=False)
            except IllegalCharacterError:
                print("⚠️ Error de carácter ilegal al guardar fila. Se omite esta fila.")

            # Backup numerado por cada X entradas
            if (len(resultados) % BACKUP_CADA == 0):
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                resultados.to_excel(f"backup_{ts}.xlsx", index=False)

        except Exception as e:
            print(f"❌ Error al procesar {pagina}: {e}")

# Guardado final
resultados.to_excel(ARCHIVO_SALIDA, index=False)
print("✅ Proceso completado.")
