import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from urllib.parse import urljoin
from openpyxl.utils.exceptions import IllegalCharacterError
from postal.parser import parse_address  # Usamos la librería postal nativa

# === CONFIGURACIÓN ===

ARCHIVO_ENTRADA = "Webs para extraer todavía.xlsx"
ARCHIVO_SALIDA = "detalles_contacto_web.xlsx"
ARCHIVO_AUX = "CCAA_y_Provincias_CP_Municipios_España.xlsx"
BACKUP_CADA = 200

proxy = {
    "http": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823",
    "https": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823"
}

# === CLAVES PARA DIRECCIONES ===

CLAVES_DIRECCION = [
    "calle", "avenida", "plaza", "camino", "paseo", "carretera", "ronda", "travesía", "urbanización", "polígono", "barrio", "glorieta", "cuesta", "callejón",
    "c.", "c/", "c/.", "cl", "cl.", "avda", "avda.", "av", "av.", "avd", "avd.", "avdª", "avdª.", "pza", "pza.", "pl", "pl.", "plz", "plz.", "plza.",
    "cno", "cno.", "cam", "cam.", "pso", "pso.", "ps", "ps.", "pº", "pº.", "ctra", "ctra.", "crta", "crta.", "rda", "rda.", "rd", "rd.", "trv", "trv.",
    "trva", "trva.", "trav", "trav.", "urb", "urb.", "urbz", "urbz.", "pol", "pol.", "polig", "polig.", "bº", "bº.", "gta", "gta.", "glta", "glta.",
    "cjón", "cjón.", "carrer", "avinguda", "plaça", "camí", "passeig", "travessia", "urbanització", "polígon", "barri", "costera", "passatge", "cr", "cr.",
    "pça", "pça.", "pz", "pz.", "cm", "cm.", "cmi", "cmi.", "pg", "pg.", "psg", "psg.", "br", "br.", "glt", "glt.", "cst", "cst.", "psgt", "psgt.",
    "rúa", "praza", "camiño", "estrada", "costa", "pasaxe", "r", "r.", "r/", "r/.", "prz", "prz.", "cmno", "cmno.", "est", "est.", "estr", "estr.",
    "csta", "csta.", "psgx", "psgx.", "kalea", "etorbidea", "bidea", "pasealekua", "errepidea", "biribilgunea", "zeharbidea", "urbanizazioa", "poligonoa",
    "auzoa", "ibilbidea", "igogailua", "k", "k.", "kl", "kl.", "k/", "k/.", "etb", "etb.", "etorb", "etorb.", "bd", "bd.", "bid", "bid.", "bda", "bda.",
    "paseal", "paseal.", "erp", "erp.", "errep", "errep.", "e", "e.", "brb", "brb.", "brblg", "brblg.", "zeh", "zeh.", "zehb", "zehb.", "au", "au.",
    "az", "az.", "ibl", "ibl.", "ibil", "ibil.", "ig", "ig.", "igog", "igog."
]

# === CLAVES PARA SUBPÁGINAS ===

SUBPAGINAS_KEYWORDS = [
    'contacto', 'contáctanos', 'contacta', 'contactar', 'contacte', 'contacte-nos', 'contacteu', 'kontaktua', 'kontaktu', 'harremanetarako',
    'contact', 'contact-us', 'get-in-touch', 'contact info',
    'sobre nosotros', 'quienes somos', 'quiénes somos', 'empresa', 'acerca de', 'información', 'sobre nostres', 'qui som', 'informació', 'sobre l’empresa',
    'sobre nós', 'quen somos', 'información', 'nor gara', 'enpresa', 'informazioa', 'gure-buruz',
    'about', 'about-us', 'about us', 'company', 'information', 'who we are',
    'aviso legal', 'legal', 'avís legal', 'avis legal', 'legal-notice', 'impressum', 'imprint', 'aviso-legal', 'lege-oharra', 'legezko oharra',
    'privacidad', 'política de privacidad', 'protección de datos', 'privacitat', 'política de privacitat', 'protecció de dades',
    'privacidade', 'política de privacidade', 'pribatutasuna', 'pribatutasun-politika', 'datuen babesa', 'privacy', 'privacy-policy', 'data-protection'
]

# === LIMPIEZA DE CARACTERES ILEGALES PARA EXCEL ===

def limpiar_illegal_characters(texto):
    if not isinstance(texto, str):
        return texto
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', texto)

# === CARGA DE DATOS AUXILIARES ===

df_aux = pd.read_excel(ARCHIVO_AUX)
set_ciudades = set(df_aux["Municipio"].dropna().str.lower())
set_provincias = set(df_aux["Provincia"].dropna().str.lower())
set_codigos_postales = set(df_aux["Código_postal"].dropna().astype(str))

# === CARGA PRINCIPAL ===

df = pd.read_excel(ARCHIVO_ENTRADA)
if "Website" not in df.columns:
    raise ValueError("No se encontró la columna 'Website' en el archivo de entrada.")
urls = df["Website"].dropna().unique().tolist()

try:
    resultados = pd.read_excel(ARCHIVO_SALIDA)
    procesadas = set(resultados["Website"].dropna().tolist())
except FileNotFoundError:
    resultados = pd.DataFrame(columns=["Website", "Email", "Teléfono", "Dirección"])
    procesadas = set()

# === FUNCIONES DE EXTRACCIÓN ===

def extraer_emails(texto):
    return list(set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", texto)))

def extraer_telefonos(texto):
    return list(set(re.findall(r"\+?\d[\d\s().-]{8,}", texto)))

def extraer_candidatos_direccion(texto):
    candidatos = set()
    lineas = re.split(r'[\n.;|•]', texto)
    for linea in lineas:
        l = linea.lower().strip()
        if len(l) < 8:
            continue
        if any(clave in l for clave in CLAVES_DIRECCION):
            candidatos.add(linea.strip())
            continue
        if any(ciudad in l for ciudad in set_ciudades):
            candidatos.add(linea.strip())
            continue
        if any(prov in l for prov in set_provincias):
            candidatos.add(linea.strip())
            continue
        if any(cp in l for cp in set_codigos_postales):
            candidatos.add(linea.strip())
            continue
        if re.search(r'\d{1,4}\s+[a-zA-Z]{4,}', l):
            candidatos.add(linea.strip())
    return list(candidatos)

def llamar_libpostal(texto):
    try:
        parsed = parse_address(texto)
        labels = dict(parsed)
        if parsed and any(label in labels for label in ["road", "city", "postcode"]):
            return ", ".join([v for k, v in parsed])
    except Exception as e:
        print(f"Error al usar la librería Libpostal: {e}")
    return ""

def extraer_direccion_postal(texto):
    candidatos = extraer_candidatos_direccion(texto)
    for cand in candidatos:
        direccion = llamar_libpostal(cand)
        if direccion:
            return cand, direccion
    return "", ""

def encontrar_subpaginas(soup, base_url):
    enlaces = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        for sub in SUBPAGINAS_KEYWORDS:
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

    # Buscar subpáginas relevantes
    try:
        res = requests.get(url, proxies=proxy, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        subpaginas = encontrar_subpaginas(soup, url)
        paginas_a_procesar.extend(subpaginas)
    except Exception as e:
        print(f"❌ Error buscando subpáginas en {url}: {e}")

    for pagina in paginas_a_procesar:
        print(f"  Extrayendo datos de: {pagina}")
        email = telefono = direccion_candidata = direccion = ""

        try:
            res = requests.get(pagina, proxies=proxy, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            texto = soup.get_text(separator=' ', strip=True)

            email = "; ".join(extraer_emails(texto))
            telefono = "; ".join(extraer_telefonos(texto))
            direccion_candidata, direccion = extraer_direccion_postal(texto)

            print(f"✓ Guardado: {email} | {telefono} | {direccion[:80]}...")

            fila = {
                "Website": limpiar_illegal_characters(pagina),
                "Email": limpiar_illegal_characters(email),
                "Teléfono": limpiar_illegal_characters(telefono),
                "Dirección": limpiar_illegal_characters(direccion)
            }

            nueva_fila = pd.DataFrame([fila])
            resultados = pd.concat([resultados, nueva_fila], ignore_index=True)

            try:
                for col in resultados.columns:
                    resultados[col] = resultados[col].apply(limpiar_illegal_characters)
                resultados.to_excel(ARCHIVO_SALIDA, index=False)
            except IllegalCharacterError:
                print("⚠️ Error de carácter ilegal al guardar fila. Se omite esta fila.")

            if (len(resultados) % BACKUP_CADA == 0):
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                resultados.to_excel(f"backup_{ts}.xlsx", index=False)

        except Exception as e:
            print(f"❌ Error al procesar {pagina}: {e}")

for col in resultados.columns:
    resultados[col] = resultados[col].apply(limpiar_illegal_characters)
resultados.to_excel(ARCHIVO_SALIDA, index=False)
print("✅ Proceso completado.")
