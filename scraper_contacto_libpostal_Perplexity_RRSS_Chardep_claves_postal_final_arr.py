import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin
from openpyxl.utils.exceptions import IllegalCharacterError
from postal.parser import parse_address

# === CONFIGURACIÓN ===

ARCHIVO_ENTRADA = "Webs para extraer todavía.xlsx"
ARCHIVO_SALIDA = "detalles_contacto_web.xlsx"
ARCHIVO_CLAVES = "CCAA_y_Provincias_CP_Municipios_España.xlsx"
BACKUP_CADA = 200

proxy = {
    "http": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823",
    "https": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823"
}

CLAVES_DIRECCION = [
    "calle", "avenida", "plaza", "camino", "paseo", "carretera", "ronda", "travesía", "urbanización", "polígono", "barrio", "glorieta", "cuesta", "callejón",
    "c/", "c.", "cl", "avda", "plz", "pza", "cam", "pso", "ps", "pº", "ctra", "rda", "trv", "urb", "pol", "gta", "cjón", "carrer", "avinguda", "plaça",
    "camí", "passeig", "travessia", "urbanització", "polígon", "barri", "costera", "passatge", "cr", "pça", "pz", "cm", "cmi", "pg", "psg", "br", "glt",
    "cst", "psgt", "rúa", "praza", "camiño", "estrada", "costa", "pasaxe", "r/", "prz", "cmno", "est", "estr", "csta", "psgx", "kalea", "etorbidea",
    "bidea", "pasealekua", "errepidea", "biribilgunea", "zeharbidea", "urbanizazioa", "poligonoa", "auzoa", "ibilbidea", "igogailua", "k", "kl", "etb",
    "etorb", "bd", "bid", "bda", "paseal", "erp", "errep", "e", "brb", "brblg", "zeh", "zehb", "au", "az", "ibl", "ibil", "ig", "igog"
]

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

# === CARGA DE CLAVES AUXILIARES ===

df_claves = pd.read_excel(ARCHIVO_CLAVES)
municipios = set(df_claves["Municipio"].astype(str).str.lower().unique())
provincias = set(df_claves["Provincia"].astype(str).str.lower().unique())
codigos_postales = set(df_claves["Código_postal"].astype(str).str.zfill(5).unique())

dic_cp_prov = dict(zip(df_claves["Código_postal"].astype(str).str.zfill(5), df_claves["Provincia"].astype(str)))
dic_cp_mun = dict(zip(df_claves["Código_postal"].astype(str).str.zfill(5), df_claves["Municipio"].astype(str)))
dic_cp_ccaa = dict(zip(df_claves["Código_postal"].astype(str).str.zfill(5), df_claves["Comunidad_Autónoma"].astype(str)))

# === FUNCIONES DE EXTRACCIÓN ===

def extraer_emails(texto):
    return list(set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", texto)))

def extraer_telefonos(texto):
    posibles = re.findall(
        r'(?:\+34|0034)?[\s\-\.]*([6789][\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d)',
        texto
    )
    limpios = []
    for t in posibles:
        solo_digitos = re.sub(r'\D', '', t)
        if len(solo_digitos) == 9:
            limpios.append(" ".join([solo_digitos[:3], solo_digitos[3:6], solo_digitos[6:]]))
    return list(set(limpios))

def extraer_candidatos_direccion(texto):
    """
    Busca claves y construye bloques de 10 palabras antes y 10 después de la clave,
    aunque abarquen varias líneas.
    """
    candidatos = set()
    # Normaliza saltos de línea y separadores
    texto_limpio = re.sub(r'[\n\r;|•]', ' ', texto)
    palabras = texto_limpio.split()
    palabras_lower = [w.lower() for w in palabras]
    total = len(palabras)
    for idx, palabra in enumerate(palabras_lower):
        es_clave = (
            any(clave in palabra for clave in CLAVES_DIRECCION)
            or palabra in municipios
            or palabra in provincias
            or palabra in codigos_postales
            or re.match(r'\d{5}', palabra)
        )
        if es_clave:
            ini = max(0, idx - 10)
            fin = min(total, idx + 11)
            fragmento = ' '.join(palabras[ini:fin])
            if len(fragmento) > 15:
                candidatos.add(fragmento.strip())
    return list(candidatos)

def llamar_libpostal(texto):
    try:
        parsed = parse_address(texto)
        componentes_permitidos = {"road", "house_number", "suburb", "city", "postcode", "state", "country"}
        direccion = [v for v, k in parsed if k in componentes_permitidos]
        if direccion and any(label in dict(parsed) for label in ["road", "city", "postcode"]):
            return ", ".join(direccion)
    except Exception as e:
        print(f"Error al usar la librería Libpostal: {e}")
    return ""

def enriquecer_direccion(direccion):
    if not direccion:
        return direccion
    cp_match = re.search(r"\b\d{5}\b", direccion)
    if cp_match:
        cp = cp_match.group()
        provincia = dic_cp_prov.get(cp, "")
        municipio = dic_cp_mun.get(cp, "")
        ccaa = dic_cp_ccaa.get(cp, "")
        partes = [direccion]
        if municipio and municipio.lower() not in direccion.lower():
            partes.append(municipio)
        if provincia and provincia.lower() not in direccion.lower():
            partes.append(provincia)
        if ccaa and ccaa.lower() not in direccion.lower():
            partes.append(ccaa)
        return ", ".join(partes)
    return direccion

def extraer_direccion_postal(texto):
    for candidata in extraer_candidatos_direccion(texto):
        direccion = llamar_libpostal(candidata)
        if direccion:
            return enriquecer_direccion(direccion)
    return ""

def limpiar_illegal_characters(texto):
    if not isinstance(texto, str):
        return texto
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', texto)

def encontrar_subpaginas(soup, base_url):
    enlaces = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        for sub in SUBPAGINAS_KEYWORDS:
            if sub in href:
                full_url = urljoin(base_url, a['href'])
                enlaces.add(full_url)
    return list(enlaces)[:3]

def get_texto_url(url, proxies, timeout=15):
    try:
        res = requests.get(url, proxies=proxies, timeout=timeout)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        return soup.get_text(separator=' ', strip=True), "OK"
    except Exception as e:
        print(f"⚠️ Error con proxy en {url}: {e}")
        try:
            res = requests.get(url, timeout=timeout)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            return soup.get_text(separator=' ', strip=True), "OK (sin proxy)"
        except Exception as e2:
            print(f"❌ Error sin proxy en {url}: {e2}")
            return "", f"Error: {e} | Fallback: {e2}"

# === CARGAR EXCEL DE WEBS ===

df = pd.read_excel(ARCHIVO_ENTRADA)
if "Website" not in df.columns:
    raise ValueError("No se encontró la columna 'Website' en el archivo de entrada.")
urls = df["Website"].dropna().unique().tolist()

# === CARGA PARCIAL SI YA EXISTE ARCHIVO DE SALIDA ===

try:
    resultados = pd.read_excel(ARCHIVO_SALIDA)
    procesadas = set(resultados["Website"].dropna().tolist())
except FileNotFoundError:
    resultados = pd.DataFrame(columns=["Website", "Email", "Teléfono", "Dirección", "Estado"])
    procesadas = set()

# === PROCESAMIENTO PRINCIPAL ===

for i, url in enumerate(urls, 1):
    if url in procesadas:
        continue
    print(f"Procesando {url} ({i}/{len(urls)})...")

    paginas_a_procesar = [url]

    # Buscar subpáginas relevantes
    texto, estado = get_texto_url(url, proxy)
    if texto:
        try:
            soup = BeautifulSoup(texto, "html.parser")
            subpaginas = encontrar_subpaginas(soup, url)
            paginas_a_procesar.extend(subpaginas)
        except Exception as e:
            print(f"❌ Error buscando subpáginas en {url}: {e}")

    for pagina in paginas_a_procesar:
        if pagina in procesadas:
            continue
        email = telefono = direccion = ""
        estado_pagina = "OK"
        texto_pagina, estado_pagina = get_texto_url(pagina, proxy)
        if texto_pagina:
            email = "; ".join(extraer_emails(texto_pagina))
            telefono = "; ".join(extraer_telefonos(texto_pagina))
            direccion = extraer_direccion_postal(texto_pagina)
            print(f"✓ Guardado: {email} | {telefono} | {direccion[:80]}...")
        else:
            print(f"❌ No se pudo obtener texto de {pagina}")

        nueva_fila = pd.DataFrame([{
            "Website": limpiar_illegal_characters(pagina),
            "Email": limpiar_illegal_characters(email),
            "Teléfono": limpiar_illegal_characters(telefono),
            "Dirección": limpiar_illegal_characters(direccion),
            "Estado": limpiar_illegal_characters(estado_pagina)
        }])

        resultados = pd.concat([resultados, nueva_fila], ignore_index=True)

        try:
            resultados.to_excel(ARCHIVO_SALIDA, index=False)
        except IllegalCharacterError:
            print("⚠️ Error de carácter ilegal al guardar fila. Se omite esta fila.")

        if (len(resultados) % BACKUP_CADA == 0):
            ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            resultados.to_excel(f"backup_{ts}.xlsx", index=False)

# Guardado final
resultados.to_excel(ARCHIVO_SALIDA, index=False)
print("✅ Proceso completado.")
