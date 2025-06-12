import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin
from openpyxl.utils.exceptions import IllegalCharacterError

# === CONFIGURACIÓN ===

ARCHIVO_ENTRADA = "Webs para extraer todavía.xlsx"
ARCHIVO_SALIDA = "detalles_contacto_web.xlsx"
BACKUP_CADA = 200  # cada cuántos registros guardar backup con timestamp

# === CREDENCIALES PROXY DATAIMPULSE ===
proxy = {
    "http": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823",
    "https": "http://35bf745b8966f72d1df5:356e064773a532ee@gw.dataimpulse.com:823"
}

# === CLAVES PARA DIRECCIONES ===

CLAVES_DIRECCION = [
    "calle", "avenida", "plaza", "camino", "paseo", "carretera", "ronda", "travesía", "urbanización", "polígono", "barrio", "glorieta", "cuesta", "callejón",
    "c/", "c.", "cl", "avda", "plz", "pza", "cam", "pso", "ps", "pº", "ctra", "rda", "trv", "urb", "pol", "gta", "cjón", "carrer", "avinguda", "plaça",
    "camí", "passeig", "travessia", "urbanització", "polígon", "barri", "costera", "passatge", "cr", "pça", "pz", "cm", "cmi", "pg", "psg", "br", "glt",
    "cst", "psgt", "rúa", "praza", "camiño", "estrada", "costa", "pasaxe", "r/", "prz", "cmno", "est", "estr", "csta", "psgx", "kalea", "etorbidea",
    "bidea", "pasealekua", "errepidea", "biribilgunea", "zeharbidea", "urbanizazioa", "poligonoa", "auzoa", "ibilbidea", "igogailua", "k", "kl", "etb",
    "etorb", "bd", "bid", "bda", "paseal", "erp", "errep", "e", "brb", "brblg", "zeh", "zehb", "au", "az", "ibl", "ibil", "ig", "igog"
]

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
        if re.search(r'\d{5}', l):  # código postal
            candidatos.add(linea.strip())
            continue
        if re.search(r'\d{1,4}\s+[a-zA-Z]{4,}', l):  # número + palabra larga
            candidatos.add(linea.strip())
    return list(candidatos)

def llamar_libpostal(texto):
    try:
        response = requests.post("http://localhost:8080/parse", data={"address": texto})
        if response.status_code == 200:
            datos = response.json()
            if any(item["label"] in ["road", "city", "postcode"] for item in datos):
                return ", ".join([item["value"] for item in datos])
    except Exception as e:
        print(f"Error al conectar con la API de Libpostal: {e}")
    return ""

def extraer_direccion_postal(texto):
    for candidata in extraer_candidatos_direccion(texto):
        direccion = llamar_libpostal(candidata)
        if direccion:
            return direccion
    return ""

# === LIMPIEZA DE CARACTERES ILEGALES PARA EXCEL ===

def limpiar_illegal_characters(texto):
    if not isinstance(texto, str):
        return texto
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', texto)

# === CARGAR EXCEL ===

df = pd.read_excel(ARCHIVO_ENTRADA)
if "Website" not in df.columns:
    raise ValueError("No se encontró la columna 'Website' en el archivo de entrada.")
urls = df["Website"].dropna().unique().tolist()

# === CARGA PARCIAL SI YA EXISTE ARCHIVO DE SALIDA ===

try:
    resultados = pd.read_excel(ARCHIVO_SALIDA)
    procesadas = set(resultados["Website"].dropna().tolist())
except FileNotFoundError:
    resultados = pd.DataFrame(columns=["Website", "Email", "Teléfono", "Dirección"])
    procesadas = set()

# === PROCESAMIENTO PRINCIPAL ===

contador = len(resultados)

for i, url in enumerate(urls, 1):
    if url in procesadas:
        continue
    print(f"Procesando {url} ({i}/{len(urls)})...")

    email = telefono = direccion = ""

    try:
        res = requests.get(url, proxies=proxy, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        texto = soup.get_text(separator=' ', strip=True)

        email = "; ".join(extraer_emails(texto))
        telefono = "; ".join(extraer_telefonos(texto))
        direccion = extraer_direccion_postal(texto)

        print(f"✓ Guardado: {email} | {telefono} | {direccion[:80]}...")

        nueva_fila = pd.DataFrame([{
            "Website": limpiar_illegal_characters(url),
            "Email": limpiar_illegal_characters(email),
            "Teléfono": limpiar_illegal_characters(telefono),
            "Dirección": limpiar_illegal_characters(direccion)
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
        print(f"❌ Error al procesar {url}: {e}")

# Guardado final
resultados.to_excel(ARCHIVO_SALIDA, index=False)
print("✅ Proceso completado.")
