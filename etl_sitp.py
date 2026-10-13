#!/usr/bin/env python3
"""ETL SITP Zonal: descarga validaciones diarias, limpia, carga a SQLite (esquema en estrella) y concilia.

Uso (variables de entorno opcionales):
    FECHA_INICIO=2026-09-21 FECHA_FIN=2026-09-21 DIR_TRABAJO=./sitp python3 etl_sitp.py
Por defecto procesa un solo día (2026-09-21). Es idempotente: si se repite, omite los días ya cargados.
Derivado de 01_ETL_SITP_Zonal.ipynb.
"""


import os, re, time, zipfile, sqlite3, codecs, shutil
from datetime import date, timedelta, datetime
import numpy as np
import pandas as pd
import requests

try:
    display
except NameError:
    display = print

# ===== PARÁMETROS (lo único que normalmente se edita) =====
FECHA_INICIO = date.fromisoformat(os.environ.get("FECHA_INICIO", "2026-09-21"))
FECHA_FIN    = date.fromisoformat(os.environ.get("FECHA_FIN", "2026-09-21"))   # por defecto, un solo día
# Para una prueba rápida con un solo día, usa: FECHA_FIN = date(2026, 9, 21)
BASE_URL     = "https://storage.googleapis.com/validaciones_tmsa/ValidacionZonal"

DIR_TRABAJO  = os.environ.get("DIR_TRABAJO", "./sitp")
DIR_STAGING  = os.path.join(DIR_TRABAJO, "staging")        # zona de staging: zips originales sin tocar
DB_PATH      = os.path.join(DIR_TRABAJO, "dw_sitp.db")     # bodega local

CHUNK        = 250_000      # filas por bloque en memoria
TOLERANCIA_SALDO = 1.0      # pesos de diferencia tolerados en Saldo_Previo - Valor = Saldo_Despues
RECHAZAR_SALDO_INCOHERENTE = False   # False: se carga y se marca con flag. True: se rechaza.

os.makedirs(DIR_STAGING, exist_ok=True)
DIAS = [FECHA_INICIO + timedelta(days=i) for i in range((FECHA_FIN - FECHA_INICIO).days + 1)]
print(f"{len(DIAS)} días: {DIAS[0]} -> {DIAS[-1]}")

def url_dia(d):
    return f"{BASE_URL}/validacionZonal{d:%Y%m%d}.zip"

def ruta_zip(d):
    return os.path.join(DIR_STAGING, f"validacionZonal{d:%Y%m%d}.zip")

def descargar(d, intentos=3):
    destino = ruta_zip(d)
    if os.path.exists(destino) and zipfile.is_zipfile(destino):
        return destino
    for i in range(1, intentos + 1):
        try:
            with requests.get(url_dia(d), stream=True, timeout=120) as r:
                r.raise_for_status()
                tmp = destino + ".part"
                with open(tmp, "wb") as f:
                    for bloque in r.iter_content(1 << 20):
                        f.write(bloque)
            os.replace(tmp, destino)
            if not zipfile.is_zipfile(destino):
                raise IOError("el archivo descargado no es un zip válido")
            return destino
        except Exception as e:
            print(f"  intento {i}/{intentos} falló: {e}")
            time.sleep(5 * i)
    raise RuntimeError(f"No se pudo descargar {url_dia(d)}")

for d in DIAS:
    t0 = time.time()
    p = descargar(d)
    print(d, f"{os.path.getsize(p) / 1e6:6.1f} MB", f"{time.time() - t0:5.0f} s")

def nombre_csv(zf):
    nombres = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if len(nombres) != 1:
        raise ValueError(f"Se esperaba 1 CSV dentro del zip y hay {len(nombres)}: {zf.namelist()}")
    return nombres[0]

def detectar_encoding(zf, nombre):
    with zf.open(nombre) as f:
        muestra = f.read(8_000_000)
    for enc in ("utf-8-sig", "latin-1"):
        try:
            codecs.getincrementaldecoder(enc)().decode(muestra)   # tolera un carácter cortado al final
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"



DDL = '''
CREATE TABLE IF NOT EXISTS dim_tiempo (
    tiempo_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL, hora INTEGER NOT NULL,
    anio INTEGER, mes INTEGER, dia INTEGER, dia_semana INTEGER, nombre_dia TEXT,
    es_fin_de_semana INTEGER, es_festivo INTEGER, franja TEXT, hora_pico_fuente TEXT,
    UNIQUE (fecha, hora)
);
CREATE TABLE IF NOT EXISTS dim_estacion (
    estacion_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    estacion_cod TEXT NOT NULL UNIQUE, estacion_nombre TEXT, fase TEXT
);
CREATE TABLE IF NOT EXISTS dim_ruta (
    ruta_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    ruta_cod TEXT NOT NULL, linea_cod TEXT NOT NULL,
    ruta_nombre TEXT, linea_nombre TEXT, sistema TEXT,
    UNIQUE (ruta_cod, linea_cod)
);
CREATE TABLE IF NOT EXISTS dim_operador (
    operador_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    operador_cod TEXT NOT NULL UNIQUE, operador_nombre TEXT
);
CREATE TABLE IF NOT EXISTS dim_perfil (
    perfil_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    perfil_cod TEXT NOT NULL, perfil_nombre TEXT NOT NULL, tipo_tarjeta TEXT NOT NULL,
    es_subsidiado INTEGER,
    UNIQUE (perfil_cod, perfil_nombre, tipo_tarjeta)
);
CREATE TABLE IF NOT EXISTS dim_vehiculo (
    vehiculo_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    id_vehiculo TEXT NOT NULL UNIQUE, tipo_vehiculo_cod TEXT, tipo_vehiculo_nombre TEXT
);
CREATE TABLE IF NOT EXISTS dim_tarifa (
    tarifa_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo_tarifa TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS fact_validaciones (
    tiempo_sk INTEGER NOT NULL REFERENCES dim_tiempo(tiempo_sk),
    estacion_sk INTEGER NOT NULL REFERENCES dim_estacion(estacion_sk),
    ruta_sk INTEGER NOT NULL REFERENCES dim_ruta(ruta_sk),
    operador_sk INTEGER NOT NULL REFERENCES dim_operador(operador_sk),
    perfil_sk INTEGER NOT NULL REFERENCES dim_perfil(perfil_sk),
    vehiculo_sk INTEGER NOT NULL REFERENCES dim_vehiculo(vehiculo_sk),
    tarifa_sk INTEGER NOT NULL REFERENCES dim_tarifa(tarifa_sk),
    numero_tarjeta TEXT,            -- dimensión degenerada
    dispositivo TEXT,               -- dimensión degenerada
    fecha_hora_transaccion TEXT,
    valor REAL,                     -- aditiva
    saldo_previo REAL,              -- semi-aditiva
    saldo_posterior REAL,           -- semi-aditiva
    flag_saldo_incoherente INTEGER, -- 1 si saldo_previo - valor != saldo_posterior
    archivo_fecha TEXT              -- linaje: de qué archivo diario viene
);
CREATE TABLE IF NOT EXISTS rechazados (
    Dispositivo TEXT, Emisor TEXT, Estacion_Parada TEXT, Fase TEXT, Fecha_Clearing TEXT,
    Fecha_Transaccion TEXT, Hora_Pico_SN TEXT, ID_Vehiculo TEXT, Linea TEXT, Nombre_Perfil TEXT,
    Numero_Tarjeta TEXT, Operador TEXT, Ruta TEXT, Saldo_Despues_Transaccion TEXT,
    Saldo_Previo_a_Transaccion TEXT, Sistema TEXT, Tipo_Tarifa TEXT, Tipo_Tarjeta TEXT,
    Tipo_Vehiculo TEXT, Valor TEXT,
    motivo TEXT, archivo_fecha TEXT
);
CREATE TABLE IF NOT EXISTS etl_control (
    archivo TEXT PRIMARY KEY, fecha_archivo TEXT, estado TEXT,
    filas_leidas INTEGER, filas_cargadas INTEGER, filas_rechazadas INTEGER, filas_duplicadas INTEGER,
    saldo_incoherente INTEGER, saldo_negativo INTEGER, fuera_de_fecha INTEGER,
    inicio TEXT, fin TEXT, mensaje TEXT
);
'''

con = sqlite3.connect(DB_PATH)
# Ajustes de velocidad para carga masiva. Si el proceso se corta de golpe, borra el .db y vuelve a correr.
con.execute("PRAGMA journal_mode = MEMORY")
con.execute("PRAGMA synchronous = OFF")
con.execute("PRAGMA temp_store = MEMORY")
con.execute("PRAGMA cache_size = -200000")
con.executescript(DDL)
con.commit()
print("Tablas:", [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")])

COLS_ORIGEN = ["Dispositivo", "Emisor", "Estacion_Parada", "Fase", "Fecha_Clearing", "Fecha_Transaccion",
               "Hora_Pico_SN", "ID_Vehiculo", "Linea", "Nombre_Perfil", "Numero_Tarjeta", "Operador", "Ruta",
               "Saldo_Despues_Transaccion", "Saldo_Previo_a_Transaccion", "Sistema", "Tipo_Tarifa",
               "Tipo_Tarjeta", "Tipo_Vehiculo", "Valor"]
OBLIGATORIAS = ["Dispositivo", "Estacion_Parada", "Fecha_Transaccion", "ID_Vehiculo", "Linea", "Nombre_Perfil",
                "Numero_Tarjeta", "Operador", "Ruta", "Tipo_Tarifa", "Tipo_Tarjeta", "Valor",
                "Saldo_Previo_a_Transaccion", "Saldo_Despues_Transaccion"]
PERFILES_SUBSIDIADOS = {"002", "005", "006", "009"}
NOMBRES_DIA = {1: "Lunes", 2: "Martes", 3: "Miércoles", 4: "Jueves", 5: "Viernes", 6: "Sábado", 7: "Domingo"}

try:
    import holidays
    FESTIVOS = holidays.country_holidays("CO", years=range(DIAS[0].year - 1, DIAS[-1].year + 2))
except Exception as e:
    FESTIVOS = {}
    print("Aviso: sin librería 'holidays', es_festivo quedará en 0 ->", e)

def extraer_codigo(serie):
    # "(10330) LK810" -> ("10330", "LK810"). Si no trae paréntesis, el código es SIN_CODIGO.
    p = serie.str.extract(r"^\(\s*([^)]*?)\s*\)\s*(.*)$")
    cod = p[0].fillna("SIN_CODIGO")
    txt = p[1].where(p[0].notna(), serie).fillna("SIN_DATO").str.strip()
    return cod, txt

def franja_horaria(hora):
    return pd.Series(np.select(
        [hora <= 4, hora <= 8, hora <= 11, hora <= 15, hora <= 19],
        ["Madrugada (0-4)", "Mañana (5-8)", "Media mañana (9-11)", "Mediodía-tarde (12-15)", "Tarde-noche (16-19)"],
        default="Noche (20-23)"), index=hora.index)

def transformar(raw, d):
    # Devuelve (hechos_listos, rechazados, estadisticas) para un bloque de filas crudas.
    df = raw[COLS_ORIGEN].copy()
    for c in COLS_ORIGEN:
        s = df[c]
        df[c] = s.where(s.isna(), s.astype(str).str.strip()).replace("", np.nan)

    ts    = pd.to_datetime(df["Fecha_Transaccion"], errors="coerce")
    valor = pd.to_numeric(df["Valor"], errors="coerce")
    sprev = pd.to_numeric(df["Saldo_Previo_a_Transaccion"], errors="coerce")
    spost = pd.to_numeric(df["Saldo_Despues_Transaccion"], errors="coerce")

    r1 = df[OBLIGATORIAS].isna().any(axis=1)
    r2 = ts.isna() | valor.isna() | sprev.isna() | spost.isna()
    r3 = valor < 0
    incoherente = (sprev - valor - spost).abs() > TOLERANCIA_SALDO
    r4 = incoherente & RECHAZAR_SALDO_INCOHERENTE

    motivo = pd.Series(np.select(
        [r1, r2, r3, r4],
        ["R1_NULO_OBLIGATORIO", "R2_FORMATO_INVALIDO", "R3_VALOR_NEGATIVO", "R4_SALDO_INCOHERENTE"],
        default=""), index=df.index)

    rech = raw.loc[motivo != "", COLS_ORIGEN].copy()
    rech["motivo"] = motivo[motivo != ""]
    rech["archivo_fecha"] = d.isoformat()

    ok = df.loc[motivo == ""]
    dup = ok.duplicated(subset=["Dispositivo", "Numero_Tarjeta", "Fecha_Transaccion", "ID_Vehiculo", "Valor"], keep="first")
    n_dup = int(dup.sum())
    ok = ok.loc[~dup]
    ix = ok.index

    h = pd.DataFrame(index=ix)
    h["fecha"] = ts[ix].dt.strftime("%Y-%m-%d")
    h["hora"] = ts[ix].dt.hour.astype("int64")
    h["fecha_hora_transaccion"] = ts[ix].dt.strftime("%Y-%m-%d %H:%M:%S")
    h["hora_pico_fuente"] = ok["Hora_Pico_SN"].fillna("SIN_DATO")
    h["estacion_cod"], est_txt = extraer_codigo(ok["Estacion_Parada"])
    h["estacion_nombre"] = (est_txt.str.split("|").str[-1]
                            .str.replace(r"^[A-Za-z0-9]+_", "", regex=True).str.strip())
    h["fase"] = ok["Fase"].fillna("SIN_DATO")
    h["ruta_cod"], h["ruta_nombre"] = extraer_codigo(ok["Ruta"])
    h["linea_cod"], h["linea_nombre"] = extraer_codigo(ok["Linea"])
    h["sistema"] = ok["Sistema"].fillna("SIN_DATO")
    h["operador_cod"], h["operador_nombre"] = extraer_codigo(ok["Operador"])
    h["perfil_cod"], h["perfil_nombre"] = extraer_codigo(ok["Nombre_Perfil"])
    h["tipo_tarjeta"] = ok["Tipo_Tarjeta"]
    h["es_subsidiado"] = h["perfil_cod"].isin(PERFILES_SUBSIDIADOS).astype("int64")
    h["id_vehiculo"] = ok["ID_Vehiculo"]
    h["tipo_vehiculo_cod"], h["tipo_vehiculo_nombre"] = extraer_codigo(ok["Tipo_Vehiculo"])
    h["tipo_tarifa"] = ok["Tipo_Tarifa"]
    h["numero_tarjeta"] = ok["Numero_Tarjeta"]
    h["dispositivo"] = ok["Dispositivo"]
    h["valor"] = valor[ix]
    h["saldo_previo"] = sprev[ix]
    h["saldo_posterior"] = spost[ix]
    h["flag_saldo_incoherente"] = incoherente[ix].astype("int64")

    est = {
        "leidas": len(raw),
        "rechazadas": int((motivo != "").sum()),
        "duplicadas": n_dup,
        "cargadas": len(h),
        "saldo_incoherente": int(h["flag_saldo_incoherente"].sum()),
        "saldo_negativo": int(((h["saldo_previo"] < 0) | (h["saldo_posterior"] < 0)).sum()),
        "fuera_de_fecha": int((h["fecha"] != d.isoformat()).sum()),
        "motivos": motivo[motivo != ""].value_counts().to_dict(),
    }
    return h, rech, est

def dims_del_chunk(h):
    # Arma, para cada dimensión, las filas candidatas (claves naturales + atributos) de este bloque.
    t = h[["fecha", "hora", "hora_pico_fuente"]].drop_duplicates(["fecha", "hora"]).copy()
    f = pd.to_datetime(t["fecha"])
    t["anio"] = f.dt.year.astype("int64")
    t["mes"] = f.dt.month.astype("int64")
    t["dia"] = f.dt.day.astype("int64")
    t["dia_semana"] = (f.dt.dayofweek + 1).astype("int64")
    t["nombre_dia"] = t["dia_semana"].map(NOMBRES_DIA)
    t["es_fin_de_semana"] = (t["dia_semana"] >= 6).astype("int64")
    t["es_festivo"] = f.dt.date.map(lambda x: int(x in FESTIVOS)).astype("int64")
    t["franja"] = franja_horaria(t["hora"])
    return {
        "dim_tiempo": t,
        "dim_estacion": h[["estacion_cod", "estacion_nombre", "fase"]],
        "dim_ruta": h[["ruta_cod", "linea_cod", "ruta_nombre", "linea_nombre", "sistema"]],
        "dim_operador": h[["operador_cod", "operador_nombre"]],
        "dim_perfil": h[["perfil_cod", "perfil_nombre", "tipo_tarjeta", "es_subsidiado"]],
        "dim_vehiculo": h[["id_vehiculo", "tipo_vehiculo_cod", "tipo_vehiculo_nombre"]],
        "dim_tarifa": h[["tipo_tarifa"]],
    }

DIMS = {
    "dim_tiempo":   dict(nat=["fecha", "hora"], attrs=["anio", "mes", "dia", "dia_semana", "nombre_dia", "es_fin_de_semana", "es_festivo", "franja", "hora_pico_fuente"], scd1=[]),
    "dim_estacion": dict(nat=["estacion_cod"], attrs=["estacion_nombre", "fase"], scd1=["estacion_nombre", "fase"]),
    "dim_ruta":     dict(nat=["ruta_cod", "linea_cod"], attrs=["ruta_nombre", "linea_nombre", "sistema"], scd1=["ruta_nombre", "linea_nombre", "sistema"]),
    "dim_operador": dict(nat=["operador_cod"], attrs=["operador_nombre"], scd1=["operador_nombre"]),
    "dim_perfil":   dict(nat=["perfil_cod", "perfil_nombre", "tipo_tarjeta"], attrs=["es_subsidiado"], scd1=["es_subsidiado"]),
    "dim_vehiculo": dict(nat=["id_vehiculo"], attrs=["tipo_vehiculo_cod", "tipo_vehiculo_nombre"], scd1=["tipo_vehiculo_cod", "tipo_vehiculo_nombre"]),
    "dim_tarifa":   dict(nat=["tipo_tarifa"], attrs=[], scd1=[]),
}
SK = {t: t.replace("dim_", "") + "_sk" for t in DIMS}
COLS_ENTERAS = {"hora", "anio", "mes", "dia", "dia_semana", "es_fin_de_semana", "es_festivo", "es_subsidiado"} | set(SK.values())

def a_tuplas(df):
    # Tipos nativos de Python (sqlite3 no acepta numpy.int64) y NaN -> None.
    return df.astype(object).where(df.notna(), None).values.tolist()

def leer_cache(con, tabla):
    df = pd.read_sql(f"SELECT * FROM {tabla}", con)
    return df.astype({c: "int64" for c in df.columns if c in COLS_ENTERAS})

def resolver_dimension(con, tabla, frame, cache):
    cfg = DIMS[tabla]
    nat, attrs, scd1, sk = cfg["nat"], cfg["attrs"], cfg["scd1"], SK[tabla]
    cols = nat + attrs
    cand = frame[cols].drop_duplicates(subset=nat)
    m = cand.merge(cache[tabla], on=nat, how="left", suffixes=("", "_act"))

    nuevos = m[m[sk].isna()]
    existentes = m[m[sk].notna()]
    cambios = existentes.iloc[0:0]
    if scd1 and len(existentes):
        dif = np.zeros(len(existentes), dtype=bool)
        for c in scd1:
            dif |= (existentes[c].values != existentes[c + "_act"].values)
        cambios = existentes[dif]

    if len(nuevos):
        con.executemany(f"INSERT INTO {tabla} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                        a_tuplas(nuevos[cols]))
    if len(cambios):
        sets = ",".join(f"{c}=?" for c in scd1)
        con.executemany(f"UPDATE {tabla} SET {sets} WHERE {sk}=?",
                        a_tuplas(cambios[scd1 + [sk]].astype({sk: "int64"})))
    if len(nuevos) or len(cambios):
        cache[tabla] = leer_cache(con, tabla)
    return len(nuevos), len(cambios)

COLS_HECHO = [SK[t] for t in DIMS] + ["numero_tarjeta", "dispositivo", "fecha_hora_transaccion", "valor",
                                       "saldo_previo", "saldo_posterior", "flag_saldo_incoherente", "archivo_fecha"]

def cargar_hechos(con, h, d, cache):
    f = h
    for tabla, cfg in DIMS.items():
        sk = SK[tabla]
        f = f.merge(cache[tabla][cfg["nat"] + [sk]], on=cfg["nat"], how="left", validate="many_to_one")
        if f[sk].isna().any():
            raise ValueError(f"Lookup sin resultado en {tabla}")
    f["archivo_fecha"] = d.isoformat()
    con.executemany(f"INSERT INTO fact_validaciones ({','.join(COLS_HECHO)}) VALUES ({','.join('?' * len(COLS_HECHO))})",
                    a_tuplas(f[COLS_HECHO]))
    return len(f)

def procesar_dia(con, d, cache):
    archivo = os.path.basename(ruta_zip(d))
    previo = con.execute("SELECT estado FROM etl_control WHERE archivo=?", (archivo,)).fetchone()
    if previo and previo[0] == "OK":
        print(f"{d}: ya procesado, se omite")
        return
    if previo:   # un intento anterior quedó a medias: se limpia ese día y se reprocesa completo
        con.execute("DELETE FROM fact_validaciones WHERE archivo_fecha=?", (d.isoformat(),))
        con.execute("DELETE FROM rechazados WHERE archivo_fecha=?", (d.isoformat(),))
    inicio = datetime.now().isoformat(timespec="seconds")
    con.execute("INSERT OR REPLACE INTO etl_control (archivo, fecha_archivo, estado, inicio) VALUES (?,?,?,?)",
                (archivo, d.isoformat(), "EN_PROCESO", inicio))
    con.commit()

    tot = dict(leidas=0, cargadas=0, rechazadas=0, duplicadas=0, saldo_incoherente=0, saldo_negativo=0, fuera_de_fecha=0)
    motivos = {}
    t0 = time.time()
    try:
        with zipfile.ZipFile(ruta_zip(d)) as zf:
            nom = nombre_csv(zf)
            enc = detectar_encoding(zf, nom)
            with zf.open(nom) as f:
                for raw in pd.read_csv(f, dtype=str, chunksize=CHUNK, encoding=enc):
                    raw.columns = [c.strip() for c in raw.columns]
                    faltan = set(COLS_ORIGEN) - set(raw.columns)
                    if faltan:
                        raise ValueError(f"Columnas faltantes en el archivo: {sorted(faltan)}")
                    h, rech, est = transformar(raw, d)
                    if len(rech):
                        rech.to_sql("rechazados", con, if_exists="append", index=False)
                    if len(h):
                        dims = dims_del_chunk(h)
                        for tabla in DIMS:
                            resolver_dimension(con, tabla, dims[tabla], cache)
                        cargar_hechos(con, h, d, cache)
                    con.commit()
                    for k in tot:
                        tot[k] += est[k]
                    for k, v in est["motivos"].items():
                        motivos[k] = motivos.get(k, 0) + v
        if tot["leidas"] != tot["cargadas"] + tot["rechazadas"] + tot["duplicadas"]:
            raise ValueError(f"No concilia: {tot}")
        con.execute('''UPDATE etl_control SET estado='OK', filas_leidas=?, filas_cargadas=?, filas_rechazadas=?,
                       filas_duplicadas=?, saldo_incoherente=?, saldo_negativo=?, fuera_de_fecha=?, fin=?, mensaje=?
                       WHERE archivo=?''',
                    (tot["leidas"], tot["cargadas"], tot["rechazadas"], tot["duplicadas"], tot["saldo_incoherente"],
                     tot["saldo_negativo"], tot["fuera_de_fecha"], datetime.now().isoformat(timespec="seconds"),
                     str(motivos), archivo))
        con.commit()
        print(f"{d} | leídas {tot['leidas']:>9,} | cargadas {tot['cargadas']:>9,} | rechazadas {tot['rechazadas']:>6,} "
              f"| duplicadas {tot['duplicadas']:>5,} | saldo incoh. {tot['saldo_incoherente']:>6,} | {time.time() - t0:5.0f} s")
    except Exception as e:
        con.rollback()
        con.execute("UPDATE etl_control SET estado='ERROR', fin=?, mensaje=? WHERE archivo=?",
                    (datetime.now().isoformat(timespec="seconds"), str(e)[:500], archivo))
        con.commit()
        print(f"{d} | ERROR: {e}")
        raise

cache = {t: leer_cache(con, t) for t in DIMS}
for d in DIAS:
    procesar_dia(con, d, cache)

t0 = time.time()
for sk in SK.values():
    con.execute(f"CREATE INDEX IF NOT EXISTS ix_fact_{sk} ON fact_validaciones({sk})")
con.execute("CREATE INDEX IF NOT EXISTS ix_fact_archivo ON fact_validaciones(archivo_fecha)")
con.commit()
print(f"Índices creados en {time.time() - t0:.0f} s")

print("Resumen por archivo:")
display(pd.read_sql('''SELECT fecha_archivo, estado, filas_leidas, filas_cargadas, filas_rechazadas, filas_duplicadas,
                       saldo_incoherente, saldo_negativo, fuera_de_fecha FROM etl_control ORDER BY fecha_archivo''', con))

print("\nMotivos de rechazo:")
display(pd.read_sql("SELECT motivo, COUNT(*) AS filas FROM rechazados GROUP BY motivo", con))

print("\nIntegridad referencial (debe ser 0 en todas):")
huerfanas = {}
for tabla, sk in SK.items():
    huerfanas[tabla] = con.execute(
        f"SELECT COUNT(*) FROM fact_validaciones f LEFT JOIN {tabla} d ON f.{sk}=d.{sk} WHERE d.{sk} IS NULL").fetchone()[0]
display(pd.Series(huerfanas, name="FK huérfanas"))

print("\nFilas por tabla:")
display(pd.Series({t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                   for t in list(DIMS) + ["fact_validaciones", "rechazados"]}, name="filas"))
