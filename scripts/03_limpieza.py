"""
Etapa 3 - Limpieza
Lee la capa cruda (olist_ingesta) y escribe una version limpia en olist_limpieza:
  - elimina registros duplicados
  - elimina/normaliza campos vacios
  - corrige fechas (string -> datetime real)
  - convierte precios (string -> float)
  - elimina espacios en textos
  - normaliza nomenclatura de ciudades/estado (Title Case + codigo BR-XX tipo ISO 3166-2)
  - valida datos (elimina o corrige valores negativos)
"""
import os
import sys
import time
from datetime import datetime

import pandas as pd
from pymongo.errors import AutoReconnect, CursorNotFound

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.db import get_db, DB_INGESTA, DB_LIMPIEZA

BATCH_SIZE = 25000
MAX_INTENTOS = 5


# ---------- helpers ----------

def clean_str(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def clean_city(value):
    value = clean_str(value)
    if value is None:
        return None
    return " ".join(word.capitalize() for word in value.split())


def clean_state_code(value):
    value = clean_str(value)
    if value is None:
        return None
    return f"BR-{value.upper()}"


def to_float(value, default=None):
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def to_int(value, default=None):
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def to_datetime(value):
    if value is None or str(value).strip() == "":
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    dt = parsed.to_pydatetime()
    # descarta fechas evidentemente corruptas (ej. anos fuera de rango del dataset)
    if dt.year < 2000 or dt.year > 2025:
        return None
    return dt


def non_negative(value):
    """Si el valor es negativo lo trata como invalido (None)."""
    if value is None:
        return None
    return value if value >= 0 else None


def load_ingesta_df(db, collection_name):
    for intento in range(1, MAX_INTENTOS + 1):
        try:
            docs = list(db[collection_name].find({}, {"_id": 0}, batch_size=BATCH_SIZE))
            return pd.DataFrame(docs)
        except (AutoReconnect, CursorNotFound) as e:
            if intento == MAX_INTENTOS:
                raise
            print(f"  [aviso] se corto la lectura de '{collection_name}' ({e}); reintentando ({intento}/{MAX_INTENTOS})...")
            time.sleep(2 * intento)


def _sanitizar_nulos(records):
    # pandas convierte None a NaT/NaN al normalizar columnas de fecha o numericas;
    # Mongo no sabe serializar esos sentinels, asi que se devuelven a None.
    return [
        {k: (None if pd.isna(v) else v) for k, v in rec.items()}
        for rec in records
    ]


def write_limpieza(db, collection_name, df):
    collection = db[collection_name]
    collection.drop()
    if df.empty:
        print(f"  {collection_name}: 0 documentos (vacio)")
        return 0
    records = _sanitizar_nulos(df.to_dict(orient="records"))
    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        collection.insert_many(batch)
        total += len(batch)
    print(f"  {collection_name}: {total} documentos limpios")
    return total


# ---------- limpieza por dataset ----------

def limpiar_customers(db_in, db_out):
    df = load_ingesta_df(db_in, "customers")
    db_in["customers"].drop()  # libera espacio: el free tier de Atlas no alcanza para tener ambas copias
    df = df.drop_duplicates()
    df = df.dropna(subset=["customer_id"])
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    df["customer_city"] = df["customer_city"].apply(clean_city)
    df["customer_state"] = df["customer_state"].apply(lambda v: clean_str(v).upper() if clean_str(v) else None)
    df["customer_location_code"] = df["customer_state"].apply(clean_state_code)
    df["customer_zip_code_prefix"] = df["customer_zip_code_prefix"].apply(clean_str)
    write_limpieza(db_out, "customers", df)


def limpiar_sellers(db_in, db_out):
    df = load_ingesta_df(db_in, "sellers")
    db_in["sellers"].drop()
    df = df.drop_duplicates()
    df = df.dropna(subset=["seller_id"])
    df = df.drop_duplicates(subset=["seller_id"], keep="first")
    df["seller_city"] = df["seller_city"].apply(clean_city)
    df["seller_state"] = df["seller_state"].apply(lambda v: clean_str(v).upper() if clean_str(v) else None)
    df["seller_location_code"] = df["seller_state"].apply(clean_state_code)
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].apply(clean_str)
    write_limpieza(db_out, "sellers", df)


def limpiar_orders(db_in, db_out):
    df = load_ingesta_df(db_in, "orders")
    db_in["orders"].drop()
    df = df.drop_duplicates()
    df = df.dropna(subset=["order_id", "customer_id"])
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    df["order_status"] = df["order_status"].apply(lambda v: clean_str(v).lower() if clean_str(v) else None)
    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        df[col] = df[col].apply(to_datetime)
    # nota: approved_at / delivered_customer_date pueden quedar en None de forma legitima
    # (pedidos cancelados o aun no entregados), no se eliminan esas filas por eso.
    write_limpieza(db_out, "orders", df)


def limpiar_order_items(db_in, db_out):
    df = load_ingesta_df(db_in, "order_items")
    db_in["order_items"].drop()
    df = df.drop_duplicates()
    df = df.dropna(subset=["order_id", "product_id", "seller_id"])
    df["order_item_id"] = df["order_item_id"].apply(to_int)
    df["price"] = df["price"].apply(to_float).apply(non_negative)
    df["freight_value"] = df["freight_value"].apply(to_float).apply(non_negative)
    df["shipping_limit_date"] = df["shipping_limit_date"].apply(to_datetime)
    antes = len(df)
    df = df.dropna(subset=["price"])  # precio invalido/negativo -> registro no confiable
    print(f"  order_items: {antes - len(df)} filas descartadas por precio invalido/negativo")
    write_limpieza(db_out, "order_items", df)


def limpiar_payments(db_in, db_out):
    df = load_ingesta_df(db_in, "payments")
    db_in["payments"].drop()
    df = df.drop_duplicates()
    df = df.dropna(subset=["order_id"])
    df["payment_type"] = df["payment_type"].apply(lambda v: clean_str(v).lower() if clean_str(v) else None)
    df["payment_sequential"] = df["payment_sequential"].apply(to_int)
    df["payment_installments"] = df["payment_installments"].apply(to_int).apply(non_negative)
    df["payment_value"] = df["payment_value"].apply(to_float).apply(non_negative)
    antes = len(df)
    df = df.dropna(subset=["payment_value"])
    print(f"  payments: {antes - len(df)} filas descartadas por monto invalido/negativo")
    write_limpieza(db_out, "payments", df)


def limpiar_reviews(db_in, db_out):
    df = load_ingesta_df(db_in, "reviews")
    db_in["reviews"].drop()
    df = df.drop_duplicates()
    df = df.dropna(subset=["review_id", "order_id"])
    df = df.drop_duplicates(subset=["review_id"], keep="first")
    df["review_score"] = df["review_score"].apply(to_int)
    df["review_score"] = df["review_score"].apply(lambda v: v if v is not None and 1 <= v <= 5 else None)
    df["review_comment_title"] = df["review_comment_title"].apply(clean_str)
    df["review_comment_message"] = df["review_comment_message"].apply(clean_str)
    df["review_creation_date"] = df["review_creation_date"].apply(to_datetime)
    df["review_answer_timestamp"] = df["review_answer_timestamp"].apply(to_datetime)
    write_limpieza(db_out, "reviews", df)


def limpiar_products(db_in, db_out, categorias_map):
    df = load_ingesta_df(db_in, "products")
    db_in["products"].drop()
    df = df.drop_duplicates()
    df = df.dropna(subset=["product_id"])
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    df["product_category_name"] = df["product_category_name"].apply(clean_str).fillna("sem_categoria")
    df["product_category_name_en"] = df["product_category_name"].map(categorias_map).fillna("unknown")
    for col in [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]:
        df[col] = df[col].apply(to_int).apply(non_negative)
    write_limpieza(db_out, "products", df)


def limpiar_product_category_translation(db_in, db_out):
    df = load_ingesta_df(db_in, "product_category_translation")
    db_in["product_category_translation"].drop()
    df = df.drop_duplicates()
    df["product_category_name"] = df["product_category_name"].apply(clean_str)
    df["product_category_name_english"] = df["product_category_name_english"].apply(clean_str)
    write_limpieza(db_out, "product_category_translation", df)
    return dict(zip(df["product_category_name"], df["product_category_name_english"]))


def limpiar_geolocation(db_in, db_out):
    df = load_ingesta_df(db_in, "geolocation")
    db_in["geolocation"].drop()
    df["geolocation_zip_code_prefix"] = df["geolocation_zip_code_prefix"].apply(clean_str)
    df["geolocation_lat"] = df["geolocation_lat"].apply(to_float)
    df["geolocation_lng"] = df["geolocation_lng"].apply(to_float)
    df["geolocation_city"] = df["geolocation_city"].apply(clean_city)
    df["geolocation_state"] = df["geolocation_state"].apply(lambda v: clean_str(v).upper() if clean_str(v) else None)
    df = df.dropna(subset=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])

    antes = len(df)
    # el dataset original trae ~1M filas con muchas coordenadas repetidas para el
    # mismo codigo postal; se colapsan a una fila por codigo postal (promedio de
    # lat/lng + ciudad/estado mas frecuente). Esto es la deduplicacion real de
    # esta tabla y de paso reduce mucho el tamano de la coleccion.
    agg = (
        df.groupby("geolocation_zip_code_prefix")
        .agg(
            geolocation_lat=("geolocation_lat", "mean"),
            geolocation_lng=("geolocation_lng", "mean"),
            geolocation_city=("geolocation_city", lambda s: s.mode().iat[0] if not s.mode().empty else None),
            geolocation_state=("geolocation_state", lambda s: s.mode().iat[0] if not s.mode().empty else None),
        )
        .reset_index()
    )
    agg["geolocation_location_code"] = agg["geolocation_state"].apply(clean_state_code)
    print(f"  geolocation: {antes} filas crudas -> {len(agg)} codigos postales unicos")
    write_limpieza(db_out, "geolocation", agg)


def main():
    db_in = get_db(DB_INGESTA)
    db_out = get_db(DB_LIMPIEZA)
    print(f"Limpiando '{DB_INGESTA}' -> '{DB_LIMPIEZA}'")

    limpiar_customers(db_in, db_out)
    limpiar_sellers(db_in, db_out)
    limpiar_orders(db_in, db_out)
    limpiar_order_items(db_in, db_out)
    limpiar_payments(db_in, db_out)
    limpiar_reviews(db_in, db_out)
    categorias_map = limpiar_product_category_translation(db_in, db_out)
    limpiar_products(db_in, db_out, categorias_map)
    limpiar_geolocation(db_in, db_out)

    print("\nLimpieza completa.")


if __name__ == "__main__":
    main()
