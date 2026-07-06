"""
Etapa 1 - Ingesta
Sube los CSV crudos del dataset de Olist a MongoDB tal cual vienen (como texto),
en la base de datos `olist_ingesta`. Esta es la capa "raw" del data lake.
"""
import os
import sys
import math

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.db import get_db, DB_INGESTA

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# nombre de coleccion -> archivo csv
DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}

BATCH_SIZE = 25000

# geolocation trae ~1M filas (192 MB) y no se usa en transformacion/agregaciones;
# se limita a una muestra para no agotar la cuota del tier gratuito de Atlas (512 MB)
# ni saturar la red. No aplica a los demas datasets.
MAX_ROWS = {
    "geolocation": 20000,
}


def load_csv_to_collection(db, collection_name, csv_path):
    collection = db[collection_name]
    collection.drop()

    # todo como string para conservar el dato "crudo" tal cual llega (sin parsear
    # fechas ni precios); eso se resuelve en la etapa de limpieza.
    reader = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=True,
        chunksize=BATCH_SIZE,
        nrows=MAX_ROWS.get(collection_name),
    )

    total = 0
    for chunk in reader:
        docs = chunk.to_dict(orient="records")
        docs = [
            {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in doc.items()}
            for doc in docs
        ]
        if docs:
            collection.insert_many(docs)
            total += len(docs)
            print(f"  {collection_name}: {total} documentos insertados", end="\r")
    print(f"  {collection_name}: {total} documentos insertados (total)")
    return total


def main():
    db = get_db(DB_INGESTA)
    print(f"Conectado a base de datos '{DB_INGESTA}'")

    resumen = {}
    for collection_name, filename in DATASETS.items():
        csv_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(csv_path):
            print(f"[AVISO] No se encontro {csv_path}, se omite.")
            continue
        print(f"Ingiriendo '{filename}' -> coleccion '{collection_name}'...")
        resumen[collection_name] = load_csv_to_collection(db, collection_name, csv_path)

    print("\nResumen de ingesta:")
    for col, count in resumen.items():
        print(f"  - {col}: {count} documentos")


if __name__ == "__main__":
    main()
