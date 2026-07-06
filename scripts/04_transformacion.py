"""
Etapa 4 - Transformacion
Lee la capa limpia (olist_limpieza) y genera en olist_transformacion:
  - pedidos_clientes: orders unido con customers
  - pedidos_productos: order_items agrupado por (order, product) con
    cantidad y total_precio = precio_unitario * cantidad, unido con products
  - ventas: tabla ancha (pedidos_productos + pedidos_clientes + sellers) que
    sirve de base para las agregaciones de la etapa 5
"""
import os
import sys
import time

import pandas as pd
from pymongo.errors import AutoReconnect, CursorNotFound

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.db import get_db, DB_LIMPIEZA, DB_TRANSFORMACION

BATCH_SIZE = 25000
MAX_INTENTOS = 5


def load_df(db, collection_name):
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
    # NaN/NaT quedan tras los merge (how="left") o al re-parsear fechas;
    # Mongo no sabe serializar esos sentinels, asi que se devuelven a None.
    return [
        {k: (None if pd.isna(v) else v) for k, v in rec.items()}
        for rec in records
    ]


def write_collection(db, collection_name, df):
    collection = db[collection_name]
    collection.drop()
    if df.empty:
        print(f"  {collection_name}: 0 documentos (vacio)")
        return
    records = _sanitizar_nulos(df.to_dict(orient="records"))
    for i in range(0, len(records), BATCH_SIZE):
        collection.insert_many(records[i : i + BATCH_SIZE])
    print(f"  {collection_name}: {len(records)} documentos")


def main():
    db_in = get_db(DB_LIMPIEZA)
    db_out = get_db(DB_TRANSFORMACION)
    print(f"Transformando '{DB_LIMPIEZA}' -> '{DB_TRANSFORMACION}'")

    orders = load_df(db_in, "orders")
    customers = load_df(db_in, "customers")
    order_items = load_df(db_in, "order_items")
    products = load_df(db_in, "products")
    sellers = load_df(db_in, "sellers")

    # -- a. Unir pedidos con clientes --
    pedidos_clientes = orders.merge(customers, on="customer_id", how="left")
    write_collection(db_out, "pedidos_clientes", pedidos_clientes)

    # -- b. Unir orders con productos + c. calcular precio * cantidad --
    # cantidad = numero de unidades del mismo producto dentro del mismo pedido
    agg_items = (
        order_items.groupby(["order_id", "product_id", "seller_id"])
        .agg(
            cantidad=("order_item_id", "count"),
            precio_unitario=("price", "mean"),
            freight_total=("freight_value", "sum"),
        )
        .reset_index()
    )
    agg_items["total_precio"] = agg_items["precio_unitario"] * agg_items["cantidad"]

    pedidos_productos = agg_items.merge(products, on="product_id", how="left")
    write_collection(db_out, "pedidos_productos", pedidos_productos)

    # -- tabla ancha "ventas" para agregaciones: producto + pedido + cliente + vendedor --
    ventas = pedidos_productos.merge(
        pedidos_clientes[
            [
                "order_id",
                "customer_id",
                "customer_unique_id",
                "customer_city",
                "customer_state",
                "customer_location_code",
                "order_status",
                "order_purchase_timestamp",
            ]
        ],
        on="order_id",
        how="left",
    )
    ventas = ventas.merge(
        sellers[["seller_id", "seller_city", "seller_state", "seller_location_code"]],
        on="seller_id",
        how="left",
    )

    ventas["order_purchase_timestamp"] = pd.to_datetime(ventas["order_purchase_timestamp"], errors="coerce")
    ventas["anio_mes"] = ventas["order_purchase_timestamp"].dt.strftime("%Y-%m")

    write_collection(db_out, "ventas", ventas)

    print("\nTransformacion completa.")


if __name__ == "__main__":
    main()
