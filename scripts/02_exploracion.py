"""
Etapa 2 - Exploracion
Corre consultas de ejemplo sobre la capa de ingesta (raw) para conocer los datos
antes de limpiarlos: conteos, muestras, valores distintos, nulos, etc.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.db import get_db, DB_INGESTA


def main():
    db = get_db(DB_INGESTA)

    print("== Conteo de documentos por coleccion ==")
    for name in db.list_collection_names():
        print(f"  {name}: {db[name].count_documents({})}")

    print("\n== db.orders.find().limit(10) ==")
    for doc in db.orders.find().limit(10):
        print(" ", {k: doc.get(k) for k in ("order_id", "order_status", "order_purchase_timestamp")})

    print("\n== db.customers.countDocuments() ==")
    print(" ", db.customers.count_documents({}))

    print("\n== db.products.find() (muestra de 5) ==")
    for doc in db.products.find().limit(5):
        print(" ", {k: doc.get(k) for k in ("product_id", "product_category_name")})

    print("\n== Valores distintos de order_status ==")
    print(" ", db.orders.distinct("order_status"))

    print("\n== Ciudades distintas en customers (muestra de 10) ==")
    print(" ", db.customers.distinct("customer_city")[:10])

    print("\n== Pedidos con campos de entrega vacios (nulos legitimos) ==")
    print("  order_approved_at nulo:", db.orders.count_documents({"order_approved_at": None}))
    print("  order_delivered_customer_date nulo:", db.orders.count_documents({"order_delivered_customer_date": None}))

    print("\n== Productos sin categoria ==")
    print(" ", db.products.count_documents({"product_category_name": None}))


if __name__ == "__main__":
    main()
