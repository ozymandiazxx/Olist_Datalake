"""
Etapa 5 - Agregaciones
Corre pipelines de agregacion de MongoDB sobre olist_transformacion.ventas
y guarda cada resultado como coleccion materializada en olist_agregaciones.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.db import get_db, DB_TRANSFORMACION, DB_AGREGACIONES


def run_and_store(collection, db_out, name, pipeline, top_n_print=10):
    results = list(collection.aggregate(pipeline, allowDiskUse=True))
    db_out[name].drop()
    if results:
        db_out[name].insert_many(results)
    print(f"\n== {name} ({len(results)} filas, mostrando {min(top_n_print, len(results))}) ==")
    for r in results[:top_n_print]:
        print(" ", r)
    return results


def main():
    db = get_db(DB_TRANSFORMACION)
    db_out = get_db(DB_AGREGACIONES)
    ventas = db["ventas"]

    # a. Ventas por ciudad
    # (agrupa primero por pedido para no acumular arrays de order_id en memoria,
    # que excede el limite de agregacion del tier gratuito de Atlas)
    run_and_store(
        ventas,
        db_out,
        "ventas_por_ciudad",
        [
            {
                "$group": {
                    "_id": {"order_id": "$order_id", "ciudad": "$customer_city"},
                    "total_pedido": {"$sum": "$total_precio"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.ciudad",
                    "total_ventas": {"$sum": "$total_pedido"},
                    "num_pedidos": {"$sum": 1},
                }
            },
            {"$project": {"_id": 0, "ciudad": "$_id", "total_ventas": 1, "num_pedidos": 1}},
            {"$sort": {"total_ventas": -1}},
        ],
    )

    # b. Productos mas vendidos
    run_and_store(
        ventas,
        db_out,
        "productos_mas_vendidos",
        [
            {
                "$group": {
                    "_id": {"product_id": "$product_id", "categoria": "$product_category_name_en"},
                    "unidades_vendidas": {"$sum": "$cantidad"},
                    "ingresos": {"$sum": "$total_precio"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "product_id": "$_id.product_id",
                    "categoria": "$_id.categoria",
                    "unidades_vendidas": 1,
                    "ingresos": 1,
                }
            },
            {"$sort": {"unidades_vendidas": -1}},
            {"$limit": 20},
        ],
    )

    # c. Clientes con mas compras
    run_and_store(
        ventas,
        db_out,
        "clientes_con_mas_compras",
        [
            {
                "$group": {
                    "_id": {"order_id": "$order_id", "cliente": "$customer_unique_id"},
                    "total_pedido": {"$sum": "$total_precio"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.cliente",
                    "num_pedidos": {"$sum": 1},
                    "total_gastado": {"$sum": "$total_pedido"},
                }
            },
            {"$project": {"_id": 0, "customer_unique_id": "$_id", "num_pedidos": 1, "total_gastado": 1}},
            {"$sort": {"num_pedidos": -1, "total_gastado": -1}},
            {"$limit": 20},
        ],
    )

    # d. Ventas por mes
    run_and_store(
        ventas,
        db_out,
        "ventas_por_mes",
        [
            {
                "$group": {
                    "_id": {"order_id": "$order_id", "anio_mes": "$anio_mes"},
                    "total_pedido": {"$sum": "$total_precio"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.anio_mes",
                    "total_ventas": {"$sum": "$total_pedido"},
                    "num_pedidos": {"$sum": 1},
                }
            },
            {"$project": {"_id": 0, "anio_mes": "$_id", "total_ventas": 1, "num_pedidos": 1}},
            {"$sort": {"anio_mes": 1}},
        ],
        top_n_print=24,
    )

    # e. Ventas por categoria
    run_and_store(
        ventas,
        db_out,
        "ventas_por_categoria",
        [
            {
                "$group": {
                    "_id": "$product_category_name_en",
                    "total_ventas": {"$sum": "$total_precio"},
                    "unidades": {"$sum": "$cantidad"},
                }
            },
            {"$project": {"_id": 0, "categoria": "$_id", "total_ventas": 1, "unidades": 1}},
            {"$sort": {"total_ventas": -1}},
        ],
    )

    # f. Promedio de compra (ticket promedio por pedido)
    run_and_store(
        ventas,
        db_out,
        "promedio_de_compra",
        [
            {"$group": {"_id": "$order_id", "total_pedido": {"$sum": "$total_precio"}}},
            {
                "$group": {
                    "_id": None,
                    "promedio_compra": {"$avg": "$total_pedido"},
                    "num_pedidos": {"$sum": 1},
                }
            },
            {"$project": {"_id": 0, "promedio_compra": 1, "num_pedidos": 1}},
        ],
    )

    # g. Top vendedores
    # (un pedido puede tener items de varios vendedores, asi que se agrupa por
    # par pedido+vendedor antes de consolidar por vendedor)
    run_and_store(
        ventas,
        db_out,
        "top_vendedores",
        [
            {
                "$group": {
                    "_id": {"order_id": "$order_id", "seller_id": "$seller_id"},
                    "total_pedido": {"$sum": "$total_precio"},
                    "unidades_pedido": {"$sum": "$cantidad"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.seller_id",
                    "total_ventas": {"$sum": "$total_pedido"},
                    "unidades": {"$sum": "$unidades_pedido"},
                    "num_pedidos": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "seller_id": "$_id",
                    "total_ventas": 1,
                    "unidades": 1,
                    "num_pedidos": 1,
                }
            },
            {"$sort": {"total_ventas": -1}},
            {"$limit": 10},
        ],
    )

    # h. Clientes recurrentes - top 10 (2+ pedidos, ordenados por num_pedidos y gasto)
    run_and_store(
        ventas,
        db_out,
        "top10_clientes_recurrentes",
        [
            {
                "$group": {
                    "_id": {"order_id": "$order_id", "cliente": "$customer_unique_id"},
                    "total_pedido": {"$sum": "$total_precio"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.cliente",
                    "num_pedidos": {"$sum": 1},
                    "total_gastado": {"$sum": "$total_pedido"},
                }
            },
            {"$project": {"_id": 0, "customer_unique_id": "$_id", "num_pedidos": 1, "total_gastado": 1}},
            {"$match": {"num_pedidos": {"$gte": 2}}},
            {"$sort": {"num_pedidos": -1, "total_gastado": -1}},
            {"$limit": 10},
        ],
    )

    print("\nAgregaciones completas. Resultados guardados en la base 'olist_agregaciones'.")


if __name__ == "__main__":
    main()
