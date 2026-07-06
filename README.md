# Olist Data Lake (MongoDB Atlas)

Ejercicio de creacion de un data lake con el dataset publico [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), usando MongoDB Atlas como almacenamiento.

## Dataset

Descarga los CSV del enlace de Kaggle de arriba y colocalos en `data/` (no se incluyen en el repo por tamano):

- olist_customers_dataset.csv
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_order_payments_dataset.csv
- olist_order_reviews_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- olist_geolocation_dataset.csv
- product_category_name_translation.csv

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # y completar MONGODB_URI con tu connection string de Atlas
```

## Pipeline (ejecutar en orden)

| Script | Etapa | Entrada -> Salida |
|---|---|---|
| `scripts/01_ingesta.py` | 1. Ingesta | CSV -> `olist_ingesta` (capa raw) |
| `scripts/02_exploracion.py` | 2. Exploracion | consultas sobre `olist_ingesta` |
| `scripts/03_limpieza.py` | 3. Limpieza | `olist_ingesta` -> `olist_limpieza` |
| `scripts/04_transformacion.py` | 4. Transformacion | `olist_limpieza` -> `olist_transformacion` |
| `scripts/05_agregaciones.py` | 5. Agregaciones | `olist_transformacion` -> `olist_agregaciones` |

```powershell
python scripts\01_ingesta.py
python scripts\02_exploracion.py
python scripts\03_limpieza.py
python scripts\04_transformacion.py
python scripts\05_agregaciones.py
```

## Notas de diseno

- **Ingesta**: cada CSV se sube tal cual (dtype string) para preservar la capa "raw". `geolocation` se limita a una muestra de 20,000 filas (de 1,000,163) porque no se usa en las etapas de transformacion/agregaciones y el tier gratuito de Atlas (M0, 512 MB) no alcanza para conservarla completa junto a las demas capas.
- **Limpieza**: elimina duplicados y campos vacios, corrige fechas y precios, quita espacios, normaliza ciudades (Title Case) y estados a codigo `BR-XX` (ISO 3166-2), y descarta valores negativos. Cada coleccion cruda se libera de `olist_ingesta` apenas se limpia, para no exceder la cuota de almacenamiento del tier gratuito.
- **Transformacion**: `pedidos_clientes` (orders + customers), `pedidos_productos` (order_items agrupado por pedido+producto+vendedor, con `total_precio = precio_unitario * cantidad`, unido con products), y `ventas` (tabla ancha que combina las anteriores, base de las agregaciones).
- **Agregaciones**: 8 pipelines sobre `ventas` (ventas por ciudad, productos mas vendidos, clientes con mas compras, ventas por mes, ventas por categoria, promedio de compra, top vendedores, clientes recurrentes). Las que cuentan pedidos distintos usan una agrupacion en dos pasos (por pedido primero, luego por la dimension) en vez de `$addToSet`, ya que Atlas M0 no permite `allowDiskUse` y el patron de arrays supera el limite de memoria de agregacion.
