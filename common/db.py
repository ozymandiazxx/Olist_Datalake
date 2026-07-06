import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

_uri = os.environ.get("MONGODB_URI")
_client = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        if not _uri:
            raise RuntimeError(
                "MONGODB_URI no esta definido. Crea un archivo .env con MONGODB_URI=<tu connection string>."
            )
        _client = MongoClient(_uri)
    return _client


def get_db(name: str):
    return get_client()[name]


DB_INGESTA = "olist_ingesta"
DB_LIMPIEZA = "olist_limpieza"
DB_TRANSFORMACION = "olist_transformacion"
DB_AGREGACIONES = "olist_agregaciones"
