import logging
from datetime import datetime
import os

from pymongo import MongoClient, errors, UpdateOne
from pymongo.collection import Collection


def setup_logging(log_dir: str, log_name: str) -> logging.Logger:
    """Configures logging with daily log files."""
    os.makedirs(log_dir, exist_ok=True)
    
    # Gerar nome do arquivo com data atual
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{log_name}_{current_date}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding='utf-8')
        ],
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Log iniciado: {log_file}")
    return logger


def send_pending_updates(collection: Collection, pending_updates: list[UpdateOne], log: logging.Logger) -> bool:
    """Envia atualizações pendentes para o MongoDB."""
    if pending_updates:
        try:
            log.info(f"Enviando {len(pending_updates)} atualizações para o MongoDB...")
            result = collection.bulk_write(pending_updates, ordered=False)
            log.info(f"Batch enviado: {result.modified_count} documentos atualizados.")
            pending_updates.clear()
            return True
        except Exception as batch_error:
            log.error(f"Erro ao enviar batch de atualizações: {batch_error}")
            pending_updates.clear()  # Limpar para evitar reenvio
            return False
    return True


def connect_to_mongodb(connection_string: str, log: logging.Logger) -> MongoClient:
    """Connects to a MongoDB database using a given connection string."""
    try:
        log.info("Conectando ao banco de dados MongoDB...")
        client = MongoClient(
            connection_string, 
            serverSelectionTimeoutMS=20000,
            socketTimeoutMS=60000,
            connectTimeoutMS=20000,
            maxPoolSize=10
        )
        client.admin.command('ping')
        log.info("Conectado ao banco de dados MongoDB.")
        return client
    except errors.ServerSelectionTimeoutError as err:
        log.error(f"Erro ao conectar ao banco de dados MongoDB: {err}")
        raise
    

def get_profiles_from_db(collection: Collection, log: logging.Logger, limit: int = 500) -> list[str]:
    """Fetches profiles from the MongoDB collection."""
    try:
        log.info("Buscando perfis do banco de dados...")
        
        # Usar aggregate com $sample para obter perfis aleatórios
        pipeline = [
            {"$match": {"status": "not_collected"}},
            {"$sample": {"size": limit}},
            {"$project": {"username": 1, "_id": 0}}
        ]
        
        cursor = collection.aggregate(
            pipeline,
            maxTimeMS=60000,
            allowDiskUse=True  # Permite usar disco se necessário para operações grandes
        )
        
        profiles = [doc["username"] for doc in cursor if "username" in doc]
        log.info(f"Buscados {len(profiles)} perfis aleatórios do banco de dados.")
        return profiles
    except errors.ExecutionTimeout as e:
        log.error(f"Timeout ao buscar perfis do banco de dados: {e}")
        return []
    except errors.OperationFailure as e:
        log.error(f"Falha na operação do banco de dados: {e}")
        return []
    except Exception as e:
        log.error(f"Erro ao buscar perfis do banco de dados: {e}")
        return []
