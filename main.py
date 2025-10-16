import logging
import time
import random
import os
import argparse

from instaloader import Instaloader, Profile
from dotenv import load_dotenv
from pymongo import UpdateOne

from src.utils import connect_to_mongodb, get_profiles_from_db, setup_logging, send_pending_updates
from src.api_db_client import ApiDbClient
from src.vpn_handler import VpnHandler


log = logging.getLogger(__name__)
log = setup_logging("logs/bio_collector_instaloader", "bio_collector")


def load_env_variables():
    load_dotenv()
    config = {
        "MONGO_CONNECTION_STRING": os.getenv("MONGO_CONNECTION_STRING"),
        "MONGO_DB": os.getenv("MONGO_DB"),
        "MONGO_COLLECTION": os.getenv("MONGO_COLLECTION"),
        "API_ROUTE": os.getenv("API_ROUTE"),
        "SECRET_TOKEN": os.getenv("SECRET_TOKEN"),
    }
    return config


def main():
    """Main function to collect Instagram profile data and send it to an API."""
    config = load_env_variables()
    
    parser = argparse.ArgumentParser(description="Instagram Bio Collector with VPN")
    parser.add_argument("vpn_service", choices=["protonvpn", "nordvpn"], help="VPN service to use")
    args = parser.parse_args()

    vpn_dir = f"vpn_files/{args.vpn_service}"
    credentials_file = f"{args.vpn_service}_credentials.txt"
    
    
    L = Instaloader()
    L.context.sleep = True # Enable built-in sleep to handle rate limits
    
    api_client = ApiDbClient(config["API_ROUTE"], config["SECRET_TOKEN"], log)
    
    try:
        client = connect_to_mongodb(config["MONGO_CONNECTION_STRING"], log)
        database = client[config["MONGO_DB"]]
        collection = database[config["MONGO_COLLECTION"]]

        vpn = VpnHandler(vpn_dir, credentials_file, log)
        vpn.load_server_list()
        request_count = 0
        vpn_connected = False
        
        # Lista para acumular atualizações em batch
        pending_updates = []
        
        while True:
            profiles = get_profiles_from_db(collection, log, 100)
            
            if not profiles:
                log.info("Não há mais perfis para processar. Encerrando o script.")
                break
            
            # Conectar VPN apenas antes de começar a coletar perfis do Instagram
            if not vpn_connected:
                vpn.connect_to_next_server()
                vpn_connected = True
            
            for profile in profiles:
                time.sleep(random.uniform(2, 5)) # Sleep aleatório entre 2 e 5 segundos
                
                if request_count >= 120:
                    log.info("Atingidas 120 requisições. Trocando servidor VPN...")
                    vpn.disconnect()
                    vpn_connected = False
                    
                    log.info("Aguardando 5 segundos antes de reconectar...")
                    time.sleep(5)
                    
                    send_pending_updates(collection, pending_updates, log)

                    vpn.connect_to_next_server()
                    vpn_connected = True
                    request_count = 0
                    
                try:
                    log.info(f"Coletando dados do perfil: {profile}")
                    profile_data = Profile.from_username(L.context, profile.strip())
                    request_count += 1
                except Exception as e:
                    log.error(f"Erro ao coletar dados do perfil {profile}: {e}")
                    # Adicionar atualização ao batch
                    if "Please wait a few minutes before you try again." in str(e):
                        request_count += 30  # Penalidade maior para erros de rate limit
                        pending_updates.append(
                            UpdateOne(
                                {"username": profile},
                                {
                                    "$set": {"status": "not_collected"},
                                    "$currentDate": {"updated_at": True}
                                }
                            )
                        )
                    else:
                        pending_updates.append(
                            UpdateOne(
                                {"username": profile},
                                {
                                    "$set": {"status": "error"},
                                    "$currentDate": {"updated_at": True}
                                }
                            )
                        )
                    continue
                
                log.info(f"Dados coletados para o perfil: {profile}. Enviando para a API.")
                
                data = {
                    "username": profile_data.username,
                    "full_name": profile_data.full_name,
                    "profile_url": f"https://www.instagram.com/{profile_data.username}/",
                    "userid": profile_data.userid,
                    "biography": profile_data.biography,
                    "external_url": profile_data.external_url,
                    "followers": profile_data.followers,
                    "following": profile_data.followees,
                }
                
                if api_client.send_json(data):
                    log.info(f"Dados enviados com sucesso para o perfil: {profile}.")
                    # Adicionar atualização ao batch
                    pending_updates.append(
                        UpdateOne(
                            {"username": profile},
                            {
                                "$set": {"status": "collected"},
                                "$currentDate": {"updated_at": True}
                            }
                        )
                    )
                else:
                    log.error(f"Falha ao enviar dados para o perfil: {profile}.")
                    # Adicionar atualização ao batch
                    pending_updates.append(
                        UpdateOne(
                            {"username": profile},
                            {
                                "$set": {"status": "error"},
                                "$currentDate": {"updated_at": True}
                            }
                        )
                    ) 
    except Exception as e:
        log.error(f"Erro geral no script: {e}")
    finally:
        log.info("Encerrando script...")
        try:
            if vpn_connected:
                vpn.disconnect()
                log.info("VPN desconectada.")
        except Exception as vpn_error:
            log.error(f"Erro ao desconectar VPN: {vpn_error}")
        
        try:
            send_pending_updates(collection, pending_updates, log)
        except Exception as update_error:
            log.error(f"Erro ao enviar atualizações finais: {update_error}")

        try:
            client.close()
            log.info("Conexão MongoDB fechada.")
        except Exception as db_error:
            log.error(f"Erro ao fechar conexão MongoDB: {db_error}")
            
            
if __name__ == "__main__":
    main()
    