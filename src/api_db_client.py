import requests
import logging


class ApiDbClient:
    """Client to interact with a REST API using a secret token for authentication."""
    def __init__(self, route: str, secret_token: str, logger: logging.Logger):
        self.route = route
        self.secret_token = secret_token
        self.headers = {
            'Authorization': f'Bearer {self.secret_token}',
            'Content-Type': 'application/json'
        }
        self.log = logger

    def send_json(self, data: dict) -> bool:
        """Sends JSON data to the API, splitting into batches of 150 items if necessary."""
        try:
            response = requests.post(self.route, json=data, headers=self.headers, timeout=30000)
            if response.status_code == 200:
                resp = response.json()
                resp = resp.get("resposta", "")
                if resp:
                    self.log.info("Resposta da API:")
                    self.log.info(f"\tSucesso: {resp.get('total', 'N/A')}")
                    self.log.info(f"\tErros: {resp.get('erros', 'N/A')}")
                return True
            else:
                self.log.error(f"Erro ao enviar os dados: {response.status_code} - {response.text}")
                return False
        except requests.exceptions.Timeout:
            self.log.error(f"Timeout ao enviar os dados. A API demorou mais de 5 minutos para responder.")
            return False
        except Exception as e:
            self.log.error(f"Erro ao enviar os dados: {e}")
            return False
