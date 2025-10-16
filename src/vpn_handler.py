import os
import re
import time
import random
import subprocess
import tempfile
import logging
import signal
from itertools import cycle


class VpnHandler:
    def __init__(self, config_directory: str, credentials_file: str, log: logging.Logger):
        """
        Initialize the VPN handler wit support.
        :param config_directory: Directory containing .ovpn configuration files.
        :param credentials_file: Path to the file containing OpenVPN credentials.
        :param log: Logger instance for logging messages.
        """
        self.config_directory = config_directory
        self.credentials_file = credentials_file
        self.connected_process = None
        self.connected_server = None
        self.remotes_cycle = None
        self.log = log
        self.tmpfile_path = None

        if not os.path.exists(self.credentials_file):
            raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {self.credentials_file}")

    def load_server_list(self):
        """
        Load all available OpenVPN configuration files and create a circular list of remote servers.
        """
        try:
            ovpn_files = [
                os.path.join(self.config_directory, f)
                for f in os.listdir(self.config_directory)
                if f.endswith(".ovpn")
            ]
            
            if not ovpn_files:
                raise FileNotFoundError("Arquivos de configuração dos servidores VPN não encontrados.")
            
            # Extract all remote servers from all .ovpn files
            all_remotes = []
            for ovpn_path in ovpn_files:
                with open(ovpn_path, 'r') as f:
                    for line in f:
                        match = re.match(r'^\s*remote\s+([^\s]+)\s+(\d+)', line)
                        if match:
                            all_remotes.append((ovpn_path, match.group(1), match.group(2)))
            
            if not all_remotes:
                raise ValueError("Nenhum servidor remoto encontrado nos arquivos de configuração.")
            
            # Randomize and create a circular iterator
            random.shuffle(all_remotes)
            self.remotes_cycle = cycle(all_remotes)
            
            self.log.info(f"{len(all_remotes)} servidores VPN carregados de {len(ovpn_files)} arquivos de configuração.")
        except Exception as e:
            self.log.error(f"Erro ao carregar a lista de servidores disponíveis: {e}")
            raise

    def _connect_to_server(self, config_file: str, host: str, port: str) -> bool:
        """
        Connect to a specific VPN server.
        :return: True if connection was successful, False otherwise.
        """
        if not os.path.exists(config_file):
            self.log.error(f"Arquivo de configuração não encontrado: {config_file}")
            return False

        if self.connected_process:
            self.log.warning("Já existe uma conexão VPN ativa.")
            return False

        try:
            # Read and modify the config file
            with open(config_file, 'r') as file:
                config_data = file.readlines()

            # Create temporary config file with specific remote and credentials
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.ovpn') as tmpfile:
                for line in config_data:
                    if line.strip().startswith("remote "):
                        continue
                    elif line.strip().startswith("auth-user-pass"):
                        tmpfile.write(f'auth-user-pass "{self.credentials_file}"\n')
                    else:
                        tmpfile.write(line)
                tmpfile.write(f"remote {host} {port}\n")
                self.tmpfile_path = tmpfile.name

            self.log.info(f"Conectando ao servidor VPN: {host}:{port}")
            
            self.connected_process = subprocess.Popen(
                ["sudo", "openvpn", "--config", self.tmpfile_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                preexec_fn=os.setsid
            )

            start_time = time.time()
            timeout = 30

            while time.time() - start_time < timeout:
                line = self.connected_process.stdout.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                    
                self.log.debug(line.strip())
                print(line.strip())  # Optional: Print output for debugging
                if "Initialization Sequence Completed" in line:
                    self.log.info(f"Conexão VPN estabelecida com sucesso: {host}:{port}")
                    self.connected_server = f"{host}:{port}"
                    return True
                elif "ERROR" in line or "FAILED" in line or "AUTH_FAILED" in line or "Exiting due to fatal error" in line:
                    self.log.error(f"Erro ao conectar ao servidor {host}:{port}: {line.strip()}")
                    self.connected_process.kill()
                    self.connected_process = None
                    return False

            self.log.error(f"Timeout ao conectar ao servidor {host}:{port}")
            if self.connected_process:
                self.connected_process.kill()
                self.connected_process = None
            return False
            
        except Exception as e:
            self.log.error(f"Falha ao conectar ao servidor {host}:{port}: {e}")
            if self.connected_process:
                try:
                    self.connected_process.kill()
                except:
                    pass
                self.connected_process = None
            return False

    def connect_to_next_server(self) -> bool:
        """
        Connect to the next server in the circular list.
        Always returns True since the list is circular and will never run out.
        """
        if not self.remotes_cycle:
            self.log.error("Lista de servidores não carregada. Execute load_server_list() primeiro.")
            return False

        # Get next server from circular list
        config_file, host, port = next(self.remotes_cycle)
        return self._connect_to_server(config_file, host, port)

    def is_connected(self) -> bool:
        """
        Check if VPN is currently connected by verifying the process is alive.
        """
        if not self.connected_process:
            return False
        
        # Check if process is still running
        poll_result = self.connected_process.poll()
        if poll_result is not None:
            self.log.warning(f"Processo VPN terminou inesperadamente com código: {poll_result}")
            self.connected_process = None
            self.connected_server = None
            return False
        
        return True
    
    def disconnect(self):
        """
        Disconnect from the current VPN connection and cleanup resources.
        """
        if self.connected_process:
            try:
                self.log.info(f"Desconectando do servidor VPN: {self.connected_server}")
                
                # Terminate the process group
                try:
                    os.killpg(os.getpgid(self.connected_process.pid), signal.SIGTERM)
                    self.connected_process.wait(timeout=10)
                    self.log.info("VPN terminada com sucesso.")
                except subprocess.TimeoutExpired:
                    self.log.warning("VPN não terminou gracefully, forçando encerramento...")
                    os.killpg(os.getpgid(self.connected_process.pid), signal.SIGKILL)
                except Exception as e:
                    self.log.error(f"Erro ao terminar processo VPN: {e}")
            except Exception as e:
                self.log.error(f"Erro ao desconectar VPN: {e}")
            finally:
                self.connected_process = None
                self.connected_server = None
                
                if self.tmpfile_path and os.path.exists(self.tmpfile_path):
                    try:
                        os.remove(self.tmpfile_path)
                    except Exception as e:
                        self.log.warning(f"Não foi possível remover o arquivo temporário: {e}")
                    self.tmpfile_path = None
        else:
            self.log.warning("Nenhuma conexão VPN ativa para desconectar.")
