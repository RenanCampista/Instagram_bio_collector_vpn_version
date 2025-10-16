# Instagram Bio Collector
Coletor de bio de perfis do Instagram.


## Instalação e Configuração
1. **Crie um ambiente virtual para instalar as dependências:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
2. **Instale as dependências necessárias:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure as variáveis de ambiente:**
   Coloque o arquivo `.env` na raiz do projeto com as seguintes variáveis de ambiente definidas.
   
4. **Configure as credenciais da VPN:**
   Coloque o arquivo `protonvpn_credentials.txt` e/ou `nordvpn_credentials.txt` na raiz do projeto com as credenciais da VPN.

5. **Instale o OpenVPN se ainda não estiver instalado:**
   ```bash
   sudo apt install openvpn
   ```

## Uso
1. **Execute o script principal:**
    ```bash
    python main.py vpn
    ```

Onde "vpn" pode ser "protonvpn" ou "nordvpn" dependendo de qual VPN você deseja usar.
Exemplo:
```bash
python main.py protonvpn
```