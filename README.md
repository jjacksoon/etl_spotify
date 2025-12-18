# 🎧 Spotify Data Pipeline — OAuth & API Ingestion

Este projeto implementa um pipeline de ingestão de dados utilizando a API do Spotify, com autenticação via OAuth 2.0 (Authorization Code Flow) e organização modular do código.

O foco está na extração estruturada de dados, separação de responsabilidades e preparação para evolução do pipeline.

---

## 📌 Visão Geral

O pipeline realiza:

- Autenticação com OAuth 2.0  
- Extração de dados da API do Spotify  
- Manipulação de respostas em formato JSON  
- Organização do código em camadas (`auth`, `extract`, `pipeline`)  
- Base para persistência e transformações futuras  

---

## 🧱 Estrutura do Projeto

```
etl_spotify/
├── .env
├── token.json
├── README.md
├── src/
│   ├── auth/
│   │   ├── app.py
│   │   └── oauth_client.py
│   ├── extract/
│   │   └── user_top_artists.py
│   └── pipeline.py
└── .venv/
```

---

## 🔐 Autenticação (OAuth 2.0)

A autenticação utiliza o Authorization Code Flow, padrão adotado por APIs modernas.

Fluxo:

1. Usuário acessa o endpoint de login  
2. É redirecionado para autenticação no Spotify  
3. A API retorna um authorization code  
4. O backend troca o código por access token e refresh token  
5. Os tokens são persistidos localmente  

---

## ⚙️ Configuração do Ambiente

### Arquivo `.env`

Na raiz do projeto:

```
SPOTIFY_CLIENT_ID=seu_client_id
SPOTIFY_CLIENT_SECRET=seu_client_secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8000/callback
SPOTIFY_SCOPE=user-top-read
```

O `REDIRECT_URI` deve ser idêntico ao configurado no Spotify Developer Dashboard.

---

## 🧪 Ambiente Virtual

Criar o ambiente virtual:

```
python -m venv .venv
```

Ativar:

Linux / Mac:
```
source .venv/bin/activate
```

Windows:
```
.venv\Scripts\activate
```

---

## 📦 Instalação das Dependências

```
pip install flask requests python-dotenv
```

---

## ▶️ Execução da Autenticação

A partir da pasta `src/auth`:

```
python app.py
```

Acesse:

```
http://127.0.0.1:8000
```

Após a autenticação, o arquivo `token.json` será criado.

---

## 📥 Extração de Dados

Endpoint utilizado:

```
GET /v1/me/top/artists
```

Implementação:

```
src/extract/user_top_artists.py
```

O retorno da API é um JSON contendo, entre outros campos:

- Nome do artista  
- Popularidade  
- Número de seguidores  
- Gêneros musicais  

---

## 🧠 Organização do Código

- `auth/` — autenticação OAuth  
- `extract/` — ingestão de dados  
- `pipeline.py` — orquestração do fluxo  

Essa separação facilita manutenção e evolução do projeto.

---

## 🚀 Possíveis Extensões

- Conversão dos dados para DataFrames  
- Persistência em banco de dados ou data lake  
- Camada de transformação  
- Refresh automático de token  
- Agendamento do pipeline  
- Versionamento histórico  

---

## 👤 Autor

Jackson Nascimento
🔗 LinkedIn: https://www.linkedin.com/in/jackson10/
