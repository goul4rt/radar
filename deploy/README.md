# Radar no ar (VPS + Cloudflare)

Tudo roda em Docker numa VPS. Quem recebe tráfego é só o **oauth2-proxy** (login Google). Radar, RSSHub e Redis
ficam na rede interna, sem porta aberta.

```
internet ─► Cloudflare (HTTPS) ─► tunnel ─► oauth2-proxy :4180 ─► radar :1210 ─► rsshub :1200 ─► redis
                                             login Google          nichos, API,     raspagem      cache
                                             (exceto /p, /api,     MCP, coleta
                                              /mcp, /privacidade)
```

**Por que não Cloudflare Workers (wrangler):** o Radar precisa de processo contínuo (coleta a cada 30 min) e o RSSHub
é um servidor Node com Redis; nenhum dos dois roda num Worker sem reescrever tudo (D1, Cron Triggers, sem RSSHub).
A Cloudflare entra como **Tunnel + DNS + HTTPS** na frente da VPS, o que dispensa abrir portas no Contabo.

Recursos: 1 vCPU e 2 GB de RAM bastam (o RSSHub usa ~100–200 MB; o Radar, ~50 MB). Qualquer VPS do Contabo serve.

## 1. VPS

```sh
ssh root@IP_DA_VPS
curl -fsSL https://get.docker.com | sh
ufw allow OpenSSH && ufw --force enable        # com o tunnel não precisa abrir 80/443
mkdir -p /opt/radar
```

## 2. Copiar o código e o banco (do seu Mac)

```sh
rsync -av ~/workspace/radar/radar.py ~/workspace/radar/deploy root@IP_DA_VPS:/opt/radar/
ssh root@IP_DA_VPS 'mkdir -p /opt/radar/deploy/dados'
# banco atual (nicho anime, seus itens). Pare o Radar local antes, ou copie um backup consistente:
sqlite3 ~/workspace/radar/dados/radar.db ".backup /tmp/radar.db"
scp /tmp/radar.db root@IP_DA_VPS:/opt/radar/deploy/dados/radar.db
ssh root@IP_DA_VPS 'chown -R 10001 /opt/radar/deploy/dados'   # o container roda como o usuário 10001
```

## 3. Login com Google

1. https://console.cloud.google.com → crie um projeto (ex.: "Radar").
2. **APIs e serviços → Tela de consentimento OAuth**: tipo *Externo*, nome "Radar", seu e-mail de suporte.
   Escopos: só os básicos (e-mail e perfil). Depois **Publicar app**: enquanto estiver em "Teste", só os e-mails
   cadastrados como testadores conseguem entrar. Escopos básicos não exigem verificação do Google.
3. **Credenciais → Criar credenciais → ID do cliente OAuth → Aplicativo da Web.**
   URI de redirecionamento autorizado: `https://radar.SEUDOMINIO/oauth2/callback`.
4. Guarde o *Client ID* e o *Client secret*.

## 4. Cloudflare Tunnel

O domínio precisa estar na Cloudflare (o plano grátis serve).

1. https://one.dash.cloudflare.com → **Networks → Tunnels → Create a tunnel → Cloudflared**, nome "radar".
2. Copie o **token** (o comando mostrado termina em `--token eyJ...`; só a parte do token).
3. **Public Hostname**: subdomínio `radar`, domínio `SEUDOMINIO`, serviço `HTTP` → `oauth2-proxy:4180`.

Sem Cloudflare: aponte o DNS `radar.SEUDOMINIO` para o IP da VPS, abra as portas (`ufw allow 80,443/tcp`) e use o
perfil `caddy` no passo 5 (HTTPS automático via Let's Encrypt).

## 5. Configurar e subir

```sh
cd /opt/radar/deploy
cp .env.example .env && chmod 600 .env
python3 -c 'import secrets,base64;print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'   # COOKIE_SECRET
nano .env      # RADAR_DOMINIO, RADAR_ADMINS, GOOGLE_*, COOKIE_SECRET, CLOUDFLARE_TUNNEL_TOKEN, TWITTER_AUTH_TOKEN (opcional)

docker compose --profile cloudflare up -d --build      # ou: --profile caddy
docker compose ps
```

Teste:
- `https://radar.SEUDOMINIO/` (apresentação) e `/p` (nichos públicos) abrem sem login.
- `https://radar.SEUDOMINIO/nichos` leva ao login Google; depois de entrar, você vê o nicho `anime` como seu.
- **Conta → Gerar nova chave** para usar a API e o MCP.

## 6. Backup diário

```sh
crontab -e
15 4 * * * /opt/radar/deploy/backup.sh      # guarda 7 dias em /opt/radar/deploy/backups
```

## 7. Usar

- **Shinbun (no Mac):** `RADAR_URL=https://radar.SEUDOMINIO python3 triagem.py`. O nicho `anime` é público, então não
  precisa de chave; se ficar privado, use também `RADAR_CHAVE=rk_...`.
- **MCP em agentes:**
  `claude mcp add --transport http radar https://radar.SEUDOMINIO/mcp --header "Authorization: Bearer rk_..."`
  No LiteLLM (gateway de MCP), registre o mesmo endereço com o cabeçalho; aí os modelos grátis também usam o Radar.
- **Outra pessoa:** entra com o Google, cria os nichos dela (privados) ou copia um público.

## 8. Atualizar

```sh
rsync -av ~/workspace/radar/radar.py root@IP_DA_VPS:/opt/radar/
ssh root@IP_DA_VPS 'cd /opt/radar/deploy && docker compose --profile cloudflare up -d --build radar'
```

## Segurança

O modelo completo de proteção está no [README da raiz](../README.md#segurança).

### Regra de ouro

- O Radar confia no cabeçalho `X-Forwarded-Email` **só** porque não tem porta publicada: todo tráfego passa pelo
  oauth2-proxy, que exige login nas rotas privadas e apaga esse cabeçalho nas rotas sem login. Nunca publique a
  porta 1210 nem rode o Radar com `RADAR_AUTH=proxy` exposto direto.
- API e MCP aceitam só a chave do próprio Radar (`Authorization: Bearer rk_...`); guardamos apenas o hash.
- O RSSHub interno (host e porta exatos) é a única exceção à regra de endereço público.
- Rotas do RSSHub que usam credencial do servidor (X, Instagram) só podem ser cadastradas por `RADAR_ADMINS`.
- "Testar fonte" e a coleta recusam endereços internos (rede privada, localhost, metadados da nuvem), inclusive
  em redirecionamentos e na hora de conectar: o IP checado é o mesmo usado na conexão, o que fecha o DNS rebinding.
- Limites por pessoa: `RADAR_LIMITE_NICHOS` (10) e `RADAR_LIMITE_FONTES` (100); "coletar agora" 1x por minuto;
  "testar fonte" `RADAR_LIMITE_TESTES` (10) por minuto por conta. Copiar nicho também respeita o limite de fontes.
- **Conta nova** começa com `RADAR_LIMITE_NICHOS_NOVO` (2) e `RADAR_LIMITE_FONTES_NOVO` (20). Para liberar alguém:
  `docker exec radar-app python3 radar.py --liberar email@dela.com` (a pessoa precisa ter entrado uma vez).
- LGPD: página `/privacidade`, exportação do nicho em `.toml` e "Apagar conta" (apaga tudo da pessoa na hora).

## Borda Cloudflare (opcional)

`cloudflare-borda/` é um Worker que fica na frente do servidor: cacheia as páginas públicas por 60 s, `/static/` por
30 dias e limita o resto a 120 requisições por minuto por IP. Copie `wrangler.example.toml` para `wrangler.toml`,
ajuste o domínio e `ORIGEM` (um registro DNS que já aponta para a VPS) e rode `npx wrangler deploy` na pasta.

Liberar uma conta nova dos limites iniciais: `docker exec radar-app python3 radar.py --liberar email@da.pessoa`.
