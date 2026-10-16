// Borda do Radar: o Custom Domain cria o DNS sozinho; o Worker repassa a requisição
// (mesmo Host, mesmo caminho) ao Contabo, resolvendo pelo registro de um subdomínio que já aponta para lá.
// O Caddy do servidor escolhe o site pelo Host e entrega ao oauth2-proxy do Radar.
//
// Proteção barata antes de chegar no servidor:
// - cache na borda das páginas que não dependem de quem está logado (/, /p, /privacidade, /static);
// - limite por IP (binding LIMITE, 120 req/min) para todo o resto: login, app, API e MCP.
const PUBLICAS = /^\/(p(\/[a-z0-9-]+)?|privacidade)?\/?$/;

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const origem = { resolveOverride: env.ORIGEM };  // host que já aponta para o servidor (var no wrangler.toml)
    if (req.method === "GET" && url.pathname.startsWith("/static/")) {
      return fetch(req, { cf: { ...origem, cacheEverything: true, cacheTtl: 2592000 } });
    }
    // ponytail: identidade vem só do oauth2-proxy por cookie; essas rotas ignoram cookie no servidor, então o cache é seguro
    if (req.method === "GET" && PUBLICAS.test(url.pathname) && !req.headers.has("Authorization")) {
      return fetch(req, { cf: { ...origem, cacheEverything: true, cacheTtlByStatus: { "200-299": 60, "404": 30, "500-599": 0 } } });
    }
    const ip = req.headers.get("CF-Connecting-IP") || "?";
    if (env.LIMITE) {
      const { success } = await env.LIMITE.limit({ key: ip });
      if (!success) {
        return new Response("Muitas requisições. Tente de novo em um minuto.", {
          status: 429, headers: { "Retry-After": "60", "Content-Type": "text/plain; charset=utf-8" } });
      }
    }
    return fetch(req, { cf: origem });
  },
};
