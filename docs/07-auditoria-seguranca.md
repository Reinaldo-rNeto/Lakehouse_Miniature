# 07. Auditoria de segurança

Escopo: revisão focada do código e da configuração, usando o roteiro da skill de segurança da
Cloudflare (`cloudflare/security-audit-skill`) como guia. **Não foi o fluxo completo de seis fases com
sub-agentes e validador independente**; para isso, instale a skill e rode no projeto (`REPORT.md`).
Nada foi testado contra a stack em execução.

| # | Achado | Severidade | Situação |
|---|---|---|---|
| 1 | `IBGE_UF` e `IBGE_PERIODOS` entravam na URL sem validação (permitiria alterar caminho ou parâmetros da requisição) | Baixa (valores vêm do seu `.env`) | Corrigido: validação por regex em `lake/config.py`, com teste |
| 2 | Chaves S3 entram num `sed`; caracteres como `&` ou `|` quebrariam ou alterariam o JSON de credenciais | Baixa | Corrigido: o contêiner recusa chaves com algo além de letras e números |
| 3 | Chaves de exemplo usadas sem troca | Média se exposto | Mitigado: o contêiner recusa valores de exemplo; portas só em 127.0.0.1 |
| 4 | Imagens `latest` (SeaweedFS, aws-cli) e comentário dizendo "fixadas" | Baixa (cadeia de suprimentos) | Comentário corrigido; fixe tags para reprodutibilidade |
| 5 | Trino e catálogo REST sem autenticação | Aceito | Só localhost; o catálogo é imagem de teste. Em produção: autenticação, TLS, autorização |
| 6 | Contêiner Spark roda como root com o projeto montado em escrita | Aceito | Escopo de estudo; em produção, usuário sem privilégio e montagem somente leitura do código |
| 7 | Segredos no código ou no repositório | Nenhum encontrado | `.env` está no `.gitignore` e no `.dockerignore`; só o `.env.example` com valores fictícios |
| 8 | `eval`, `exec`, `pickle`, `shell=True`, `verify=False` | Nenhum encontrado | Busca no código Python |

Dados de terceiros: os fixtures de teste usam códigos reais de municípios e valores fictícios; não há
dado pessoal. As APIs do IBGE são públicas.
