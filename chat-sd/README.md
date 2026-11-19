# Sistema de troca de mensagens (BBS/IRC simplificado)

Projeto de Sistemas Distribuídos. Bots publicam em canais públicos, servidores guardam tudo em disco
e todos os servidores mantêm uma cópia completa dos dados. Tudo roda com `docker compose up`.

## Como executar

```
docker compose up --build
```

Os logs de clientes e servidores mostram cada mensagem enviada e recebida. Os dados de cada servidor
ficam em `dados/<nome-do-servidor>.tsv`.

## Componentes

| Serviço | Linguagem | Função |
|---|---|---|
| `broker` | Python | ROUTER (5555, clientes) e DEALER (5556, servidores), balanceamento round-robin |
| `proxy` | JavaScript | Proxy Pub/Sub: XSUB na 5557 e XPUB na 5558 |
| `referencia` | Java | Rank dos servidores, lista de servidores ativos e heartbeat (porta 5559) |
| `servidor-*` | Python, JavaScript, Java | 2 de cada, mesma lógica nas 3 linguagens |
| `bot-*` | Python, JavaScript, Java | 2 de cada, mesma lógica nas 3 linguagens |

Total: 6 servidores e 6 bots, dois de cada linguagem, todos trocando mensagens entre si.

## Escolhas

**Linguagens.** Python, Java e JavaScript (uma por integrante). Cada integrante fica com o cliente e o servidor
da sua linguagem. Broker, proxy e serviço de referência ficaram uma em cada linguagem.

**Serialização.** Protocol Buffers. Existe um único arquivo, `proto/chat.proto`, usado pelas três linguagens
(compilado nos Dockerfiles de Python e Java, carregado em tempo de execução no JavaScript). Todas as mensagens
usam a mesma estrutura `Msg`, com o campo `tipo` indicando a operação, além de `timestamp` (hora do envio)
e `relogio` (relógio lógico) em todas elas.

**Persistência.** Cada servidor grava em um arquivo próprio (`dados/<nome>.tsv`), uma linha por evento, separada
por tabulação: tipo (`login`, `canal` ou `publicacao`), usuário, canal, texto, timestamp, servidor de origem e
relógio lógico. Ao reiniciar, o servidor lê o arquivo de volta para a memória. Escolhemos texto simples porque
só o próprio servidor lê o arquivo e assim é fácil conferir o conteúdo.

## Mensagens

- Login: `login` -> `login` com `status` ok/erro. Nome válido: letras, números, `-` e `_`, até 32 caracteres. Em caso de erro o bot tenta de novo.
- Criar canal: `canal` -> `canal` com `status`. Erros: nome inválido, nome reservado (`servers`, `replica`) ou canal já existente.
- Listar canais: `listar` -> `listar` com a lista de nomes.
- Publicar: `publicar` (canal, texto) -> o servidor publica no tópico com o nome do canal e responde `publicar` com `status`.
- Serviço de referência: `rank` (devolve o rank), `list` (nomes e ranks) e `heartbeat` (responde OK).
- Entre servidores (porta 6000 de cada um): `eleicao` (responde OK), `relogio` (devolve a hora) e `sync` (devolve todos os registros).
- Tópicos Pub/Sub dos servidores: `servers` (anúncio do coordenador) e `replica` (replicação).

O rank é sequencial, na ordem em que os servidores se registram, e é mantido se o servidor reiniciar.
O endereço de cada servidor é `tcp://<nome>:6000`, e o nome é o do serviço no compose (o DNS do Docker resolve).

## Bots

Ao conectar, cada bot faz login e entra em um loop. Em cada volta do loop:

1. lista os canais; se houver menos de 5, cria um novo;
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (escolhido aleatoriamente);
3. escolhe um canal da lista e envia 10 mensagens aleatórias, uma por segundo.

Decisão nossa: os passos 1 e 2 são repetidos a cada volta (e não só uma vez), assim o sistema chega a 5 canais e cada bot a 3 inscrições
mesmo que o primeiro bot a subir encontre tudo vazio. Toda mensagem recebida por inscrição é impressa com canal, usuário,
texto, hora de envio, hora de recebimento e relógio lógico.

## Relógios

**Lógico.** Cada processo tem um contador. Ele é incrementado antes de cada envio e vai na mensagem. Ao receber, o contador
passa a ser o máximo entre o valor local e o da mensagem.

**Físico, eleição e sincronização.** Os servidores têm um coordenador. A eleição segue o algoritmo do valentão (Bully):
o servidor que inicia a eleição pergunta aos servidores de rank maior; se algum responde OK, ele desiste
(e quem respondeu inicia a própria eleição). Se ninguém responde, ele se declara coordenador e publica o nome no tópico `servers`.
Uma eleição acontece quando o servidor inicia e quando o coordenador não responde ao pedido de relógio.

A cada 15 mensagens de clientes, o servidor pede a hora ao coordenador (`relogio`) e ajusta o próprio relógio com a hora recebida
mais metade do tempo de ida e volta. Seguimos o diagrama do enunciado (o servidor pede, o coordenador responde),
ou seja, uma versão simplificada do Berkeley em que a hora do coordenador é a referência. O coordenador não ajusta o próprio relógio.

**Heartbeat.** A cada 10 mensagens de clientes, o servidor envia `heartbeat` ao serviço de referência, que não devolve mais a hora.
O serviço remove da lista o servidor que ficar mais de 30 segundos sem heartbeat. Esse tempo não foi definido no enunciado.
O servidor removido volta para a lista (com o mesmo rank) quando enviar o próximo heartbeat.

## Replicação

Método escolhido: **replicação ativa por difusão das escritas**. Como o broker distribui os pedidos em round-robin,
cada servidor só vê parte deles. Então, toda vez que um servidor grava um evento (login, canal ou publicação),
ele também publica o registro no tópico `replica`. Todos os servidores assinam esse tópico e aplicam os registros que vêm dos outros,
ignorando os próprios. Aplicar é idempotente (um registro repetido é descartado), então receber duas vezes não causa problema.

Como isso resolve o problema: depois de propagadas as escritas, todos os servidores têm os mesmos dados e a lista de canais
é a mesma em qualquer servidor, não importa para qual o broker mande o pedido. Se um servidor cair, nada se perde.

Mudanças que tivemos que fazer em relação ao método puro:

- **Servidor novo ou reiniciado.** Ele perderia o que foi publicado enquanto estava fora. Por isso, ao iniciar, ele pede o histórico completo
  (`sync`) a outro servidor ativo e aplica o que faltava.
- Não há ordenação total nem confirmação de entrega: a consistência é eventual. Como o Pub/Sub do ZeroMQ não garante entrega,
  uma mensagem de replicação pode se perder se um servidor estiver desconectado naquele instante. Nesse caso ele só recupera o dado no próximo `sync` ao reiniciar.

## Limitações conhecidas

- Os bots esperam 5 segundos antes de começar, para dar tempo de os servidores se registrarem e assinarem os tópicos.
- A lista de servidores é por tempo: um servidor que fique mais de 30 s sem receber mensagens de clientes sai da lista até o próximo heartbeat.
- Um pedido que estava com um servidor no momento em que ele caiu se perde; o bot espera 5 s e reenvia.
