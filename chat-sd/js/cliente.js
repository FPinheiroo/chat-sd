const util = require("util");
const zmq = require("zeromq");
const { codificar, decodificar } = require("./comum");

const NOME = process.env.NOME;
const BROKER = process.env.BROKER || "tcp://broker:5555";
const PROXY_SUB = process.env.PROXY_SUB || "tcp://proxy:5558";

const FRASES = [
  "bom dia pessoal",
  "alguem viu o resultado?",
  "teste de mensagem",
  "sistemas distribuidos e legal",
  "mais uma mensagem aleatoria",
  "ola canal",
  "subindo mais um bot",
  "tudo certo por aqui",
];

let relogio = 0;
const inscritos = [];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sorteia = (lista) => lista[Math.floor(Math.random() * lista.length)];

function hora(ts) {
  const d = new Date(ts * 1000);
  return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

function log(direcao, m) {
  const limpo = {};
  for (const [k, v] of Object.entries(m)) {
    if (v === "" || v === 0 || v === null || (Array.isArray(v) && v.length === 0)) continue;
    limpo[k] = v;
  }
  const texto = util.inspect(limpo, { breakLength: Infinity, depth: 4 });
  console.log(`[${new Date().toTimeString().slice(0, 8)}] ${NOME} ${direcao} ${texto}`);
}

function nova(tipo, extra = {}) {
  relogio++;
  return { tipo, de: NOME, usuario: NOME, timestamp: Date.now() / 1000, relogio, ...extra };
}

function receberRelogio(m) {
  relogio = Math.max(relogio, m.relogio);
}

async function requisitar(msg) {
  while (true) {
    const s = new zmq.Request({ sendTimeout: 5000, receiveTimeout: 5000, linger: 0 });
    s.connect(BROKER);
    try {
      log("ENVIADO", msg);
      await s.send(codificar(msg));
      const [dados] = await s.receive();
      const resp = decodificar(dados);
      receberRelogio(resp);
      log("RECEBIDO", resp);
      return resp;
    } catch (e) {
      console.log(`[${NOME}] sem resposta do broker, tentando de novo`);
    } finally {
      s.close();
    }
  }
}

async function ouvir(sub) {
  for await (const [topico, dados] of sub) {
    const recebido = Date.now() / 1000;
    const m = decodificar(dados);
    receberRelogio(m);
    console.log(
      `[${NOME}] RECEBIDO canal=${topico.toString()} usuario=${m.usuario} texto="${m.texto}" ` +
        `enviado=${hora(m.timestamp)} recebido=${hora(recebido)} relogio=${m.relogio}`
    );
  }
}

async function listar() {
  const r = await requisitar(nova("listar"));
  return r.canais;
}

async function main() {
  await sleep(5000);
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_SUB);
  ouvir(sub);

  while (true) {
    const r = await requisitar(nova("login"));
    if (r.status === "ok") break;
    console.log(`[${NOME}] login recusado: ${r.erro}`);
    await sleep(1000);
  }

  while (true) {
    let canais = await listar();
    if (canais.length < 5) {
      const n = String(Math.floor(Math.random() * 10000)).padStart(4, "0");
      await requisitar(nova("canal", { canal: `canal-${n}` }));
      canais = await listar();
    }

    if (inscritos.length < 3) {
      const livres = canais.filter((c) => !inscritos.includes(c));
      if (livres.length > 0) {
        const c = sorteia(livres);
        sub.subscribe(c);
        inscritos.push(c);
        console.log(`[${NOME}] inscrito no canal ${c}`);
      }
    }

    if (canais.length === 0) {
      await sleep(1000);
      continue;
    }

    const canal = sorteia(canais);
    for (let i = 0; i < 10; i++) {
      await requisitar(nova("publicar", { canal, texto: `${sorteia(FRASES)} (${i + 1}/10)` }));
      await sleep(1000);
    }
  }
}

main();
