const fs = require("fs");
const util = require("util");
const zmq = require("zeromq");
const { codificar, decodificar } = require("./comum");

const NOME = process.env.NOME;
const BROKER = process.env.BROKER || "tcp://broker:5556";
const PROXY_PUB = process.env.PROXY_PUB || "tcp://proxy:5557";
const PROXY_SUB = process.env.PROXY_SUB || "tcp://proxy:5558";
const REFERENCIA = process.env.REFERENCIA || "tcp://referencia:5559";
const PORTA_PAR = 6000;
const ARQUIVO = `/dados/${NOME}.tsv`;
const RESERVADOS = ["servers", "replica"];
const VALIDO = /^[A-Za-z0-9_-]{1,32}$/;

let pub;
let relogio = 0;
let offset = 0;
let rank = 0;
let coordenador = null;
let total = 0;
let emEleicao = false;
const canais = [];
const registros = [];
const chaves = new Set();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const agora = () => Date.now() / 1000 + offset;

function log(direcao, m) {
  const limpo = {};
  for (const [k, v] of Object.entries(m)) {
    if (v === "" || v === 0 || v === null || (Array.isArray(v) && v.length === 0)) continue;
    limpo[k] = k === "registros" ? `${v.length} registros` : v;
  }
  const texto = util.inspect(limpo, { breakLength: Infinity, depth: 4 });
  console.log(`[${new Date().toTimeString().slice(0, 8)}] ${NOME} ${direcao} ${texto}`);
}

function nova(tipo, extra = {}) {
  relogio++;
  return { tipo, de: NOME, timestamp: agora(), relogio, ...extra };
}

function receberRelogio(m) {
  relogio = Math.max(relogio, m.relogio);
}

async function pedir(endereco, msg, tempo = 2000) {
  const s = new zmq.Request({ sendTimeout: tempo, receiveTimeout: tempo, linger: 0 });
  s.connect(endereco);
  try {
    log("ENVIADO -> " + endereco, msg);
    await s.send(codificar(msg));
    const [dados] = await s.receive();
    const resp = decodificar(dados);
    receberRelogio(resp);
    log("RECEBIDO <- " + endereco, resp);
    return resp;
  } catch (e) {
    return null;
  } finally {
    s.close();
  }
}

function adicionar(r, gravar = true) {
  const k = [r.tipo, r.usuario, r.canal, r.texto, r.timestamp, r.servidor].join("|");
  if (chaves.has(k)) return;
  chaves.add(k);
  registros.push(r);
  if (r.tipo === "canal" && !canais.includes(r.canal)) canais.push(r.canal);
  if (gravar) {
    const linha = [r.tipo, r.usuario, r.canal, r.texto, r.timestamp, r.servidor, r.relogio].join("\t") + "\n";
    fs.appendFileSync(ARQUIVO, linha);
  }
}

function carregar() {
  if (!fs.existsSync(ARQUIVO)) return;
  for (const linha of fs.readFileSync(ARQUIVO, "utf8").split("\n")) {
    const p = linha.split("\t");
    if (p.length !== 7) continue;
    adicionar(
      { tipo: p[0], usuario: p[1], canal: p[2], texto: p[3], timestamp: Number(p[4]), servidor: p[5], relogio: Number(p[6]) },
      false
    );
  }
  console.log(`${NOME}: ${registros.length} registros carregados do disco`);
}

async function guardarEReplicar(r) {
  adicionar(r);
  const m = nova("replica", { registro: r });
  await pub.send(["replica", codificar(m)]);
  log("ENVIADO(PUB replica)", m);
}

async function tratarCliente(req) {
  if (req.tipo === "login") {
    if (!VALIDO.test(req.usuario)) return nova("login", { status: "erro", erro: "nome de usuario invalido" });
    await guardarEReplicar({ tipo: "login", usuario: req.usuario, timestamp: agora(), servidor: NOME, relogio });
    return nova("login", { status: "ok" });
  }

  if (req.tipo === "canal") {
    if (!VALIDO.test(req.canal) || RESERVADOS.includes(req.canal)) {
      return nova("canal", { status: "erro", erro: "nome de canal invalido" });
    }
    if (canais.includes(req.canal)) return nova("canal", { status: "erro", erro: "canal ja existe" });
    await guardarEReplicar({ tipo: "canal", usuario: req.usuario, canal: req.canal, timestamp: agora(), servidor: NOME, relogio });
    return nova("canal", { status: "ok" });
  }

  if (req.tipo === "listar") {
    return nova("listar", { status: "ok", canais });
  }

  if (req.tipo === "publicar") {
    if (!canais.includes(req.canal)) return nova("publicar", { status: "erro", erro: "canal nao existe" });
    if (!req.texto) return nova("publicar", { status: "erro", erro: "mensagem vazia" });
    const texto = req.texto.replace(/[\t\n]/g, " ");
    const p = nova("publicacao", { usuario: req.usuario, canal: req.canal, texto });
    await pub.send([req.canal, codificar(p)]);
    log(`ENVIADO(PUB ${req.canal})`, p);
    await guardarEReplicar({
      tipo: "publicacao",
      usuario: req.usuario,
      canal: req.canal,
      texto,
      timestamp: p.timestamp,
      servidor: NOME,
      relogio: p.relogio,
    });
    return nova("publicar", { status: "ok" });
  }

  return nova("erro", { status: "erro", erro: "tipo desconhecido" });
}

async function listarServidores() {
  const r = await pedir(REFERENCIA, nova("list"));
  return r ? r.servidores : [];
}

async function buscarDados() {
  for (const s of await listarServidores()) {
    if (s.nome === NOME) continue;
    const resp = await pedir(`tcp://${s.nome}:${PORTA_PAR}`, nova("sync"), 5000);
    if (resp) {
      for (const r of resp.registros) adicionar(r);
      console.log(`${NOME}: dados copiados de ${s.nome} (${registros.length} registros)`);
      return;
    }
  }
}

async function eleicao() {
  if (emEleicao) return;
  emEleicao = true;
  try {
    let respondeu = false;
    for (const s of await listarServidores()) {
      if (s.rank > rank) {
        const r = await pedir(`tcp://${s.nome}:${PORTA_PAR}`, nova("eleicao"), 1000);
        if (r && r.status === "OK") respondeu = true;
      }
    }
    if (respondeu) {
      if (coordenador === NOME) coordenador = null;
      return;
    }
    coordenador = NOME;
    const anuncio = nova("coordenador", { coordenador: NOME });
    await pub.send(["servers", codificar(anuncio)]);
    log("ENVIADO(PUB servers)", anuncio);
  } finally {
    emEleicao = false;
  }
}

async function sincronizar() {
  if (coordenador === null) {
    await eleicao();
    return;
  }
  if (coordenador === NOME) return;
  const inicio = Date.now() / 1000;
  const r = await pedir(`tcp://${coordenador}:${PORTA_PAR}`, nova("relogio"), 2000);
  const fim = Date.now() / 1000;
  if (!r) {
    console.log(`${NOME}: coordenador ${coordenador} nao respondeu, iniciando eleicao`);
    coordenador = null;
    await eleicao();
    return;
  }
  offset = r.hora + (fim - inicio) / 2 - fim;
  console.log(`${NOME}: relogio ajustado com ${coordenador}, offset = ${offset.toFixed(6)} s`);
}

async function heartbeat() {
  const r = await pedir(REFERENCIA, nova("heartbeat"));
  if (!r) console.log(`${NOME}: referencia nao respondeu ao heartbeat`);
}

async function iniciar() {
  carregar();
  while (true) {
    const r = await pedir(REFERENCIA, nova("rank"), 3000);
    if (r) {
      rank = r.rank;
      break;
    }
    await sleep(1000);
  }
  console.log(`${NOME}: rank ${rank}`);
  await buscarDados();
  await eleicao();
}

async function ouvirBroker(back) {
  for await (const [dados] of back) {
    const req = decodificar(dados);
    receberRelogio(req);
    log("RECEBIDO", req);
    let resp;
    try {
      resp = await tratarCliente(req);
    } catch (e) {
      resp = nova("erro", { status: "erro", erro: "falha interna" });
    }
    await back.send(codificar(resp));
    log("ENVIADO", resp);
    total++;
    if (total % 10 === 0) await heartbeat();
    if (total % 15 === 0) await sincronizar();
  }
}

async function ouvirPar(par) {
  for await (const [dados] of par) {
    const req = decodificar(dados);
    receberRelogio(req);
    log("RECEBIDO(par)", req);
    let resp;
    if (req.tipo === "eleicao") resp = nova("eleicao", { status: "OK" });
    else if (req.tipo === "relogio") resp = nova("relogio", { hora: agora() });
    else if (req.tipo === "sync") resp = nova("sync", { registros });
    else resp = nova("erro", { status: "erro", erro: "tipo desconhecido" });
    await par.send(codificar(resp));
    log("ENVIADO(par)", resp);
    if (req.tipo === "eleicao") eleicao();
  }
}

async function ouvirSub(sub) {
  for await (const [topico, dados] of sub) {
    const m = decodificar(dados);
    receberRelogio(m);
    const t = topico.toString();
    if (t === "servers") {
      coordenador = m.coordenador;
      log("RECEBIDO(SUB servers)", m);
    } else if (t === "replica" && m.de !== NOME) {
      adicionar(m.registro);
      log("RECEBIDO(SUB replica)", m);
    }
  }
}

async function main() {
  fs.mkdirSync("/dados", { recursive: true });

  const par = new zmq.Reply();
  await par.bind(`tcp://*:${PORTA_PAR}`);
  pub = new zmq.Publisher();
  pub.connect(PROXY_PUB);
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_SUB);
  sub.subscribe("servers", "replica");

  ouvirPar(par);
  ouvirSub(sub);
  await sleep(1000);

  await iniciar();

  const back = new zmq.Reply();
  back.connect(BROKER);
  await ouvirBroker(back);
}

main();
