import os
import re
import time

import zmq

import chat_pb2 as pb

NOME = os.environ["NOME"]
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY_PUB = os.environ.get("PROXY_PUB", "tcp://proxy:5557")
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy:5558")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")
PORTA_PAR = 6000
ARQUIVO = "/dados/" + NOME + ".tsv"
RESERVADOS = ("servers", "replica")
VALIDO = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

ctx = zmq.Context()
pub = ctx.socket(zmq.PUB)

relogio = 0
offset = 0.0
rank = 0
coordenador = None
canais = []
registros = []
chaves = set()
total = 0
eleicao_pendente = False


def agora():
    return time.time() + offset


def nova(tipo, **campos):
    global relogio
    relogio += 1
    return pb.Msg(tipo=tipo, de=NOME, timestamp=agora(), relogio=relogio, **campos)


def receber_relogio(m):
    global relogio
    relogio = max(relogio, m.relogio)


def log(direcao, msg):
    if msg.tipo == "sync" and msg.registros:
        conteudo = 'tipo: "sync" de: "%s" relogio: %d registros: %d' % (msg.de, msg.relogio, len(msg.registros))
    else:
        conteudo = " ".join(str(msg).split())
    print("[%s] %s %s %s" % (time.strftime("%H:%M:%S"), NOME, direcao, conteudo), flush=True)


def pedir(endereco, msg, tempo=2000):
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(endereco)
    try:
        log("ENVIADO -> " + endereco, msg)
        s.send(msg.SerializeToString())
        if s.poll(tempo) == 0:
            return None
        resp = pb.Msg()
        resp.ParseFromString(s.recv())
        receber_relogio(resp)
        log("RECEBIDO <- " + endereco, resp)
        return resp
    finally:
        s.close()


def adicionar(r, gravar=True):
    k = (r.tipo, r.usuario, r.canal, r.texto, r.timestamp, r.servidor)
    if k in chaves:
        return
    chaves.add(k)
    registros.append(r)
    if r.tipo == "canal" and r.canal not in canais:
        canais.append(r.canal)
    if gravar:
        campos = [r.tipo, r.usuario, r.canal, r.texto, repr(r.timestamp), r.servidor, str(r.relogio)]
        with open(ARQUIVO, "a", encoding="utf-8") as f:
            f.write("\t".join(campos) + "\n")


def carregar():
    if not os.path.exists(ARQUIVO):
        return
    with open(ARQUIVO, encoding="utf-8") as f:
        for linha in f:
            p = linha.rstrip("\n").split("\t")
            if len(p) != 7:
                continue
            r = pb.Registro(tipo=p[0], usuario=p[1], canal=p[2], texto=p[3],
                            timestamp=float(p[4]), servidor=p[5], relogio=int(p[6]))
            adicionar(r, False)
    print("%s: %d registros carregados do disco" % (NOME, len(registros)), flush=True)


def guardar_e_replicar(r):
    adicionar(r)
    m = nova("replica", registro=r)
    pub.send_multipart([b"replica", m.SerializeToString()])
    log("ENVIADO(PUB replica)", m)


def tratar_cliente(req):
    if req.tipo == "login":
        if not VALIDO.match(req.usuario):
            return nova("login", status="erro", erro="nome de usuario invalido")
        guardar_e_replicar(pb.Registro(tipo="login", usuario=req.usuario, timestamp=agora(),
                                       servidor=NOME, relogio=relogio))
        return nova("login", status="ok")

    if req.tipo == "canal":
        if not VALIDO.match(req.canal) or req.canal in RESERVADOS:
            return nova("canal", status="erro", erro="nome de canal invalido")
        if req.canal in canais:
            return nova("canal", status="erro", erro="canal ja existe")
        guardar_e_replicar(pb.Registro(tipo="canal", usuario=req.usuario, canal=req.canal,
                                       timestamp=agora(), servidor=NOME, relogio=relogio))
        return nova("canal", status="ok")

    if req.tipo == "listar":
        return nova("listar", status="ok", canais=canais)

    if req.tipo == "publicar":
        if req.canal not in canais:
            return nova("publicar", status="erro", erro="canal nao existe")
        if not req.texto:
            return nova("publicar", status="erro", erro="mensagem vazia")
        texto = req.texto.replace("\t", " ").replace("\n", " ")
        p = nova("publicacao", usuario=req.usuario, canal=req.canal, texto=texto)
        pub.send_multipart([req.canal.encode(), p.SerializeToString()])
        log("ENVIADO(PUB " + req.canal + ")", p)
        guardar_e_replicar(pb.Registro(tipo="publicacao", usuario=req.usuario, canal=req.canal,
                                       texto=texto, timestamp=p.timestamp, servidor=NOME,
                                       relogio=p.relogio))
        return nova("publicar", status="ok")

    return nova("erro", status="erro", erro="tipo desconhecido")


def tratar_par(req):
    global eleicao_pendente
    if req.tipo == "eleicao":
        eleicao_pendente = True
        return nova("eleicao", status="OK")
    if req.tipo == "relogio":
        return nova("relogio", hora=agora())
    if req.tipo == "sync":
        return nova("sync", registros=registros)
    return nova("erro", status="erro", erro="tipo desconhecido")


def tratar_sub(topico, dados):
    global coordenador
    m = pb.Msg()
    m.ParseFromString(dados)
    receber_relogio(m)
    if topico == b"servers":
        coordenador = m.coordenador
        log("RECEBIDO(SUB servers)", m)
    elif topico == b"replica" and m.de != NOME:
        adicionar(m.registro)
        log("RECEBIDO(SUB replica)", m)


def listar_servidores():
    r = pedir(REFERENCIA, nova("list"))
    return list(r.servidores) if r is not None else []


def buscar_dados():
    for s in listar_servidores():
        if s.nome == NOME:
            continue
        resp = pedir("tcp://%s:%d" % (s.nome, PORTA_PAR), nova("sync"), 5000)
        if resp is not None:
            for r in resp.registros:
                adicionar(r)
            print("%s: dados copiados de %s (%d registros)" % (NOME, s.nome, len(registros)), flush=True)
            return


def eleicao():
    global coordenador
    respondeu = False
    for s in listar_servidores():
        if s.rank > rank:
            r = pedir("tcp://%s:%d" % (s.nome, PORTA_PAR), nova("eleicao"), 1000)
            if r is not None and r.status == "OK":
                respondeu = True
    if respondeu:
        coordenador = None
        return
    coordenador = NOME
    anuncio = nova("coordenador", coordenador=NOME)
    pub.send_multipart([b"servers", anuncio.SerializeToString()])
    log("ENVIADO(PUB servers)", anuncio)


def sincronizar():
    global offset, coordenador
    if coordenador is None:
        eleicao()
        return
    if coordenador == NOME:
        return
    inicio = time.time()
    r = pedir("tcp://%s:%d" % (coordenador, PORTA_PAR), nova("relogio"), 2000)
    fim = time.time()
    if r is None:
        print("%s: coordenador %s nao respondeu, iniciando eleicao" % (NOME, coordenador), flush=True)
        coordenador = None
        eleicao()
        return
    offset = r.hora + (fim - inicio) / 2 - fim
    print("%s: relogio ajustado com %s, offset = %.6f s" % (NOME, coordenador, offset), flush=True)


def heartbeat():
    r = pedir(REFERENCIA, nova("heartbeat"))
    if r is None:
        print("%s: referencia nao respondeu ao heartbeat" % NOME, flush=True)


def iniciar():
    global rank
    carregar()
    while True:
        r = pedir(REFERENCIA, nova("rank"), 3000)
        if r is not None:
            rank = r.rank
            break
        time.sleep(1)
    print("%s: rank %d" % (NOME, rank), flush=True)
    buscar_dados()
    eleicao()


def main():
    global total, eleicao_pendente

    par = ctx.socket(zmq.REP)
    par.bind("tcp://*:%d" % PORTA_PAR)
    pub.connect(PROXY_PUB)
    sub = ctx.socket(zmq.SUB)
    sub.connect(PROXY_SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"servers")
    sub.setsockopt(zmq.SUBSCRIBE, b"replica")
    time.sleep(1)

    iniciar()

    back = ctx.socket(zmq.REP)
    back.connect(BROKER)

    poller = zmq.Poller()
    poller.register(back, zmq.POLLIN)
    poller.register(par, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)

    while True:
        eventos = dict(poller.poll(1000))

        if back in eventos:
            req = pb.Msg()
            req.ParseFromString(back.recv())
            receber_relogio(req)
            log("RECEBIDO", req)
            resp = tratar_cliente(req)
            back.send(resp.SerializeToString())
            log("ENVIADO", resp)
            total += 1
            if total % 10 == 0:
                heartbeat()
            if total % 15 == 0:
                sincronizar()

        if par in eventos:
            req = pb.Msg()
            req.ParseFromString(par.recv())
            receber_relogio(req)
            log("RECEBIDO(par)", req)
            resp = tratar_par(req)
            par.send(resp.SerializeToString())
            log("ENVIADO(par)", resp)

        if sub in eventos:
            topico, dados = sub.recv_multipart()
            tratar_sub(topico, dados)

        if eleicao_pendente:
            eleicao_pendente = False
            eleicao()


main()
