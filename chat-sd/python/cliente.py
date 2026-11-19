import os
import random
import time
from datetime import datetime

import zmq

import chat_pb2 as pb

NOME = os.environ["NOME"]
BROKER = os.environ.get("BROKER", "tcp://broker:5555")
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy:5558")

FRASES = [
    "bom dia pessoal",
    "alguem viu o resultado?",
    "teste de mensagem",
    "sistemas distribuidos e legal",
    "mais uma mensagem aleatoria",
    "ola canal",
    "subindo mais um bot",
    "tudo certo por aqui",
]

ctx = zmq.Context()
sub = ctx.socket(zmq.SUB)
relogio = 0
inscritos = []


def hora(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]


def log(direcao, msg):
    conteudo = " ".join(str(msg).split())
    print("[%s] %s %s %s" % (time.strftime("%H:%M:%S"), NOME, direcao, conteudo), flush=True)


def nova(tipo, **campos):
    global relogio
    relogio += 1
    return pb.Msg(tipo=tipo, de=NOME, usuario=NOME, timestamp=time.time(), relogio=relogio, **campos)


def receber_relogio(m):
    global relogio
    relogio = max(relogio, m.relogio)


def requisitar(msg):
    while True:
        s = ctx.socket(zmq.REQ)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(BROKER)
        log("ENVIADO", msg)
        s.send(msg.SerializeToString())
        if s.poll(5000):
            resp = pb.Msg()
            resp.ParseFromString(s.recv())
            s.close()
            receber_relogio(resp)
            log("RECEBIDO", resp)
            return resp
        s.close()
        print("[%s] sem resposta do broker, tentando de novo" % NOME, flush=True)


def mostrar(topico, dados):
    recebido = time.time()
    m = pb.Msg()
    m.ParseFromString(dados)
    receber_relogio(m)
    print('[%s] RECEBIDO canal=%s usuario=%s texto="%s" enviado=%s recebido=%s relogio=%d'
          % (NOME, topico.decode(), m.usuario, m.texto, hora(m.timestamp), hora(recebido), m.relogio),
          flush=True)


def esperar(segundos):
    fim = time.time() + segundos
    while True:
        resta = fim - time.time()
        if resta <= 0:
            return
        if sub.poll(int(resta * 1000)):
            topico, dados = sub.recv_multipart()
            mostrar(topico, dados)


def listar():
    return list(requisitar(nova("listar")).canais)


def main():
    time.sleep(5)
    sub.connect(PROXY_SUB)

    while True:
        r = requisitar(nova("login"))
        if r.status == "ok":
            break
        print("[%s] login recusado: %s" % (NOME, r.erro), flush=True)
        esperar(1)

    while True:
        canais = listar()
        if len(canais) < 5:
            requisitar(nova("canal", canal="canal-%04d" % random.randint(0, 9999)))
            canais = listar()

        if len(inscritos) < 3:
            livres = [c for c in canais if c not in inscritos]
            if livres:
                c = random.choice(livres)
                sub.setsockopt(zmq.SUBSCRIBE, c.encode())
                inscritos.append(c)
                print("[%s] inscrito no canal %s" % (NOME, c), flush=True)

        if not canais:
            esperar(1)
            continue

        canal = random.choice(canais)
        for i in range(10):
            texto = "%s (%d/10)" % (random.choice(FRASES), i + 1)
            requisitar(nova("publicar", canal=canal, texto=texto))
            esperar(1)


main()
