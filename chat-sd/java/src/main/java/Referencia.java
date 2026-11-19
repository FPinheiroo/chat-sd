import chat.InfoServidor;
import chat.Msg;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Referencia {
    static final long LIMITE = 30000;
    static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    static int relogio = 0;
    static int proximo = 1;
    static Map<String, Integer> ranks = new HashMap<>();
    static Map<String, Long> vistos = new HashMap<>();

    static void log(String direcao, Msg m) {
        String conteudo = m.toString().replaceAll("\\s+", " ").trim();
        System.out.println("[" + LocalTime.now().format(HORA) + "] referencia " + direcao + " " + conteudo);
    }

    static Msg.Builder nova(String tipo) {
        relogio++;
        return Msg.newBuilder().setTipo(tipo).setDe("referencia")
                .setTimestamp(System.currentTimeMillis() / 1000.0).setRelogio(relogio);
    }

    static int registrar(String nome) {
        Integer r = ranks.get(nome);
        if (r == null) {
            r = proximo++;
            ranks.put(nome, r);
        }
        vistos.put(nome, System.currentTimeMillis());
        return r;
    }

    static void limpar() {
        long agora = System.currentTimeMillis();
        vistos.entrySet().removeIf(e -> {
            boolean vencido = agora - e.getValue() > LIMITE;
            if (vencido) System.out.println("referencia: " + e.getKey() + " removido da lista (sem heartbeat)");
            return vencido;
        });
    }

    public static void main(String[] args) throws Exception {
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket rep = ctx.createSocket(SocketType.REP);
            rep.bind("tcp://*:5559");
            System.out.println("referencia: escutando na porta 5559");

            while (!Thread.currentThread().isInterrupted()) {
                byte[] dados = rep.recv();
                if (dados == null) continue;
                Msg req = Msg.parseFrom(dados);
                relogio = Math.max(relogio, req.getRelogio());
                log("RECEBIDO", req);
                limpar();

                Msg resp;
                String nome = req.getDe();
                if (nome.isEmpty()) {
                    resp = nova(req.getTipo()).setStatus("erro").setErro("nome vazio").build();
                } else if (req.getTipo().equals("rank")) {
                    resp = nova("rank").setRank(registrar(nome)).build();
                } else if (req.getTipo().equals("heartbeat")) {
                    registrar(nome);
                    resp = nova("heartbeat").setStatus("OK").build();
                } else if (req.getTipo().equals("list")) {
                    List<String> nomes = new ArrayList<>(vistos.keySet());
                    nomes.sort(Comparator.comparingInt(ranks::get));
                    Msg.Builder b = nova("list").setStatus("ok");
                    for (String n : nomes) {
                        b.addServidores(InfoServidor.newBuilder().setNome(n).setRank(ranks.get(n)));
                    }
                    resp = b.build();
                } else {
                    resp = nova(req.getTipo()).setStatus("erro").setErro("tipo desconhecido").build();
                }

                rep.send(resp.toByteArray());
                log("ENVIADO", resp);
            }
        }
    }
}
