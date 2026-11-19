import chat.Msg;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Cliente {
    static final String NOME = System.getenv("NOME");
    static final String BROKER = env("BROKER", "tcp://broker:5555");
    static final String PROXY_SUB = env("PROXY_SUB", "tcp://proxy:5558");
    static final String[] FRASES = {
        "bom dia pessoal", "alguem viu o resultado?", "teste de mensagem",
        "sistemas distribuidos e legal", "mais uma mensagem aleatoria",
        "ola canal", "subindo mais um bot", "tudo certo por aqui"
    };
    static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");
    static final DateTimeFormatter HORA_MS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    static ZContext ctx = new ZContext();
    static ZMQ.Socket sub;
    static int relogio = 0;
    static List<String> inscritos = new ArrayList<>();
    static Random rnd = new Random();

    static String env(String nome, String padrao) {
        String v = System.getenv(nome);
        return v == null ? padrao : v;
    }

    static String hora(double ts) {
        return Instant.ofEpochMilli((long) (ts * 1000)).atZone(ZoneId.systemDefault()).toLocalTime().format(HORA_MS);
    }

    static void log(String direcao, Msg m) {
        String conteudo = m.toString().replaceAll("\\s+", " ").trim();
        System.out.println("[" + LocalTime.now().format(HORA) + "] " + NOME + " " + direcao + " " + conteudo);
    }

    static Msg.Builder nova(String tipo) {
        relogio++;
        return Msg.newBuilder().setTipo(tipo).setDe(NOME).setUsuario(NOME)
                .setTimestamp(System.currentTimeMillis() / 1000.0).setRelogio(relogio);
    }

    static void receberRelogio(Msg m) {
        relogio = Math.max(relogio, m.getRelogio());
    }

    static Msg requisitar(Msg msg) throws Exception {
        while (true) {
            ZMQ.Socket s = ctx.createSocket(SocketType.REQ);
            s.setLinger(0);
            s.setReceiveTimeOut(5000);
            s.connect(BROKER);
            log("ENVIADO", msg);
            s.send(msg.toByteArray());
            byte[] dados = s.recv();
            ctx.destroySocket(s);
            if (dados != null) {
                Msg resp = Msg.parseFrom(dados);
                receberRelogio(resp);
                log("RECEBIDO", resp);
                return resp;
            }
            System.out.println("[" + NOME + "] sem resposta do broker, tentando de novo");
        }
    }

    static void mostrar(String topico, byte[] dados) throws Exception {
        double recebido = System.currentTimeMillis() / 1000.0;
        Msg m = Msg.parseFrom(dados);
        receberRelogio(m);
        System.out.println("[" + NOME + "] RECEBIDO canal=" + topico + " usuario=" + m.getUsuario()
                + " texto=\"" + m.getTexto() + "\" enviado=" + hora(m.getTimestamp())
                + " recebido=" + hora(recebido) + " relogio=" + m.getRelogio());
    }

    static void esperar(long ms) throws Exception {
        long fim = System.currentTimeMillis() + ms;
        while (true) {
            long resta = fim - System.currentTimeMillis();
            if (resta <= 0) return;
            sub.setReceiveTimeOut((int) resta);
            String topico = sub.recvStr();
            if (topico == null) return;
            byte[] dados = sub.recv();
            mostrar(topico, dados);
        }
    }

    static List<String> listar() throws Exception {
        return new ArrayList<>(requisitar(nova("listar").build()).getCanaisList());
    }

    public static void main(String[] args) throws Exception {
        Thread.sleep(5000);
        sub = ctx.createSocket(SocketType.SUB);
        sub.connect(PROXY_SUB);

        while (true) {
            Msg r = requisitar(nova("login").build());
            if (r.getStatus().equals("ok")) break;
            System.out.println("[" + NOME + "] login recusado: " + r.getErro());
            esperar(1000);
        }

        while (true) {
            List<String> canais = listar();
            if (canais.size() < 5) {
                String novo = String.format("canal-%04d", rnd.nextInt(10000));
                requisitar(nova("canal").setCanal(novo).build());
                canais = listar();
            }

            if (inscritos.size() < 3) {
                List<String> livres = new ArrayList<>();
                for (String c : canais) {
                    if (!inscritos.contains(c)) livres.add(c);
                }
                if (!livres.isEmpty()) {
                    String c = livres.get(rnd.nextInt(livres.size()));
                    sub.subscribe(c);
                    inscritos.add(c);
                    System.out.println("[" + NOME + "] inscrito no canal " + c);
                }
            }

            if (canais.isEmpty()) {
                esperar(1000);
                continue;
            }

            String canal = canais.get(rnd.nextInt(canais.size()));
            for (int i = 0; i < 10; i++) {
                String texto = FRASES[rnd.nextInt(FRASES.length)] + " (" + (i + 1) + "/10)";
                requisitar(nova("publicar").setCanal(canal).setTexto(texto).build());
                esperar(1000);
            }
        }
    }
}
