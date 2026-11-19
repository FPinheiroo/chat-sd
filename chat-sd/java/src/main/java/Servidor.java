import chat.InfoServidor;
import chat.Msg;
import chat.Registro;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class Servidor {
    static final String NOME = System.getenv("NOME");
    static final String BROKER = env("BROKER", "tcp://broker:5556");
    static final String PROXY_PUB = env("PROXY_PUB", "tcp://proxy:5557");
    static final String PROXY_SUB = env("PROXY_SUB", "tcp://proxy:5558");
    static final String REFERENCIA = env("REFERENCIA", "tcp://referencia:5559");
    static final int PORTA_PAR = 6000;
    static final Path ARQUIVO = Path.of("/dados/" + NOME + ".tsv");
    static final Set<String> RESERVADOS = Set.of("servers", "replica");
    static final Pattern VALIDO = Pattern.compile("^[A-Za-z0-9_-]{1,32}$");
    static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    static ZContext ctx = new ZContext();
    static ZMQ.Socket pub;

    static int relogio = 0;
    static double offset = 0;
    static int rank = 0;
    static String coordenador = null;
    static List<String> canais = new ArrayList<>();
    static List<Registro> registros = new ArrayList<>();
    static Set<String> chaves = new HashSet<>();
    static int total = 0;
    static boolean eleicaoPendente = false;

    static String env(String nome, String padrao) {
        String v = System.getenv(nome);
        return v == null ? padrao : v;
    }

    static double agora() {
        return System.currentTimeMillis() / 1000.0 + offset;
    }

    static Msg.Builder nova(String tipo) {
        relogio++;
        return Msg.newBuilder().setTipo(tipo).setDe(NOME).setTimestamp(agora()).setRelogio(relogio);
    }

    static void receberRelogio(Msg m) {
        relogio = Math.max(relogio, m.getRelogio());
    }

    static void log(String direcao, Msg m) {
        String conteudo;
        if (m.getTipo().equals("sync") && m.getRegistrosCount() > 0) {
            conteudo = "tipo: \"sync\" de: \"" + m.getDe() + "\" relogio: " + m.getRelogio()
                    + " registros: " + m.getRegistrosCount();
        } else {
            conteudo = m.toString().replaceAll("\\s+", " ").trim();
        }
        System.out.println("[" + LocalTime.now().format(HORA) + "] " + NOME + " " + direcao + " " + conteudo);
    }

    static Msg pedir(String endereco, Msg msg, int tempo) {
        ZMQ.Socket s = ctx.createSocket(SocketType.REQ);
        s.setLinger(0);
        s.setReceiveTimeOut(tempo);
        s.setSendTimeOut(tempo);
        s.connect(endereco);
        try {
            log("ENVIADO -> " + endereco, msg);
            s.send(msg.toByteArray());
            byte[] dados = s.recv();
            if (dados == null) return null;
            Msg resp = Msg.parseFrom(dados);
            receberRelogio(resp);
            log("RECEBIDO <- " + endereco, resp);
            return resp;
        } catch (Exception e) {
            return null;
        } finally {
            ctx.destroySocket(s);
        }
    }

    static void adicionar(Registro r, boolean gravar) {
        String k = r.getTipo() + "|" + r.getUsuario() + "|" + r.getCanal() + "|" + r.getTexto()
                + "|" + r.getTimestamp() + "|" + r.getServidor();
        if (!chaves.add(k)) return;
        registros.add(r);
        if (r.getTipo().equals("canal") && !canais.contains(r.getCanal())) canais.add(r.getCanal());
        if (gravar) {
            String linha = String.join("\t", r.getTipo(), r.getUsuario(), r.getCanal(), r.getTexto(),
                    String.valueOf(r.getTimestamp()), r.getServidor(), String.valueOf(r.getRelogio())) + "\n";
            try {
                Files.writeString(ARQUIVO, linha, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (Exception e) {
                System.out.println(NOME + ": erro ao gravar no disco: " + e);
            }
        }
    }

    static void carregar() throws Exception {
        if (!Files.exists(ARQUIVO)) return;
        for (String linha : Files.readAllLines(ARQUIVO, StandardCharsets.UTF_8)) {
            String[] p = linha.split("\t", -1);
            if (p.length != 7) continue;
            Registro r = Registro.newBuilder().setTipo(p[0]).setUsuario(p[1]).setCanal(p[2]).setTexto(p[3])
                    .setTimestamp(Double.parseDouble(p[4])).setServidor(p[5])
                    .setRelogio(Integer.parseInt(p[6])).build();
            adicionar(r, false);
        }
        System.out.println(NOME + ": " + registros.size() + " registros carregados do disco");
    }

    static void guardarEReplicar(Registro r) {
        adicionar(r, true);
        Msg m = nova("replica").setRegistro(r).build();
        pub.sendMore("replica");
        pub.send(m.toByteArray());
        log("ENVIADO(PUB replica)", m);
    }

    static Msg tratarCliente(Msg req) {
        String tipo = req.getTipo();

        if (tipo.equals("login")) {
            if (!VALIDO.matcher(req.getUsuario()).matches()) {
                return nova("login").setStatus("erro").setErro("nome de usuario invalido").build();
            }
            guardarEReplicar(Registro.newBuilder().setTipo("login").setUsuario(req.getUsuario())
                    .setTimestamp(agora()).setServidor(NOME).setRelogio(relogio).build());
            return nova("login").setStatus("ok").build();
        }

        if (tipo.equals("canal")) {
            String c = req.getCanal();
            if (!VALIDO.matcher(c).matches() || RESERVADOS.contains(c)) {
                return nova("canal").setStatus("erro").setErro("nome de canal invalido").build();
            }
            if (canais.contains(c)) {
                return nova("canal").setStatus("erro").setErro("canal ja existe").build();
            }
            guardarEReplicar(Registro.newBuilder().setTipo("canal").setUsuario(req.getUsuario()).setCanal(c)
                    .setTimestamp(agora()).setServidor(NOME).setRelogio(relogio).build());
            return nova("canal").setStatus("ok").build();
        }

        if (tipo.equals("listar")) {
            return nova("listar").setStatus("ok").addAllCanais(canais).build();
        }

        if (tipo.equals("publicar")) {
            if (!canais.contains(req.getCanal())) {
                return nova("publicar").setStatus("erro").setErro("canal nao existe").build();
            }
            if (req.getTexto().isEmpty()) {
                return nova("publicar").setStatus("erro").setErro("mensagem vazia").build();
            }
            String texto = req.getTexto().replace("\t", " ").replace("\n", " ");
            Msg p = nova("publicacao").setUsuario(req.getUsuario()).setCanal(req.getCanal()).setTexto(texto).build();
            pub.sendMore(req.getCanal());
            pub.send(p.toByteArray());
            log("ENVIADO(PUB " + req.getCanal() + ")", p);
            guardarEReplicar(Registro.newBuilder().setTipo("publicacao").setUsuario(req.getUsuario())
                    .setCanal(req.getCanal()).setTexto(texto).setTimestamp(p.getTimestamp())
                    .setServidor(NOME).setRelogio(p.getRelogio()).build());
            return nova("publicar").setStatus("ok").build();
        }

        return nova("erro").setStatus("erro").setErro("tipo desconhecido").build();
    }

    static Msg tratarPar(Msg req) {
        switch (req.getTipo()) {
            case "eleicao":
                eleicaoPendente = true;
                return nova("eleicao").setStatus("OK").build();
            case "relogio":
                return nova("relogio").setHora(agora()).build();
            case "sync":
                return nova("sync").addAllRegistros(registros).build();
            default:
                return nova("erro").setStatus("erro").setErro("tipo desconhecido").build();
        }
    }

    static void tratarSub(String topico, byte[] dados) throws Exception {
        Msg m = Msg.parseFrom(dados);
        receberRelogio(m);
        if (topico.equals("servers")) {
            coordenador = m.getCoordenador();
            log("RECEBIDO(SUB servers)", m);
        } else if (topico.equals("replica") && !m.getDe().equals(NOME)) {
            adicionar(m.getRegistro(), true);
            log("RECEBIDO(SUB replica)", m);
        }
    }

    static List<InfoServidor> listarServidores() {
        Msg r = pedir(REFERENCIA, nova("list").build(), 2000);
        return r == null ? new ArrayList<>() : r.getServidoresList();
    }

    static void buscarDados() {
        for (InfoServidor s : listarServidores()) {
            if (s.getNome().equals(NOME)) continue;
            Msg resp = pedir("tcp://" + s.getNome() + ":" + PORTA_PAR, nova("sync").build(), 5000);
            if (resp != null) {
                for (Registro r : resp.getRegistrosList()) adicionar(r, true);
                System.out.println(NOME + ": dados copiados de " + s.getNome() + " (" + registros.size() + " registros)");
                return;
            }
        }
    }

    static void eleicao() {
        boolean respondeu = false;
        for (InfoServidor s : listarServidores()) {
            if (s.getRank() > rank) {
                Msg r = pedir("tcp://" + s.getNome() + ":" + PORTA_PAR, nova("eleicao").build(), 1000);
                if (r != null && r.getStatus().equals("OK")) respondeu = true;
            }
        }
        if (respondeu) {
            coordenador = null;
            return;
        }
        coordenador = NOME;
        Msg anuncio = nova("coordenador").setCoordenador(NOME).build();
        pub.sendMore("servers");
        pub.send(anuncio.toByteArray());
        log("ENVIADO(PUB servers)", anuncio);
    }

    static void sincronizar() {
        if (coordenador == null) {
            eleicao();
            return;
        }
        if (coordenador.equals(NOME)) return;
        double inicio = System.currentTimeMillis() / 1000.0;
        Msg r = pedir("tcp://" + coordenador + ":" + PORTA_PAR, nova("relogio").build(), 2000);
        double fim = System.currentTimeMillis() / 1000.0;
        if (r == null) {
            System.out.println(NOME + ": coordenador " + coordenador + " nao respondeu, iniciando eleicao");
            coordenador = null;
            eleicao();
            return;
        }
        offset = r.getHora() + (fim - inicio) / 2 - fim;
        System.out.println(NOME + ": relogio ajustado com " + coordenador + ", offset = " + String.format("%.6f", offset) + " s");
    }

    static void heartbeat() {
        Msg r = pedir(REFERENCIA, nova("heartbeat").build(), 2000);
        if (r == null) System.out.println(NOME + ": referencia nao respondeu ao heartbeat");
    }

    static void iniciar() throws Exception {
        carregar();
        while (true) {
            Msg r = pedir(REFERENCIA, nova("rank").build(), 3000);
            if (r != null) {
                rank = r.getRank();
                break;
            }
            Thread.sleep(1000);
        }
        System.out.println(NOME + ": rank " + rank);
        buscarDados();
        eleicao();
    }

    public static void main(String[] args) throws Exception {
        Files.createDirectories(ARQUIVO.getParent());

        ZMQ.Socket par = ctx.createSocket(SocketType.REP);
        par.bind("tcp://*:" + PORTA_PAR);
        pub = ctx.createSocket(SocketType.PUB);
        pub.connect(PROXY_PUB);
        ZMQ.Socket sub = ctx.createSocket(SocketType.SUB);
        sub.connect(PROXY_SUB);
        sub.subscribe("servers");
        sub.subscribe("replica");
        Thread.sleep(1000);

        iniciar();

        ZMQ.Socket back = ctx.createSocket(SocketType.REP);
        back.connect(BROKER);

        ZMQ.Poller poller = ctx.createPoller(3);
        poller.register(back, ZMQ.Poller.POLLIN);
        poller.register(par, ZMQ.Poller.POLLIN);
        poller.register(sub, ZMQ.Poller.POLLIN);

        while (!Thread.currentThread().isInterrupted()) {
            poller.poll(1000);

            if (poller.pollin(0)) {
                Msg req = Msg.parseFrom(back.recv());
                receberRelogio(req);
                log("RECEBIDO", req);
                Msg resp = tratarCliente(req);
                back.send(resp.toByteArray());
                log("ENVIADO", resp);
                total++;
                if (total % 10 == 0) heartbeat();
                if (total % 15 == 0) sincronizar();
            }

            if (poller.pollin(1)) {
                Msg req = Msg.parseFrom(par.recv());
                receberRelogio(req);
                log("RECEBIDO(par)", req);
                Msg resp = tratarPar(req);
                par.send(resp.toByteArray());
                log("ENVIADO(par)", resp);
            }

            if (poller.pollin(2)) {
                String topico = sub.recvStr();
                byte[] dados = sub.recv();
                tratarSub(topico, dados);
            }

            if (eleicaoPendente) {
                eleicaoPendente = false;
                eleicao();
            }
        }
    }
}
