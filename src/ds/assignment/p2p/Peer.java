package ds.assignment.p2p;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A peer of the anti-entropy network.
 *
 * Usage: Peer [neighbour-host[:port] ...] [--port N] [--name NAME] [--value V] [--rate R]
 *
 * One thread waits for connections: other peers register themselves with it, or
 * ask it to synchronize. The main thread registers this peer with the neighbours
 * given on the command line and then, following a Poisson process, picks a random
 * neighbour from the map and averages its value with it.
 */
public class Peer {
    static final int DEFAULT_PORT = 7000;

    private static final double DEFAULT_SYNCS_PER_MINUTE = 2.0;
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int REPLY_TIMEOUT_MS = 2000;
    private static final long RETRY_MS = 1000;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String name;
    private final int port;
    private final double syncsPerMinute;
    // Peers that registered with us, or that we registered with: "host:port" -> address.
    private final Map<String, InetSocketAddress> neighbours = new ConcurrentHashMap<>();
    private double value;   // guarded by "this"

    Peer(String name, int port, double value, double syncsPerMinute) {
        this.name = name;
        this.port = port;
        this.value = value;
        this.syncsPerMinute = syncsPerMinute;
    }

    public static void main(String[] args) throws Exception {
        int port = DEFAULT_PORT;
        String name = InetAddress.getLocalHost().getHostName();
        double value = new Random().nextDouble();
        double syncsPerMinute = DEFAULT_SYNCS_PER_MINUTE;
        List<String> contacts = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (++i == args.length) usage();
                    port = Integer.parseInt(args[i]);
                    break;
                case "--name":
                    if (++i == args.length) usage();
                    name = args[i];
                    break;
                case "--value":
                    if (++i == args.length) usage();
                    value = Double.parseDouble(args[i]);
                    break;
                case "--rate":
                    if (++i == args.length) usage();
                    syncsPerMinute = Double.parseDouble(args[i]);
                    break;
                default:
                    if (args[i].startsWith("--")) usage();
                    contacts.add(args[i]);
            }
        }
        new Peer(name, port, value, syncsPerMinute).run(contacts);
    }

    private static void usage() {
        System.err.println("usage: Peer [neighbour-host[:port] ...] [--port N] [--name NAME] [--value V] [--rate R]");
        System.exit(1);
    }

    private static InetSocketAddress parseAddress(String address, int defaultPort) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            return new InetSocketAddress(address, defaultPort);
        }
        return new InetSocketAddress(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
    }

    private static String key(InetSocketAddress address) {
        return address.getHostString() + ":" + address.getPort();
    }

    private void run(List<String> contacts) throws IOException, InterruptedException {
        ServerSocket listener = new ServerSocket(port);
        log("listening on port " + port + ", initial value " + value);

        Thread acceptor = new Thread(() -> acceptConnections(listener), "acceptor");
        acceptor.setDaemon(true);
        acceptor.start();

        for (String contact : contacts) {
            register(parseAddress(contact, DEFAULT_PORT));
        }
        synchronizeForever();
    }

    private void acceptConnections(ServerSocket listener) {
        while (true) {
            try {
                Socket socket = listener.accept();
                new Thread(() -> serve(socket)).start();
            } catch (IOException e) {
                log("failed to accept a connection: " + e.getMessage());
            }
        }
    }

    /** Handles one incoming message: "REGISTER <port>" or "SYNC <value>". */
    private void serve(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            String line = in.readLine();
            if (line == null) {
                return;
            }
            String[] parts = line.trim().split("\\s+");
            if (parts.length == 2 && parts[0].equals("REGISTER")) {
                // The IP comes from the connection, the listening port from the message.
                InetSocketAddress peer = new InetSocketAddress(
                        socket.getInetAddress().getHostAddress(), Integer.parseInt(parts[1]));
                neighbours.put(key(peer), peer);
                out.println("OK");
                log(key(peer) + " registered, neighbours: " + neighbours.keySet());
            } else if (parts.length == 2 && parts[0].equals("SYNC")) {
                double theirs = Double.parseDouble(parts[1]);
                double mine;
                // Logged while holding the lock, so that the lines of a peer come
                // out in the order in which its value changed.
                synchronized (this) {
                    mine = value;
                    value = (mine + theirs) / 2;
                    log(String.format(Locale.ROOT, "sync from %s: %.6f and %.6f -> %.6f",
                            socket.getInetAddress().getHostAddress(), mine, theirs, value));
                }
                out.println("VALUE " + mine);
            }
        } catch (IOException | NumberFormatException e) {
            log("bad message: " + e.getMessage());
        }
    }

    /** Announces this peer to a neighbour; retries until the neighbour is up. */
    private void register(InetSocketAddress neighbour) throws InterruptedException {
        boolean warned = false;
        while (true) {
            String reply = request(neighbour, "REGISTER " + port);
            if ("OK".equals(reply)) {
                neighbours.put(key(neighbour), neighbour);
                log("registered with " + key(neighbour) + ", neighbours: " + neighbours.keySet());
                return;
            }
            if (!warned) {
                log(key(neighbour) + " unreachable, retrying until it comes up");
                warned = true;
            }
            Thread.sleep(RETRY_MS);
        }
    }

    /** Synchronizations form a Poisson process, so the gaps between them are exponential. */
    private void synchronizeForever() throws InterruptedException {
        Random random = new Random();
        double ratePerMs = syncsPerMinute / 60_000.0;
        while (true) {
            Thread.sleep((long) (-Math.log(1.0 - random.nextDouble()) / ratePerMs));
            List<InetSocketAddress> candidates = new ArrayList<>(neighbours.values());
            if (candidates.isEmpty()) {
                continue;
            }
            synchronizeWith(candidates.get(random.nextInt(candidates.size())));
        }
    }

    /**
     * Push-pull anti-entropy: we send our value and get the neighbour's back.
     *
     * The neighbour moves to the average of the two values, i.e. it changes by
     * (sent - theirs) / 2. We apply the opposite change instead of overwriting our
     * value, so the sum over the network is preserved even if another peer
     * synchronized with us while we were waiting for the reply.
     */
    private void synchronizeWith(InetSocketAddress neighbour) {
        double sent = getValue();
        String reply = request(neighbour, "SYNC " + sent);
        if (reply == null || !reply.startsWith("VALUE ")) {
            log("sync with " + key(neighbour) + " failed");
            return;
        }
        double theirs = Double.parseDouble(reply.substring(6));
        synchronized (this) {
            value += (theirs - sent) / 2;
            log(String.format(Locale.ROOT, "sync with %s: %.6f and %.6f -> %.6f", key(neighbour), sent, theirs, value));
        }
    }

    /** Sends one line to target and returns the one-line reply, or null on failure. */
    private String request(InetSocketAddress target, String message) {
        try (Socket socket = new Socket()) {
            socket.connect(target, CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(REPLY_TIMEOUT_MS);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(message);
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    private synchronized double getValue() {
        return value;
    }

    private void log(String message) {
        System.out.println("[" + LocalTime.now().format(TIME) + "] [" + name + "] " + message);
    }
}
