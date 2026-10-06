package ds.assignment.tring;

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
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A peer of the token ring.
 *
 * Usage: Peer <next-host[:port]> <server-host[:port]> [--port N] [--name NAME] [--token]
 *
 * One thread generates random calculator requests (Poisson process) into a local
 * queue. The main thread waits for the token; while it holds the token it has
 * exclusive access to the server, so it flushes the queue and then forwards the
 * token to the next peer. Exactly one peer of the ring must be started with
 * --token, to put the token in circulation.
 */
public class Peer {
    static final int DEFAULT_PORT = 5000;
    static final String TOKEN = "TOKEN";

    private static final double REQUESTS_PER_MINUTE = 4.0;
    private static final String[] OPERATIONS = {"add", "sub", "mul", "div"};
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final long RETRY_MS = 1000;
    // Pause before forwarding, so an idle ring does not spin at full speed.
    private static final long TOKEN_DELAY_MS = 500;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String name;
    private final int port;
    private final InetSocketAddress next;
    private final InetSocketAddress server;
    private final Queue<String> requests = new ConcurrentLinkedQueue<>();

    Peer(String name, int port, InetSocketAddress next, InetSocketAddress server) {
        this.name = name;
        this.port = port;
        this.next = next;
        this.server = server;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
        }
        int port = DEFAULT_PORT;
        String name = InetAddress.getLocalHost().getHostName();
        boolean startsWithToken = false;
        for (int i = 2; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (++i == args.length) usage();
                    port = Integer.parseInt(args[i]);
                    break;
                case "--name":
                    if (++i == args.length) usage();
                    name = args[i];
                    break;
                case "--token":
                    startsWithToken = true;
                    break;
                default:
                    usage();
            }
        }
        Peer peer = new Peer(name, port, parseAddress(args[0], DEFAULT_PORT),
                parseAddress(args[1], Server.DEFAULT_PORT));
        peer.run(startsWithToken);
    }

    private static void usage() {
        System.err.println("usage: Peer <next-host[:port]> <server-host[:port]> [--port N] [--name NAME] [--token]");
        System.exit(1);
    }

    private static InetSocketAddress parseAddress(String address, int defaultPort) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            return new InetSocketAddress(address, defaultPort);
        }
        return new InetSocketAddress(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
    }

    private void run(boolean startsWithToken) throws IOException, InterruptedException {
        try (ServerSocket listener = new ServerSocket(port)) {
            log("listening on port " + port + ", next peer " + next + ", server " + server);

            Thread generator = new Thread(this::generateRequests, "request-generator");
            generator.setDaemon(true);
            generator.start();

            if (startsWithToken) {
                log("creating the token");
                onToken();
            }
            while (true) {
                if (receiveToken(listener)) {
                    onToken();
                }
            }
        }
    }

    /** Request arrivals form a Poisson process, so the gaps between them are exponential. */
    private void generateRequests() {
        Random random = new Random();
        double ratePerMs = REQUESTS_PER_MINUTE / 60_000.0;
        try {
            while (true) {
                Thread.sleep((long) (-Math.log(1.0 - random.nextDouble()) / ratePerMs));
                String operation = OPERATIONS[random.nextInt(OPERATIONS.length)];
                String request = operation + " " + (random.nextInt(100) + 1) + " " + (random.nextInt(100) + 1);
                requests.add(request);
                log("queued \"" + request + "\" (" + requests.size() + " pending)");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean receiveToken(ServerSocket listener) {
        try (Socket previous = listener.accept();
             BufferedReader in = new BufferedReader(new InputStreamReader(previous.getInputStream()))) {
            return TOKEN.equals(in.readLine());
        } catch (IOException e) {
            log("failed to receive a message: " + e.getMessage());
            return false;
        }
    }

    private void onToken() throws InterruptedException {
        if (!requests.isEmpty()) {
            processRequests();
        }
        Thread.sleep(TOKEN_DELAY_MS);
        forwardToken();
    }

    /** Called only while holding the token, i.e. with exclusive access to the server. */
    private void processRequests() {
        log("holding the token, " + requests.size() + " request(s) to process");
        try (Socket socket = new Socket()) {
            socket.connect(server, CONNECT_TIMEOUT_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String request;
            // A request only leaves the queue once its result has arrived.
            while ((request = requests.peek()) != null) {
                out.println(request);
                String result = in.readLine();
                if (result == null) {
                    throw new IOException("connection closed by the server");
                }
                requests.poll();
                log("result: " + request + " = " + result);
            }
        } catch (IOException e) {
            log("server unavailable (" + e.getMessage() + "), keeping " + requests.size()
                    + " request(s) for the next round");
        }
        log("releasing the token");
    }

    private void forwardToken() throws InterruptedException {
        boolean warned = false;
        while (true) {
            try (Socket socket = new Socket()) {
                socket.connect(next, CONNECT_TIMEOUT_MS);
                new PrintWriter(socket.getOutputStream(), true).println(TOKEN);
                return;
            } catch (IOException e) {
                if (!warned) {
                    log("next peer " + next + " unreachable, retrying until it comes back");
                    warned = true;
                }
                Thread.sleep(RETRY_MS);
            }
        }
    }

    private void log(String message) {
        System.out.println("[" + LocalTime.now().format(TIME) + "] [" + name + "] " + message);
    }
}
