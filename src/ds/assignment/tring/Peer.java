package ds.assignment.tring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
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
 *
 * Fault tolerance: the next peer acknowledges the token with the address of its
 * own successor, so a peer whose successor is down can skip it. A peer that sees
 * no token for too long regenerates it with a higher generation number (epoch).
 * The token carries its epoch and the name of the peer that created it; a peer
 * discards any token lower, in (epoch, creator) order, than the highest it has
 * seen, so if two peers regenerate at the same time only one token survives.
 */
public class Peer {
    static final int DEFAULT_PORT = 5000;
    static final String TOKEN = "TOKEN";

    private static final double REQUESTS_PER_MINUTE = 4.0;
    private static final String[] OPERATIONS = {"add", "sub", "mul", "div"};
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int ACK_TIMEOUT_MS = 2000;
    private static final long RETRY_MS = 1000;
    // Silence after which the token is considered lost.
    private static final int TOKEN_TIMEOUT_MS = 20_000;
    // Pause before forwarding, so an idle ring does not spin at full speed.
    private static final long TOKEN_DELAY_MS = 500;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String name;
    private final int port;
    private final InetSocketAddress next;
    private final InetSocketAddress server;
    private final Queue<String> requests = new ConcurrentLinkedQueue<>();

    // Fault tolerance state, used only by the token thread.
    private InetSocketAddress nextNext;   // learned from the ACK of the next peer
    private long epoch = 0;               // highest token generation seen so far
    private String creator = "";          // peer that created that token (tie-breaker)
    // Random extra, so that two peers rarely regenerate at the same time.
    private final int tokenTimeoutMs = TOKEN_TIMEOUT_MS + new Random().nextInt(10_000);

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
                creator = name;
                log("creating the token");
                listener.setSoTimeout(tokenTimeoutMs);
                onToken();
            }
            while (true) {
                if (receiveToken(listener)) {
                    // From the first token on, watch for its loss.
                    listener.setSoTimeout(tokenTimeoutMs);
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
             BufferedReader in = new BufferedReader(new InputStreamReader(previous.getInputStream()));
             PrintWriter out = new PrintWriter(previous.getOutputStream(), true)) {
            String line = in.readLine();
            if (line == null || !line.startsWith(TOKEN + " ")) {
                return false;
            }
            // Tell the sender who comes after us, so it can skip us if we fail.
            out.println("ACK " + next.getHostString() + ":" + next.getPort());
            String[] parts = line.split(" ");
            long receivedEpoch = Long.parseLong(parts[1]);
            String receivedCreator = parts.length > 2 ? parts[2] : "";
            if (receivedEpoch < epoch
                    || (receivedEpoch == epoch && receivedCreator.compareTo(creator) < 0)) {
                log("discarding duplicate token (epoch " + receivedEpoch + " by " + receivedCreator
                        + ", already seen epoch " + epoch + " by " + creator + ")");
                return false;
            }
            epoch = receivedEpoch;
            creator = receivedCreator;
            return true;
        } catch (SocketTimeoutException e) {
            epoch++;
            creator = name;
            log("no token for " + tokenTimeoutMs + " ms, regenerating it (epoch " + epoch + ")");
            return true;
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
            if (sendToken(next)) {
                return;
            }
            if (nextNext != null && sendToken(nextNext)) {
                log("next peer " + next + " is down, token sent to " + nextNext);
                return;
            }
            if (!warned) {
                log("next peer " + next + " unreachable, retrying until it comes back");
                warned = true;
            }
            Thread.sleep(RETRY_MS);
        }
    }

    /** Sends the token to target; true only if target acknowledged it. */
    private boolean sendToken(InetSocketAddress target) {
        try (Socket socket = new Socket()) {
            socket.connect(target, CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(ACK_TIMEOUT_MS);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(TOKEN + " " + epoch + " " + creator);
            String ack = in.readLine();
            if (ack == null || !ack.startsWith("ACK ")) {
                return false;
            }
            if (target.equals(next)) {
                nextNext = parseAddress(ack.substring(4), DEFAULT_PORT);
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void log(String message) {
        System.out.println("[" + LocalTime.now().format(TIME) + "] [" + name + "] " + message);
    }
}
