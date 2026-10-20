package ds.assignment.tom;

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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * A peer of the totally-ordered multicast network using Lamport clocks.
 *
 * Usage: Peer <peer-host[:port]> ... [--port N] [--name NAME] [--rate R]
 *
 * Each peer multicasts messages to all other peers with a Lamport timestamp.
 * Messages are delivered in total order: sorted by (timestamp, sender-id).
 */
public class Peer {
    static final int DEFAULT_PORT = 8000;
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int MESSAGE_RATE_PER_MIN = 10;  // Default messages per minute

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String name;
    private final int port;
    private final List<InetSocketAddress> peers;
    private final Map<String, InetSocketAddress> peerAddresses;
    private final Set<String> acksReceived;
    private final PriorityBlockingQueue<Message> messageQueue;
    private int lamportClock;
    private int messageCounter;
    private final Random random;
    private final int messageRatePerMin;

    private static class Message implements Comparable<Message> {
        final int timestamp;
        final String senderId;
        final int messageId;
        final String content;
        final Set<String> acks;

        Message(int timestamp, String senderId, int messageId, String content) {
            this.timestamp = timestamp;
            this.senderId = senderId;
            this.messageId = messageId;
            this.content = content;
            this.acks = ConcurrentHashMap.newKeySet();
        }

        @Override
        public int compareTo(Message other) {
            if (this.timestamp != other.timestamp) {
                return Integer.compare(this.timestamp, other.timestamp);
            }
            return this.senderId.compareTo(other.senderId);
        }

        String getKey() {
            return senderId + ":" + messageId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Message)) return false;
            Message m = (Message) o;
            return timestamp == m.timestamp && senderId.equals(m.senderId) && messageId == m.messageId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, senderId, messageId);
        }
    }

    public Peer(String name, int port, List<InetSocketAddress> peers, int messageRatePerMin) {
        this.name = name;
        this.port = port;
        this.peers = new ArrayList<>(peers);
        this.peerAddresses = new ConcurrentHashMap<>();
        this.acksReceived = ConcurrentHashMap.newKeySet();
        this.messageQueue = new PriorityBlockingQueue<>();
        this.lamportClock = 0;
        this.messageCounter = 0;
        this.random = new Random();
        this.messageRatePerMin = messageRatePerMin;
    }

    public static void main(String[] args) throws Exception {
        String name = "peer";
        int port = DEFAULT_PORT;
        int messageRatePerMin = MESSAGE_RATE_PER_MIN;
        List<InetSocketAddress> peers = new ArrayList<>();

        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring(7));
            } else if (arg.startsWith("--name=")) {
                name = arg.substring(7);
            } else if (arg.startsWith("--rate=")) {
                messageRatePerMin = Integer.parseInt(arg.substring(7));
            } else if (!arg.startsWith("--")) {
                peers.add(parseAddress(arg, DEFAULT_PORT));
            }
        }

        new Peer(name, port, peers, messageRatePerMin).run();
    }

    private static InetSocketAddress parseAddress(String spec, int defaultPort) {
        String[] parts = spec.split(":", 2);
        String host = parts[0].isEmpty() ? InetAddress.getLoopbackAddress().getHostAddress() : parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : defaultPort;
        return new InetSocketAddress(host, port);
    }

    private void run() throws Exception {
        // Register this peer's address for acknowledgments
        peerAddresses.put(name, new InetSocketAddress(InetAddress.getLocalHost(), port));

        // Start listener thread
        new Thread(this::listener).start();
        log("listening on port " + port);

        // Connect to all known peers
        Thread.sleep(500);  // Give time for all peers to start
        for (InetSocketAddress peer : peers) {
            new Thread(() -> connectToPeer(peer)).start();
        }

        // Wait for peer connections to establish
        Thread.sleep(1000);

        // Start message generator thread
        new Thread(this::generateMessages).start();

        // Start delivery thread
        deliveryLoop();
    }

    private void listener() {
        try (ServerSocket listener = new ServerSocket(port)) {
            while (true) {
                Socket client = listener.accept();
                new Thread(() -> handleConnection(client)).start();
            }
        } catch (IOException e) {
            log("listener failed: " + e.getMessage());
        }
    }

    private void handleConnection(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                String[] parts = line.split(" ", 5);
                String messageType = parts[0];

                if (messageType.equals("HELLO")) {
                    // HELLO <name> <host> <port>
                    String peerName = parts[1];
                    String host = parts[2];
                    int peerPort = Integer.parseInt(parts[3]);
                    peerAddresses.put(peerName, new InetSocketAddress(host, peerPort));
                    log("registered peer " + peerName + " at " + host + ":" + peerPort);
                    out.println("OK");

                } else if (messageType.equals("MSG")) {
                    // MSG <timestamp> <senderId> <messageId> <content>
                    int timestamp = Integer.parseInt(parts[1]);
                    String senderId = parts[2];
                    int messageId = Integer.parseInt(parts[3]);
                    String content = parts.length > 4 ? parts[4] : "";

                    synchronized (this) {
                        lamportClock = Math.max(lamportClock, timestamp) + 1;
                    }

                    Message msg = new Message(timestamp, senderId, messageId, content);
                    messageQueue.offer(msg);
                    log("received message [" + timestamp + ", " + senderId + ":" + messageId + "] \"" + content + "\"");

                    // Send ACK back
                    out.println("ACK " + senderId + " " + messageId + " " + name);

                } else if (messageType.equals("ACK")) {
                    // ACK <senderId> <messageId> <ackFrom>
                    String senderId = parts[1];
                    int messageId = Integer.parseInt(parts[2]);
                    String ackFrom = parts[3];
                    String key = senderId + ":" + messageId;

                    // Record this acknowledgment
                    synchronized (this) {
                        acksReceived.add(key + ":" + ackFrom);
                    }

                    // Update the message in the queue with this ACK
                    for (Message msg : messageQueue) {
                        if (msg.getKey().equals(key)) {
                            msg.acks.add(ackFrom);
                            break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            // Connection closed
        }
    }

    private void connectToPeer(InetSocketAddress peer) {
        String reply = sendMessage(peer, "HELLO " + name + " " +
                                   InetAddress.getLoopbackAddress().getHostAddress() + " " + port);
        if (reply != null && reply.equals("OK")) {
            log("connected to peer at " + peer);
        } else {
            log("failed to connect to peer at " + peer);
        }
    }

    private void generateMessages() {
        try {
            // Calculate delay between messages (Poisson process)
            double lambda = messageRatePerMin / 60.0;  // messages per second

            while (true) {
                double delay = -Math.log(1 - random.nextDouble()) / lambda * 1000;  // milliseconds
                Thread.sleep((long) delay);

                // Generate and multicast a new message
                multicastMessage("Message " + messageCounter);
            }
        } catch (InterruptedException e) {
            log("message generator stopped");
        }
    }

    private synchronized void multicastMessage(String content) {
        lamportClock++;
        int timestamp = lamportClock;
        int msgId = messageCounter++;

        Message msg = new Message(timestamp, name, msgId, content);
        msg.acks.add(name);  // Implicitly ACK our own message
        messageQueue.offer(msg);

        log("multicast [" + timestamp + ", " + name + ":" + msgId + "] \"" + content + "\"");

        // Send to all known peers
        for (Map.Entry<String, InetSocketAddress> entry : peerAddresses.entrySet()) {
            if (!entry.getKey().equals(name)) {
                InetSocketAddress addr = entry.getValue();
                new Thread(() -> {
                    sendMessage(addr, "MSG " + timestamp + " " + name + " " + msgId + " " + content);
                }).start();
            }
        }
    }

    private void deliveryLoop() throws InterruptedException {
        while (true) {
            Thread.sleep(100);  // Check delivery conditions periodically

            Message msg = messageQueue.peek();
            if (msg != null && canDeliver(msg)) {
                messageQueue.poll();
                deliverMessage(msg);
            }
        }
    }

    private boolean canDeliver(Message msg) {
        // A message can be delivered if:
        // 1. We have ACKs from all known peers (including ourselves)
        // 2. It is the smallest message in the queue (already at head due to priority queue)

        int expectedAcks = peerAddresses.size();
        if (msg.acks.size() < expectedAcks) {
            return false;
        }

        // Check if any message with smaller timestamp exists
        for (Message other : messageQueue) {
            if (other != msg && other.compareTo(msg) < 0) {
                return false;
            }
        }

        return true;
    }

    private void deliverMessage(Message msg) {
        log("deliver [" + msg.timestamp + ", " + msg.senderId + ":" + msg.messageId + "] \"" + msg.content + "\"");
    }

    private String sendMessage(InetSocketAddress target, String message) {
        try (Socket socket = new Socket()) {
            socket.connect(target, CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(CONNECT_TIMEOUT_MS);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(message);
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    private synchronized void log(String message) {
        System.out.println("[" + LocalTime.now().format(TIME) + "] [" + name + "] " + message);
    }
}
