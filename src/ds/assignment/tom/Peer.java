package ds.assignment.tom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * A peer of the totally-ordered multicast network using Lamport clocks.
 *
 * Usage: Peer <peer-host[:port]> ... [--port N] [--name NAME] [--rate R]
 *
 * The arguments list every other peer. Each peer multicasts random words with a
 * Lamport timestamp; every receiver multicasts an ACK to all peers. A message is
 * delivered (printed) when it is at the head of the queue, ordered by
 * (timestamp, sender), and has been acknowledged by every peer. Messages to each
 * peer leave through a single thread, so channels are FIFO.
 */
public class Peer {
    static final int DEFAULT_PORT = 8000;
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int MESSAGE_RATE_PER_MIN = 60;  // Default: 1 message per second
    private static final int MAX_CLOCK_DRIFT = 100;  // Maximum allowed future timestamp drift

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String name;
    private final int port;
    private final List<InetSocketAddress> peers;
    private final Map<String, Set<String>> acks = new ConcurrentHashMap<>();
    private final Map<InetSocketAddress, ExecutorService> outbox = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue<Message> messageQueue;
    private int lamportClock;
    private int messageCounter;
    private final Random random;
    private final int messageRatePerMin;
    private final List<String> dictionary;
    private String evil;          // demo only: "past" or "future" makes this peer cheat
    private int lastTimestamp;

    // Malicious peer detection: track highest timestamp seen from each peer
    private final Map<String, Integer> peerMaxTimestamps;
    // Track (timestamp, senderId) pairs to detect timestamp reuse
    private final Set<String> seenTimestamps;
    // Blacklist of detected malicious peers
    private final Set<String> blacklistedPeers;

    private static class Message implements Comparable<Message> {
        final int timestamp;
        final String senderId;
        final int messageId;
        final String content;

        Message(int timestamp, String senderId, int messageId, String content) {
            this.timestamp = timestamp;
            this.senderId = senderId;
            this.messageId = messageId;
            this.content = content;
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
        this.messageQueue = new PriorityBlockingQueue<>();
        this.lamportClock = 0;
        this.messageCounter = 0;
        this.random = new Random();
        this.messageRatePerMin = messageRatePerMin;
        this.dictionary = loadDictionary();
        this.peerMaxTimestamps = new ConcurrentHashMap<>();
        this.seenTimestamps = ConcurrentHashMap.newKeySet();
        this.blacklistedPeers = ConcurrentHashMap.newKeySet();
    }

    private List<String> loadDictionary() {
        List<String> words = new ArrayList<>();
        try {
            // Try to load from resources folder first
            java.nio.file.Path path = Paths.get("resources/words.txt");
            if (Files.exists(path)) {
                words = Files.readAllLines(path);
            } else {
                // Fallback: try to load from classpath
                InputStream is = getClass().getClassLoader().getResourceAsStream("words.txt");
                if (is != null) {
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            words.add(line.trim());
                        }
                    }
                }
            }

            if (words.isEmpty()) {
                log("WARNING: Dictionary not found, using fallback words");
                // Fallback to a small list if dictionary file is not found
                words = Arrays.asList("hello", "world", "distributed", "systems", "consensus",
                                     "multicast", "lamport", "clock", "message", "network");
            }
        } catch (IOException e) {
            log("ERROR loading dictionary: " + e.getMessage());
            words = Arrays.asList("hello", "world", "distributed", "systems", "consensus");
        }
        return words;
    }

    public static void main(String[] args) throws Exception {
        String name = InetAddress.getLocalHost().getHostName();
        int port = DEFAULT_PORT;
        int messageRatePerMin = MESSAGE_RATE_PER_MIN;
        List<InetSocketAddress> peers = new ArrayList<>();
        String evil = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String option = arg, value = null;
            if (arg.startsWith("--") && arg.contains("=")) {
                option = arg.substring(0, arg.indexOf('='));
                value = arg.substring(arg.indexOf('=') + 1);
            } else if (arg.startsWith("--") && i + 1 < args.length) {
                value = args[++i];
            }
            switch (option) {
                case "--port": port = Integer.parseInt(value); break;
                case "--name": name = value; break;
                case "--rate": messageRatePerMin = Integer.parseInt(value); break;
                case "--evil": evil = value; break;
                default:
                    if (arg.startsWith("--")) throw new IllegalArgumentException("unknown option " + arg);
                    peers.add(parseAddress(arg, DEFAULT_PORT));
            }
        }

        Peer peer = new Peer(name, port, peers, messageRatePerMin);
        peer.evil = evil;
        peer.run();
    }

    private static InetSocketAddress parseAddress(String spec, int defaultPort) {
        String[] parts = spec.split(":", 2);
        String host = parts[0].isEmpty() ? InetAddress.getLoopbackAddress().getHostAddress() : parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : defaultPort;
        return new InetSocketAddress(host, port);
    }

    private void run() throws Exception {
        // Register this peer's address for acknowledgments

        // Start listener thread
        new Thread(this::listener).start();
        log("listening on port " + port);

        // Connect to all known peers
        Thread.sleep(500);  // Give time for all peers to start

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

                if (messageType.equals("MSG")) {
                    // MSG <timestamp> <senderId> <messageId> <content>
                    int timestamp = Integer.parseInt(parts[1]);
                    String senderId = parts[2];
                    int messageId = Integer.parseInt(parts[3]);
                    String content = parts.length > 4 ? parts[4] : "";

                    // Validate message for malicious behavior
                    if (!validateMessage(timestamp, senderId)) {
                        log("⚠️  MALICIOUS: rejected message from " + senderId + " [" + timestamp + "]");
                        out.println("REJECT " + senderId + " " + messageId);
                        continue;
                    }

                    synchronized (this) {
                        lamportClock = Math.max(lamportClock, timestamp) + 1;
                    }

                    Message msg = new Message(timestamp, senderId, messageId, content);
                    ack(msg.getKey(), senderId);
                    ack(msg.getKey(), name);
                    messageQueue.offer(msg);
                    log("received message [" + timestamp + ", " + senderId + ":" + messageId + "] \"" + content + "\"");

                    // Send ACK back
                    sendToAll("ACK " + senderId + " " + messageId + " " + name);
                    out.println("OK");

                } else if (messageType.equals("ACK")) {
                    // ACK <senderId> <messageId> <ackFrom>
                    String senderId = parts[1];
                    int messageId = Integer.parseInt(parts[2]);
                    String ackFrom = parts[3];
                    String key = senderId + ":" + messageId;

                    ack(senderId + ":" + messageId, ackFrom);
                    out.println("OK");
                }
            }
        } catch (IOException e) {
            // Connection closed
        }
    }

    /**
     * Validates a message to detect malicious peer behavior:
     * (a) "Rewriting history": using a timestamp already seen from this peer
     * (b) "Writing in the future": using a timestamp too far ahead of current clock
     *
     * @return true if message is valid, false if malicious behavior detected
     */
    private synchronized boolean validateMessage(int timestamp, String senderId) {
        // Check if peer is already blacklisted
        if (blacklistedPeers.contains(senderId)) {
            return false;
        }

        // Create unique key for this (timestamp, sender) pair
        String timestampKey = senderId + ":" + timestamp;

        // (a) Detect "rewriting history": same timestamp used twice by same peer
        if (seenTimestamps.contains(timestampKey)) {
            log("🚨 MALICIOUS BEHAVIOR DETECTED: " + senderId +
                " reusing timestamp " + timestamp + " (rewriting history)");
            blacklistedPeers.add(senderId);
            return false;
        }

        // Get the highest timestamp we've seen from this peer
        Integer maxTimestamp = peerMaxTimestamps.get(senderId);

        if (maxTimestamp != null && timestamp < maxTimestamp) {
            // (a) Detect "rewriting history": timestamp going backwards
            log("🚨 MALICIOUS BEHAVIOR DETECTED: " + senderId +
                " sent timestamp " + timestamp + " after sending " + maxTimestamp +
                " (going backwards in time)");
            blacklistedPeers.add(senderId);
            return false;
        }

        // (b) Detect "writing in the future": timestamp too far ahead
        if (timestamp > lamportClock + MAX_CLOCK_DRIFT) {
            log("🚨 MALICIOUS BEHAVIOR DETECTED: " + senderId +
                " sent timestamp " + timestamp + " when current clock is " + lamportClock +
                " (writing too far in the future)");
            blacklistedPeers.add(senderId);
            return false;
        }

        // Message is valid - record the timestamp
        seenTimestamps.add(timestampKey);
        peerMaxTimestamps.put(senderId, Math.max(maxTimestamp != null ? maxTimestamp : 0, timestamp));

        return true;
    }

    private void generateMessages() {
        try {
            // Calculate delay between messages (Poisson process)
            double lambda = messageRatePerMin / 60.0;  // messages per second

            while (true) {
                double delay = -Math.log(1 - random.nextDouble()) / lambda * 1000;  // milliseconds
                Thread.sleep((long) delay);

                // Generate and multicast a new message with a random word from dictionary
                String randomWord = dictionary.get(random.nextInt(dictionary.size()));
                multicastMessage(randomWord);
            }
        } catch (InterruptedException e) {
            log("message generator stopped");
        }
    }

    private synchronized void multicastMessage(String content) {
        lamportClock++;
        int timestamp = lamportClock;
        int msgId = messageCounter++;
        // Malicious behaviour for the demo, from the 4th message on
        if ("past".equals(evil) && msgId >= 3) {
            timestamp = lastTimestamp;               // reuses an old timestamp
            log("EVIL: reusing timestamp " + timestamp);
        } else if ("future".equals(evil) && msgId >= 3) {
            timestamp = lamportClock + 1000;         // jumps far ahead
            log("EVIL: sending timestamp " + timestamp);
        }
        lastTimestamp = timestamp;

        Message msg = new Message(timestamp, name, msgId, content);
        ack(msg.getKey(), name);
        messageQueue.offer(msg);

        log("multicast [" + timestamp + ", " + name + ":" + msgId + "] \"" + content + "\"");

        sendToAll("MSG " + timestamp + " " + name + " " + msgId + " " + content);
    }

    private void deliveryLoop() throws InterruptedException {
        while (true) {
            Thread.sleep(100);  // Check delivery conditions periodically

            Message msg;
            while ((msg = messageQueue.peek()) != null && canDeliver(msg)) {
                messageQueue.poll();
                deliverMessage(msg);
            }
        }
    }

    private boolean canDeliver(Message msg) {
        // A message can be delivered if:
        // 1. We have ACKs from all known peers (including ourselves)
        // 2. It is the smallest message in the queue (already at head due to priority queue)

        if (acks.getOrDefault(msg.getKey(), Set.of()).size() < peers.size() + 1) {
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

    private void ack(String key, String from) {
        acks.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(from);
    }

    private void sendToAll(String message) {
        for (InetSocketAddress peer : peers) {
            outbox.computeIfAbsent(peer, p -> Executors.newSingleThreadExecutor())
                  .submit(() -> {
                      while (sendMessage(peer, message) == null) {
                          try { Thread.sleep(500); } catch (InterruptedException e) { return; }
                      }
                  });
        }
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
