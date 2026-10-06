package ds.assignment.tring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Multi-threaded calculator server (one thread per client connection).
 *
 * Protocol: one request per line, "<op> <a> <b>" with op in {add, sub, mul, div};
 * the reply is a single line with the result, or "error: <reason>".
 */
public class Server {
    static final int DEFAULT_PORT = 6000;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        try (ServerSocket listener = new ServerSocket(port)) {
            log("listening on port " + port);
            while (true) {
                Socket client = listener.accept();
                new Thread(() -> serve(client)).start();
            }
        }
    }

    private static void serve(Socket client) {
        String who = client.getInetAddress().getHostAddress() + ":" + client.getPort();
        try (client;
             BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {
            log(who + " connected");
            String request;
            while ((request = in.readLine()) != null) {
                String reply = compute(request);
                log(who + ": " + request + " = " + reply);
                out.println(reply);
            }
        } catch (IOException e) {
            log(who + " failed: " + e.getMessage());
        }
        log(who + " disconnected");
    }

    static String compute(String request) {
        String[] parts = request.trim().split("\\s+");
        if (parts.length != 3) {
            return "error: expected <op> <a> <b>";
        }
        double a, b;
        try {
            a = Double.parseDouble(parts[1]);
            b = Double.parseDouble(parts[2]);
        } catch (NumberFormatException e) {
            return "error: invalid number";
        }
        switch (parts[0]) {
            case "add": return Double.toString(a + b);
            case "sub": return Double.toString(a - b);
            case "mul": return Double.toString(a * b);
            case "div": return b == 0 ? "error: division by zero" : Double.toString(a / b);
            default:    return "error: unknown operation " + parts[0];
        }
    }

    private static void log(String message) {
        System.out.println("[" + LocalTime.now().format(TIME) + "] [server] " + message);
    }
}
