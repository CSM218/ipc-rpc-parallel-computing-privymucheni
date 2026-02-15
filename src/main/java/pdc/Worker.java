package pdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 *
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final ExecutorService exec = Executors
            .newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
    private volatile boolean running = true;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        String workerIdEnv = System.getenv("WORKER_ID");
        final String workerId = (workerIdEnv == null) ? "worker-unknown" : workerIdEnv;
        String studentId = System.getenv("STUDENT_ID");

        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress(masterHost, port), 5000);
            sock.setKeepAlive(true);

            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(sock.getOutputStream(), true, StandardCharsets.UTF_8)) {

                // Send registration
                Message reg = new Message();
                reg.messageType = "REGISTER_WORKER";
                reg.studentId = studentId;
                reg.payload = workerId;
                out.println(reg.toJson());

                // Start heartbeat sender
                Thread heartbeat = new Thread(() -> {
                    while (running && !sock.isClosed()) {
                        try {
                            Message hb = new Message();
                            hb.messageType = "HEARTBEAT";
                            hb.studentId = studentId;
                            hb.payload = workerId;
                            out.println(hb.toJson());
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
                heartbeat.setDaemon(true);
                heartbeat.start();

                // Read loop: handle commands from master
                String line;
                while (running && (line = in.readLine()) != null) {
                    try {
                        Message msg = Message.parse(line);
                        if (msg == null)
                            continue;

                        if ("RPC_REQUEST".equals(msg.messageType)) {
                            final String p = msg.payload == null ? "" : msg.payload;
                            int first = p.indexOf(';');
                            int second = p.indexOf(';', first + 1);
                            if (first > 0 && second > first) {
                                final String taskId = p.substring(0, first);
                                final String taskType = p.substring(first + 1, second);
                                final String taskPayload = p.substring(second + 1);

                                // Execute asynchronously
                                exec.submit(() -> {
                                    try {
                                        if ("MATRIX_MULTIPLY".equals(taskType)) {
                                            String result = handleMatrixMultiply(taskPayload);
                                            Message resp = new Message();
                                            resp.messageType = "TASK_COMPLETE";
                                            resp.studentId = studentId;
                                            resp.payload = taskId + ";" + result;
                                            out.println(resp.toJson());
                                        } else {
                                            Message resp = new Message();
                                            resp.messageType = "TASK_ERROR";
                                            resp.studentId = studentId;
                                            resp.payload = taskId + ";unsupported";
                                            out.println(resp.toJson());
                                        }
                                    } catch (Exception ex) {
                                        Message resp = new Message();
                                        resp.messageType = "TASK_ERROR";
                                        resp.studentId = studentId;
                                        resp.payload = taskId + ";" + ex.getMessage();
                                        out.println(resp.toJson());
                                    }
                                });
                            }
                        } else if ("HEARTBEAT_ACK".equals(msg.messageType) || "WORKER_ACK".equals(msg.messageType)) {
                            // ignore or log
                        }
                    } catch (Exception ex) {
                        // ignore malformed
                    }
                }

            }
        } catch (IOException e) {
            // connection failed; exit or retry is possible
        } finally {
            shutdown();
        }
    }

    private String handleMatrixMultiply(String payload) {
        String[] parts = payload.split("\\|", 2);
        if (parts.length < 2)
            return "";
        int[][] a = parseMatrix(parts[0]);
        int[][] b = parseMatrix(parts[1]);
        int[][] r = multiply(a, b);
        return serializeMatrix(r);
    }

    private int[][] parseMatrix(String s) {
        String[] rows = s.split("\\\\");
        int n = rows.length;
        int m = rows[0].isEmpty() ? 0 : rows[0].split(",").length;
        int[][] mat = new int[n][m];
        for (int i = 0; i < n; i++) {
            if (rows[i].isEmpty())
                continue;
            String[] cols = rows[i].split(",");
            for (int j = 0; j < cols.length; j++) {
                try {
                    mat[i][j] = Integer.parseInt(cols[j]);
                } catch (NumberFormatException e) {
                    mat[i][j] = 0;
                }
            }
        }
        return mat;
    }

    private int[][] multiply(int[][] a, int[][] b) {
        int n = a.length;
        int m = b[0].length;
        int common = a[0].length;
        int[][] res = new int[n][m];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                long sum = 0;
                for (int k = 0; k < common; k++)
                    sum += (long) a[i][k] * b[k][j];
                res[i][j] = (int) sum;
            }
        }
        return res;
    }

    private String serializeMatrix(int[][] a) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < a.length; i++) {
            if (i > 0)
                sb.append('\\');
            for (int j = 0; j < a[i].length; j++) {
                if (j > 0)
                    sb.append(',');
                sb.append(a[i][j]);
            }
        }
        return sb.toString();
    }

    public void execute() {
        // Placeholder: primary behavior runs in joinCluster; expose execute for
        // compatibility
    }

    public void shutdown() {
        running = false;
        exec.shutdownNow();
        try {
            exec.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
