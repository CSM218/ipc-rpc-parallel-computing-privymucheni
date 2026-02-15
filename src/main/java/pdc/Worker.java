package pdc;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    private Socket socket;
    private Thread heartbeatThread;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public boolean joinCluster(String masterHost, int port) {
        String workerIdEnv = System.getenv("WORKER_ID");
        final String workerId = (workerIdEnv == null) ? "worker-unknown" : workerIdEnv;
        String studentId = System.getenv("STUDENT_ID");

        try {
            this.socket = new Socket();
            this.socket.connect(new InetSocketAddress(masterHost, port), 5000);
            this.socket.setKeepAlive(true);

            InputStream is = new BufferedInputStream(this.socket.getInputStream());
            DataInputStream din = new DataInputStream(is);
            OutputStream os = this.socket.getOutputStream();
            DataOutputStream dout = new DataOutputStream(os);

            // Send registration
            Message reg = new Message();
            reg.messageType = "REGISTER_WORKER";
            reg.studentId = studentId;
            reg.payload = workerId;
            dout.write(reg.framedBytes());
            dout.flush();

            // Wait for ACK
            int len = din.readInt();
            if (len <= 0 || len > 10_000_000)
                return false;
            byte[] body = new byte[len];
            din.readFully(body);
            Message ack = Message.fromBodyBytes(body);
            if (ack == null || !"WORKER_ACK".equals(ack.messageType))
                return false;

            this.running = true;

            // Heartbeat sender (does not consume responses)
            heartbeatThread = new Thread(() -> {
                while (running) {
                    try {
                        Message hb = new Message();
                        hb.messageType = "HEARTBEAT";
                        hb.studentId = studentId;
                        hb.payload = workerId;
                        try {
                            dout.write(hb.framedBytes());
                            dout.flush();
                        } catch (IOException ignore) {
                        }
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            heartbeatThread.setDaemon(true);
            heartbeatThread.start();

            // Reader thread: consumes messages from master
            Thread reader = new Thread(() -> {
                try {
                    while (running) {
                        int l;
                        try {
                            l = din.readInt();
                        } catch (IOException e) {
                            break;
                        }
                        if (l <= 0 || l > 10_000_000)
                            break;
                        byte[] b = new byte[l];
                        din.readFully(b);
                        Message m = Message.fromBodyBytes(b);
                        if (m == null)
                            continue;
                        if ("RPC_REQUEST".equals(m.messageType)) {
                            final String p = m.payload == null ? "" : m.payload;
                            int first = p.indexOf(';');
                            int second = p.indexOf(';', first + 1);
                            if (first > 0 && second > first) {
                                final String taskId = p.substring(0, first);
                                final String taskType = p.substring(first + 1, second);
                                final String taskPayload = p.substring(second + 1);
                                exec.submit(() -> {
                                    try {
                                        if ("MATRIX_MULTIPLY".equals(taskType)) {
                                            String result = handleMatrixMultiply(taskPayload);
                                            Message resp = new Message();
                                            resp.messageType = "TASK_COMPLETE";
                                            resp.studentId = studentId;
                                            resp.payload = taskId + ";" + result;
                                            try {
                                                dout.write(resp.framedBytes());
                                                dout.flush();
                                            } catch (IOException ignore) {
                                            }
                                        } else {
                                            Message resp = new Message();
                                            resp.messageType = "TASK_ERROR";
                                            resp.studentId = studentId;
                                            resp.payload = taskId + ";unsupported";
                                            try {
                                                dout.write(resp.framedBytes());
                                                dout.flush();
                                            } catch (IOException ignore) {
                                            }
                                        }
                                    } catch (Exception ex) {
                                        Message resp = new Message();
                                        resp.messageType = "TASK_ERROR";
                                        resp.studentId = studentId;
                                        resp.payload = taskId + ";" + ex.getMessage();
                                        try {
                                            dout.write(resp.framedBytes());
                                            dout.flush();
                                        } catch (IOException ignore) {
                                        }
                                    }
                                });
                            }
                        }
                    }
                } catch (Exception e) {
                    // end
                }
            });
            reader.setDaemon(true);
            reader.start();

            return true;
        } catch (IOException e) {
            return false;
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
        if (heartbeatThread != null) {
            try {
                heartbeatThread.interrupt();
            } catch (Exception ignore) {
            }
        }
        exec.shutdownNow();
        try {
            exec.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignore) {
            }
        }
    }
}
