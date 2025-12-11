import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * High-performance async NIO client for the ASCII-framed custom protocol:
 *
 * Frame:
 *   BEGIN (ASCII bytes)
 *   msgLength (4 bytes, big-endian)  -> length of payload bytes
 *   keyCount  (4 bytes, big-endian)
 *   payload   (ASCII bytes, e.g. key=value\n entries)
 *   END   (ASCII bytes)
 *
 * Features:
 *  - Non-blocking AsynchronousSocketChannel
 *  - writeFully that handles partial writes
 *  - ByteBufferPool to reuse direct buffers
 *  - Promise-based API (CompletableFuture)
 *
 * Improvements (when needed): Netty, Zero-copy file regions, TLS wrapping, Protocol Buffers for payload
 */
public final class AsyncAsciiClient implements Closeable {

    private static final byte[] BEGIN_BYTES = "BEGIN".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] END_BYTES = "END".getBytes(StandardCharsets.US_ASCII);
    private static final int INT_BYTES = Integer.BYTES;

    private final InetSocketAddress remote;
    private final ExecutorService executor;
    private final ByteBufferPool bufferPool;
    private AsynchronousSocketChannel channel;
    private final Duration connectTimeout;

    public AsyncAsciiClient(String host, int port) {
        this(host, port, Executors.newCachedThreadPool(), new ByteBufferPool(32, 8 * 1024), Duration.ofSeconds(5));
    }

    public AsyncAsciiClient(String host, int port, ExecutorService executor, ByteBufferPool pool, Duration connectTimeout) {
        this.remote = new InetSocketAddress(Objects.requireNonNull(host), port);
        this.executor = executor;
        this.bufferPool = pool;
        this.connectTimeout = connectTimeout;
    }

    /**
     * Connect to remote (async). Returns a CompletableFuture which completes when connection established.
     */
    public CompletableFuture<Void> connect() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                channel = AsynchronousSocketChannel.open();
                // Optionally configure socket options
                channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
                // connect (blocking wait using Future with timeout)
                var f = channel.connect(remote);
                f.get(connectTimeout.toMillis(), TimeUnit.MILLISECONDS);
                return null;
            } catch (Exception e) {
                throw new RuntimeException("Failed to connect", e);
            }
        }, executor);
    }

    /**
     * Send a map payload asynchronously. Returns a CompletableFuture that completes when the full frame is written.
     */
    public CompletableFuture<Void> send(Map<String, Object> payload) {
        Objects.requireNonNull(channel, "Not connected - call connect() first");

        // 1) Serialize payload to ASCII bytes
        byte[] payloadBytes = AsciiSerializer.serializeMap(payload);

        // 2) Prepare ByteBuffers (we keep them small and possibly direct via pool)
        ByteBuffer beginBuf = ByteBuffer.wrap(BEGIN_BYTES); // small - wrap is fine
        ByteBuffer lengthBuf = ByteBuffer.allocate(INT_BYTES).order(ByteOrder.BIG_ENDIAN);
        ByteBuffer keyCountBuf = ByteBuffer.allocate(INT_BYTES).order(ByteOrder.BIG_ENDIAN);
        ByteBuffer payloadBuf = bufferPool.get(Math.max(payloadBytes.length, 1024)); // get buffer sized >= payload
        ByteBuffer endBuf = ByteBuffer.wrap(END_BYTES);

        try {
            lengthBuf.putInt(payloadBytes.length).flip();
            keyCountBuf.putInt(payload.size()).flip();

            payloadBuf.clear();
            payloadBuf.put(payloadBytes);
            payloadBuf.flip();

            // We'll write these buffers in sequence: begin, length, keyCount, payload, end
            ByteBuffer[] buffers = new ByteBuffer[] { beginBuf, lengthBuf, keyCountBuf, payloadBuf, endBuf };

            return writeFully(buffers)
                    .whenComplete((v, ex) -> {
                        // return pooled buffer
                        bufferPool.offer(payloadBuf);
                        if (ex != null) {
                            // on error, consider closing channel (caller decides)
                        }
                    });
        } catch (Throwable t) {
            // ensure we return buffer if exception
            bufferPool.offer(payloadBuf);
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }
    }

    /**
     * Ensures all provided buffers are fully written to channel sequentially.
     */
    private CompletableFuture<Void> writeFully(ByteBuffer[] buffers) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        writeBuffersSequentially(0, buffers, promise);
        return promise;
    }

    private void writeBuffersSequentially(int idx, ByteBuffer[] buffers, CompletableFuture<Void> promise) {
        if (idx >= buffers.length) {
            promise.complete(null);
            return;
        }

        ByteBuffer buf = buffers[idx];
        if (!buf.hasRemaining()) {
            // move to next buffer
            writeBuffersSequentially(idx + 1, buffers, promise);
            return;
        }

        // write until this buffer exhausted (handles partial writes)
        channel.write(buf, null, new java.nio.channels.CompletionHandler<>() {
            @Override
            public void completed(Integer result, Object attachment) {
                if (buf.hasRemaining()) {
                    // continue writing same buffer
                    channel.write(buf, null, this);
                } else {
                    // advance to next buffer
                    writeBuffersSequentially(idx + 1, buffers, promise);
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                promise.completeExceptionally(exc);
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } finally {
            executor.shutdown();
        }
    }

    // -------- Helper classes below --------

    /**
     * Simple ASCII serializer for Map<String,Object> => "key=value\n" per entry.
     * Keep logic centralized so it can be replaced with a stricter binary serializer later.
     */
    public static final class AsciiSerializer {
        public static byte[] serializeMap(Map<String, Object> map) {
            // Estimate size
            int est = map.size() * 16;
            StringBuilder sb = new StringBuilder(est);
            for (Map.Entry<String, Object> e : map.entrySet()) {
                sb.append(e.getKey()).append('=').append(e.getValue().toString()).append('\n');
            }
            return sb.toString().getBytes(StandardCharsets.US_ASCII);
        }
    }

    /**
     * Very small direct buffer pool to reduce allocations under load.
     * Not thread-perf optimized — synchronized methods are enough for moderate concurrency.
     * Pool returns ByteBuffers that are ready for use (position = 0, limit = capacity).
     */
    public static final class ByteBufferPool {
        private final int maxBuffers;
        private final int bufferCapacity;
        private final Queue<ByteBuffer> pool;

        public ByteBufferPool(int maxBuffers, int bufferCapacity) {
            this.maxBuffers = maxBuffers;
            this.bufferCapacity = bufferCapacity;
            this.pool = new ArrayDeque<>(maxBuffers);
        }

        public synchronized ByteBuffer get(int minCapacity) {
            // If requested larger than pool capacity, allocate new direct buffer of requested size
            int cap = Math.max(minCapacity, bufferCapacity);
            ByteBuffer buf = pool.poll();
            if (buf == null || buf.capacity() < cap) {
                // allocate direct for better I/O performance
                ByteBuffer newBuf = ByteBuffer.allocateDirect(cap).order(ByteOrder.BIG_ENDIAN);
                return newBuf;
            }
            buf.clear();
            return buf;
        }

        public synchronized void offer(ByteBuffer buffer) {
            if (buffer == null) return;
            if (pool.size() >= maxBuffers) return;
            // Only accept direct buffers we created
            if (!buffer.isDirect()) return;
            buffer.clear();
            pool.offer(buffer);
        }
    }

    // -------- Example usage --------
    public static void main(String[] args) throws Exception {
        AsyncAsciiClient client = new AsyncAsciiClient("127.0.0.1", 9000);

        client.connect().thenCompose(v -> {
            Map<String, Object> payload = Map.of(
                    "temperature", 22.5,
                    "pressure", 101325,
                    "status", "OK"
            );
            return client.send(payload);
        }).thenAccept(v -> {
            System.out.println("Message sent successfully (async).");
        }).exceptionally(ex -> {
            ex.printStackTrace();
            return null;
        }).get(10, TimeUnit.SECONDS); // demo only: wait for completion here

        client.close();
    }
}
