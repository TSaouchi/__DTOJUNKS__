import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SimpleSyncNioClient {

    private static final byte[] BEGIN = "BEGIN".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] END   = "END".getBytes(StandardCharsets.US_ASCII);

    public static String sendAndReceive(String host, int port, Map<String, Object> payload) throws IOException {

        try (SocketChannel channel = SocketChannel.open()) {
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress(host, port));

            // --- prepare payload ---
            byte[] payloadBytes = serializeAsciiMap(payload);

            ByteBuffer beginBuf  = ByteBuffer.wrap(BEGIN);
            ByteBuffer lenBuf    = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            ByteBuffer countBuf  = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            ByteBuffer payloadBuf = ByteBuffer.wrap(payloadBytes);
            ByteBuffer endBuf    = ByteBuffer.wrap(END);

            lenBuf.putInt(payloadBytes.length).flip();
            countBuf.putInt(payload.size()).flip();

            // --- write ---
            writeFully(channel, beginBuf);
            writeFully(channel, lenBuf);
            writeFully(channel, countBuf);
            writeFully(channel, payloadBuf);
            writeFully(channel, endBuf);

            // --- read response ---
            return readResponse(channel);
        }
    }

    /**
     * Reads until the server closes the connection.
     * If your protocol uses END markers, we can modify this.
     */
    private static String readResponse(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        StringBuilder sb = new StringBuilder();

        while (true) {
            buffer.clear();
            int read = channel.read(buffer);
            if (read == -1) break;  // server closed socket
            buffer.flip();
            sb.append(StandardCharsets.US_ASCII.decode(buffer));
        }

        return sb.toString();
    }

    private static byte[] serializeAsciiMap(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder(map.size() * 16);
        map.forEach((k, v) -> sb.append(k).append("=").append(v).append("\n"));
        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static void writeFully(SocketChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }
}
