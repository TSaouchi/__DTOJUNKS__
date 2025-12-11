import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReactorSocketClient {

    public Mono<String> sendMessage(String host, int port, List<String> keys) {

        TcpClient client = TcpClient.create()
                .host(host)
                .port(port);

        return client.connect()
                .flatMap(conn -> {

                    ByteBuf frame = buildFrame(keys, conn.outbound().alloc());

                    return conn.outbound()
                            .sendObject(frame)
                            .then()
                            .thenMany(conn.inbound().receive().aggregate().asString(StandardCharsets.US_ASCII))
                            .single()
                            .doFinally(s -> conn.dispose());
                });
    }

    private ByteBuf buildFrame(List<String> keys, ByteBufAllocator alloc) {
        StringBuilder sb = new StringBuilder();
        keys.forEach(k -> sb.append(k).append("\n"));

        byte[] payload = sb.toString().getBytes(StandardCharsets.US_ASCII);

        ByteBuf buf = alloc.buffer();
        buf.writeBytes("BEGIN".getBytes(StandardCharsets.US_ASCII));
        buf.writeInt(payload.length);
        buf.writeInt(keys.size());
        buf.writeBytes(payload);
        buf.writeBytes("END".getBytes(StandardCharsets.US_ASCII));
        return buf;
    }
}
