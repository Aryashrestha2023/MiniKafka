package com.minikafka.broker.tcp;

import com.minikafka.config.MiniKafkaProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class TcpBrokerServer {

    private static final Logger log = LoggerFactory.getLogger(TcpBrokerServer.class);

    private final MiniKafkaProperties properties;
    private final TcpProtocolHandler protocolHandler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService acceptor;
    private ExecutorService workers;
    private Selector selector;
    private ServerSocketChannel serverChannel;

    public TcpBrokerServer(MiniKafkaProperties properties, TcpProtocolHandler protocolHandler) {
        this.properties = properties;
        this.protocolHandler = protocolHandler;
    }

    @PostConstruct
    public void start() {
        if (!properties.getTcp().isEnabled()) {
            log.info("event=tcp_server_disabled");
            return;
        }
        running.set(true);
        workers = Executors.newFixedThreadPool(properties.getTcp().getWorkerThreads());
        acceptor = Executors.newSingleThreadExecutor();
        acceptor.submit(this::runLoop);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        closeQuietly(serverChannel);
        closeQuietly(selector);
        if (acceptor != null) {
            acceptor.shutdownNow();
        }
        if (workers != null) {
            workers.shutdownNow();
        }
        log.info("event=tcp_server_stopped");
    }

    private void runLoop() {
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.bind(new InetSocketAddress(properties.getTcp().getHost(), properties.getTcp().getPort()));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            log.info("event=tcp_server_started host={} port={}", properties.getTcp().getHost(), properties.getTcp().getPort());

            while (running.get()) {
                selector.select(500);
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept(key);
                    } else if (key.isReadable()) {
                        read(key);
                    }
                }
            }
        } catch (IOException ex) {
            if (running.get()) {
                log.error("event=tcp_server_failed message={}", ex.getMessage(), ex);
            }
        }
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client == null) {
            return;
        }
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ, new ClientBuffer());
        log.info("event=tcp_client_connected remote={}", client.getRemoteAddress());
    }

    private void read(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ClientBuffer clientBuffer = (ClientBuffer) key.attachment();
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        int read;
        try {
            read = client.read(buffer);
            if (read == -1) {
                closeClient(key, client);
                return;
            }
        } catch (IOException ex) {
            closeClient(key, client);
            return;
        }

        buffer.flip();
        clientBuffer.append(StandardCharsets.UTF_8.decode(buffer).toString());
        String line;
        while ((line = clientBuffer.nextLine()) != null) {
            String commandLine = line;
            workers.submit(() -> respond(client, commandLine));
        }
    }

    private void respond(SocketChannel client, String line) {
        String response = protocolHandler.handleLine(line);
        ByteBuffer out = ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8));
        synchronized (client) {
            try {
                while (out.hasRemaining()) {
                    client.write(out);
                }
            } catch (IOException ex) {
                closeQuietly(client);
            }
        }
    }

    private void closeClient(SelectionKey key, SocketChannel client) {
        key.cancel();
        closeQuietly(client);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Shutdown should be best-effort.
        }
    }

    private static final class ClientBuffer {
        private final StringBuilder builder = new StringBuilder();

        void append(String text) {
            builder.append(text);
        }

        String nextLine() {
            int index = builder.indexOf("\n");
            if (index < 0) {
                return null;
            }
            String line = builder.substring(0, index).replace("\r", "");
            builder.delete(0, index + 1);
            return line;
        }
    }
}
