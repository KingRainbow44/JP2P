package moe.seikimo.jp2p;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This is a simple Java-only P2P communication library over UDP.
 * <p>
 * The API includes:
 * - {@link JP2P#onClientConnected(Consumer)} to handle new client connections.
 * - {@link JP2P#onClientDisconnected(Consumer)} to handle client disconnections.
 * - {@link JP2P#onMessageReceived(BiConsumer)} to handle received messages.
 * - {@link JP2P#sendMessage(byte[])} to send a message to all connected clients.
 * - {@link JP2P#forgetClients()} to forget all connected clients.
 * - {@link JP2P#isConnectionAlive()} to check if the connection is alive.
 * - {@link JP2P#shutdown()} to safely terminate the P2P instance.
 * - {@link JP2P#start()} to start the P2P instance.
 * <p>
 * If you are using the library as a server, you can use the following API:
 * - {@link JP2P#setTimeout(long)} to set the timeout for clients to be removed.
 * - {@link JP2P#broadcastToRoom(long, byte[])} to broadcast a message to a room.
 */
public final class JP2P extends Thread {
    private static final String USAGE_MESSAGE = "Usage: java -jar jp2p.jar <client|server> [address|port]";

    /**
     * This is an example of a simple P2P library.
     *
     * @param args The arguments to the program.
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println(USAGE_MESSAGE);
            return;
        }

        switch (args[0]) {
            case "server" -> {
                var p2p = new JP2P(Short.parseShort(args[1]));
                p2p.setTimeout(TimeUnit.SECONDS.toMillis(10));
                p2p.start();

                System.out.println("Discovery server started!\n");

                System.out.println("Note:");
                System.out.println("The discovery server does not log any messages.");
                System.out.println("To test the server, run the included client.");
            }
            case "client" -> {
                var p2p = new JP2P(args[1], 0);

                // Configure the P2P instance.
                p2p.onClientConnected(client ->
                        System.out.printf("Client connected: %s:%s%n", client.address, client.port));
                p2p.onClientDisconnected(client ->
                        System.out.printf("Client disconnected: %s:%s%n", client.address, client.port));
                p2p.onMessageReceived((client, message) ->
                        System.out.printf("Message received: %s%n", new String(message.data, StandardCharsets.UTF_8)));
                p2p.start();

                System.out.println("Enter a message to send:");

                var scanner = new Scanner(System.in);
                while (true) try {
                    System.out.print("> ");

                    var message = scanner.nextLine();
                    switch (message.trim()) {
                        case "exit" -> {
                            p2p.shutdown();
                            return;
                        }
                        case "forget" -> {
                            p2p.forgetClients();
                            System.out.println("Forgotten all clients.");
                        }
                        default -> p2p.sendMessage(message.getBytes(StandardCharsets.UTF_8));
                    }
                } catch (NoSuchElementException ignored) {
                    System.out.println("Goodbye!");
                }
            }
            default -> System.out.println(USAGE_MESSAGE);
        }
    }

    /**
     * Parses an address in the format "host:port" into an InetSocketAddress.
     *
     * @param address The address to parse.
     * @return The parsed InetSocketAddress.
     */
    private static InetSocketAddress parseAddress(String address) {
        var parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format");
        }

        return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    }

    /**
     * A JP2P message.
     *
     * @param packetId The ID of the packet.
     * @param data The data of the packet.
     */
    public record Message(short packetId, byte[] data) {}

    /**
     * A JP2P client.
     *
     * @param address The address of the client.
     * @param port The port of the client.
     */
    public record Client(String address, int port) {
        /**
         * Static utility to decode a client from a buffer.
         *
         * @param buffer The buffer to decode.
         * @return The decoded client.
         */
        public static Client fromBuffer(ByteBuffer buffer) {
            var addressLength = buffer.getInt();
            var address = new byte[addressLength];
            buffer.get(address);

            var port = buffer.getInt();
            return new Client(new String(address, StandardCharsets.UTF_8), port);
        }

        /**
         * @return The client as an InetSocketAddress.
         */
        public InetSocketAddress asSocketAddress() {
            return new InetSocketAddress(this.address, this.port);
        }

        /**
         * @return A buffer representation of the client.
         */
        public ByteBuffer asBuffer() {
            var address = this.address.getBytes(StandardCharsets.UTF_8);
            var buffer = ByteBuffer.allocate(4 + address.length + 4);
            buffer.putInt(address.length);
            buffer.put(address);
            buffer.putInt(this.port);

            return buffer;
        }
    }

    private final boolean isServer;
    private final DatagramSocket socket;
    private final byte[] buffer = new byte[1024];

    private boolean isAlive = false;
    private Consumer<Throwable> errorHandler = Throwable::printStackTrace;

    private SocketAddress serverAddress = null;
    private long roomId = Long.MAX_VALUE;

    public JP2P(short port) throws IOException {
        super("P2P Server");

        this.isServer = true;
        this.socket = new DatagramSocket(port);
    }

    public JP2P(String address, long roomId) throws IOException {
        super("P2P Client");

        this.isServer = false;
        this.socket = new DatagramSocket();
        this.serverAddress = JP2P.parseAddress(address);

        this.roomId = roomId;
    }

    /**
     * Sets the error handler for the P2P instance.
     *
     * @param errorHandler The error handler to set.
     * @return The instance for method chaining.
     */
    public JP2P setErrorHandler(Consumer<Throwable> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    @Override
    public void run() {
        // Start the separate ping thread.
        var pingThread = new Thread(() -> {
            while (!this.isAlive) {
                try {
                    if (!this.isServer) {
                        this.clientPing();
                    } else {
                        this.serverPing();
                    }
                } catch (Throwable t) {
                    this.errorHandler.accept(t);
                }
            }
        });
        pingThread.start();

        // Do the main loop.
        while (!this.isAlive) {
            try {
                if (this.isServer) {
                    this.serverLoop();
                } else {
                    this.clientLoop();
                }
            } catch (Throwable t) {
                this.errorHandler.accept(t);
            }
        }

        this.socket.close();
    }

    /**
     * @return Whether the connection thread is alive.
     */
    public boolean isConnectionAlive() {
        return this.isAlive;
    }

    /**
     * Safely terminates the P2P instance.
     */
    public void shutdown() {
        if (!this.isAlive) {
            throw new IllegalStateException("P2P is not running");
        }

        this.isAlive = false;
    }

    /// <editor-fold desc="Common" defaultstate="collapsed">

    /// Packet format:
    /// +--------+------------+---------------+-------------+--------+
    /// | Header | Message ID | Packet Length | Packet Data | Footer |
    /// +--------+------------+---------------+-------------+--------+
    /// | 0x32   | short      | int32         | byte[]      | 0x64   |
    /// +--------+------------+---------------+-------------+--------+

    /**
     * This is first broadcast by the client to indicate a connection.
     * Fields:
     * - Room ID: long
     */
    public static final byte WAITING_FOR_CLIENT = 0x01;

    /**
     * This is sent by the server to indicate a client has connected with a matching room ID.
     * Fields:
     * - Client Address Length: int32
     * - Client Address String: byte[] (UTF-8)
     * - Client Port: int32
     */
    public static final byte NEW_CLIENT_CONNECTED = 0x02;

    /**
     * This is sent from clients to other clients.
     * This message has no fields; the data is arbitrary.
     */
    public static final byte USER_MESSAGE = 0x03;

    /**
     * This is sent from the client to the server to indicate a ping.
     * This message has no fields.
     */
    public static final byte SERVER_PING = 0x04;

    /**
     * This is sent from the server to the client to indicate a client has disconnected.
     * Fields:
     * - Client Address Length: int32
     * - Client Address String: byte[] (UTF-8)
     * - Client Port: int32
     */
    public static final byte OLD_CLIENT_DISCONNECTED = 0x05;

    /**
     * Waits for a message to be sent.
     *
     * @return The received message.
     */
    private DatagramPacket recv0() throws IOException {
        var packet = new DatagramPacket(this.buffer, this.buffer.length);
        this.socket.receive(packet);

        return packet;
    }

    /**
     * Sends a message to the server.
     *
     * @param message The message to send.
     * @throws IOException If an I/O error occurs.
     */
    private void send0(byte[] message) throws IOException {
        var packet = new DatagramPacket(message, message.length, this.serverAddress);
        this.socket.send(packet);
    }

    /**
     * Sends a message to a client.
     *
     * @param client The client to send the message to.
     * @param message The message to send.
     */
    private void send0(Client client, byte[] message) throws IOException {
        var packet = new DatagramPacket(message, message.length, client.asSocketAddress());
        this.socket.send(packet);
    }

    /**
     * Reads a message from a packet.
     *
     * @param packet The packet to read from.
     * @return The message data.
     */
    private Message readMessage(DatagramPacket packet) {
        var reader = ByteBuffer.wrap(packet.getData());

        // Read packet data.
        var header = reader.get();
        var packetId = reader.getShort();
        var length = reader.getInt();

        var data = new byte[length];
        reader.get(data);

        var footer = reader.get();

        // Perform input validation.
        if (header != 0x32) {
            throw new IllegalArgumentException("Header should be '50', instead '%s'".formatted(header));
        }
        if (footer != 0x64) {
            throw new IllegalArgumentException("Footer should be '100', instead '%s'".formatted(footer));
        }

        return new Message(packetId, data);
    }

    /**
     * Creates a client instance from a packet.
     *
     * @param packet The packet to create the client from.
     * @return The created client.
     */
    private Client getClient(DatagramPacket packet) {
        var address = packet.getAddress().toString();
        if (address.startsWith("/")) {
            address = address.substring(address.indexOf('/') + 1);
        }
        return new Client(address, packet.getPort());
    }

    /**
     * Creates a new message.
     *
     * @param messageId The ID of the message.
     * @param dataBuffer The data of the message.
     * @return The created message.
     */
    private byte[] newMessage(byte messageId, ByteBuffer dataBuffer) {
        var data = dataBuffer.array();
        var buffer = ByteBuffer.allocate(
                1 + // Header
                2 + // Message ID
                4 + // Packet Length

                data.length + // Data
                1 // Footer
        );

        buffer.put((byte) 0x32); // Header
        buffer.putShort(messageId); // Message ID
        buffer.putInt(data.length); // Packet Length

        buffer.put(data); // Data

        buffer.put((byte) 0x64); // Footer

        return buffer.array();
    }

    /// </editor-fold>

    /// <editor-fold desc="Discovery" defaultstate="collapsed">

    private long lastCheck = System.currentTimeMillis();
    private long timeout = TimeUnit.SECONDS.toMillis(30);

    /**
     * A map of Room ID -> Clients in the room.
     */
    private final Map<Long, Set<Client>> clients = new ConcurrentHashMap<>();

    /**
     * A map of Client -> Last ping timestamp.
     */
    private final Map<Client, Long> timestamps = new ConcurrentHashMap<>();

    /**
     * Sets the timeout for clients to be removed.
     *
     * @param timeout The timeout to set in milliseconds.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Broadcasts a discovery message to the network.
     */
    private void serverLoop() throws IOException {
        // Receive any packets.
        var packet = this.recv0();
        var message = this.readMessage(packet);
        var client = this.getClient(packet);

        // Handle the message.
        var reader = ByteBuffer.wrap(message.data);
        switch (message.packetId) {
            case WAITING_FOR_CLIENT -> {
                var roomId = reader.getLong();
                var clients = this.clients.computeIfAbsent(
                        roomId, k -> ConcurrentHashMap.newKeySet());

                // Broadcast to existing clients.
                var newClientMessage = this.newClientMessage(client);
                for (var existingClient : Set.copyOf(clients)) try {
                    // Notify existing clients of the new client.
                    this.send0(existingClient, newClientMessage);

                    // Notify the new client of the existing client.
                    var existingClientMessage = this.newClientMessage(existingClient);
                    this.send0(client, existingClientMessage);
                } catch (PortUnreachableException ignored) {
                    // Remove the client if the port is unreachable.
                    clients.remove(existingClient);
                }

                // Add the new client.
                clients.add(client);
            }
            case SERVER_PING ->
                    // Update the timestamp of the client.
                    this.timestamps.put(client, System.currentTimeMillis());
            default -> throw new IllegalArgumentException("Unknown packet ID '%s'".formatted(message.packetId));
        }
    }

    /**
     * Checks for timed out clients.
     */
    private void serverPing() throws IOException {
        // Check for timed out clients.
        var now = System.currentTimeMillis();
        if (this.lastCheck + 30_000 > now) {
            return;
        }

        this.lastCheck = now;

        // Remove timed out clients.
        for (var entry : this.clients.entrySet()) {
            var clients = entry.getValue();
            for (var client : Set.copyOf(clients)) {
                if (this.timestamps.containsKey(client)) {
                    var timestamp = this.timestamps.get(client);
                    if (timestamp + this.timeout < now) {
                        clients.remove(client);

                        // Notify the other clients of the forgotten client.
                        var forgottenClientMessage = this.oldClientMessage(client);
                        this.broadcastToRoom(entry.getKey(), forgottenClientMessage);
                    }
                } else {
                    // The client might not have pinged yet, set the timestamp.
                    this.timestamps.put(client, now);
                }
            }
        }
    }

    /**
     * Broadcasts a message to a room.
     *
     * @param roomId The ID of the room to broadcast to.
     * @param data The data to broadcast.
     */
    public void broadcastToRoom(long roomId, byte[] data) throws IOException {
        var clients = this.clients.get(roomId);
        if (clients == null) {
            return;
        }

        for (var client : Set.copyOf(clients)) try {
            this.send0(client, data);
        } catch (PortUnreachableException ignored) {
            // Remove the client if the port is unreachable.
            clients.remove(client);
        }
    }

    /**
     * Creates a message announcing a new client.
     *
     * @param client The client to announce.
     * @return The created message.
     */
    private byte[] newClientMessage(Client client) {
        return this.newMessage(NEW_CLIENT_CONNECTED, client.asBuffer());
    }

    /**
     * Creates a message announcing the removal of an old client.
     *
     * @param client The client to announce.
     * @return The created message.
     */
    private byte[] oldClientMessage(Client client) {
        return this.newMessage(OLD_CLIENT_DISCONNECTED, client.asBuffer());
    }

    /// </editor-fold>

    /// <editor-fold desc="Client" defaultstate="collapsed">

    private long lastPingTime = System.currentTimeMillis();

    private boolean hasInitialized = false;
    private final Set<Client> connectedClients = ConcurrentHashMap.newKeySet();

    private Consumer<Client> onClientConnected = client -> {};
    private Consumer<Client> onClientDisconnected = client -> {};
    private BiConsumer<Client, Message> onMessageReceived = (client, message) -> {};

    /**
     * Sets the callback for when a client connects.
     *
     * @param onClientConnected The callback to set.
     */
    public void onClientConnected(Consumer<Client> onClientConnected) {
        this.onClientConnected = onClientConnected;
    }

    /**
     * Sets the callback for when a client disconnects.
     *
     * @param onClientDisconnected The callback to set.
     */
    public void onClientDisconnected(Consumer<Client> onClientDisconnected) {
        this.onClientDisconnected = onClientDisconnected;
    }

    /**
     * Sets the callback for when a message is received.
     *
     * @param onMessageReceived The callback to set.
     */
    public void onMessageReceived(BiConsumer<Client, Message> onMessageReceived) {
        this.onMessageReceived = onMessageReceived;
    }

    /**
     * Sends a message to all connected clients.
     *
     * @param data The data to send.
     * @throws IOException If an I/O error occurs.
     */
    public void sendMessage(byte[] data) throws IOException {
        var message = this.userMessage(data);

        // Forward the data to all clients.
        for (var client : Set.copyOf(this.connectedClients)) try {
            this.send0(client, message);
        } catch (PortUnreachableException ignored) {
            // Remove the client if the port is unreachable.
            this.connectedClients.remove(client);
        }
    }

    /**
     * Forgets all connected clients.
     */
    public void forgetClients() {
        this.connectedClients.clear();
    }

    /**
     * Handles discovering other clients and connecting to them.
     */
    private void clientLoop() throws IOException {
        if (!this.hasInitialized) {
            this.send0(this.joinRoomMessage(this.roomId));
            this.hasInitialized = true;
            return;
        }

        // Receive any packets.
        var packet = this.recv0();
        var message = this.readMessage(packet);

        // Handle the message.
        var reader = ByteBuffer.wrap(message.data);
        switch (message.packetId) {
            case NEW_CLIENT_CONNECTED -> {
                var client = Client.fromBuffer(reader);
                this.connectedClients.add(client);
                this.onClientConnected.accept(client);
            }
            case USER_MESSAGE -> {
                var client = this.getClient(packet);
                this.onMessageReceived.accept(client, message);
            }
            case OLD_CLIENT_DISCONNECTED -> {
                var client = Client.fromBuffer(reader);
                this.connectedClients.remove(client);
                this.onClientDisconnected.accept(client);
            }
            default -> throw new IllegalArgumentException("Unknown packet ID '%s'".formatted(message.packetId));
        }
    }

    /**
     * Pings the server to keep the connection alive.
     */
    private void clientPing() throws IOException {
        if (this.lastPingTime + 5_000 > System.currentTimeMillis()) {
            return;
        }

        // Send a ping message to the server.
        this.send0(this.pingMessage());
        // Update the last ping time.
        this.lastPingTime = System.currentTimeMillis();
    }

    /**
     * @return A ping message.
     */
    private byte[] pingMessage() {
        return this.newMessage(SERVER_PING, ByteBuffer.allocate(0));
    }

    /**
     * Creates a message to join a room.
     *
     * @param roomId The ID of the room to join.
     * @return The created message.
     */
    private byte[] joinRoomMessage(long roomId) {
        var buffer = ByteBuffer.allocate(8);
        buffer.putLong(roomId);

        return this.newMessage(WAITING_FOR_CLIENT, buffer);
    }

    /**
     * Creates a user message.
     *
     * @param data The data to send.
     * @return The created message.
     */
    private byte[] userMessage(byte[] data) {
        return this.newMessage(USER_MESSAGE, ByteBuffer.wrap(data));
    }

    /// </editor-fold>
}
