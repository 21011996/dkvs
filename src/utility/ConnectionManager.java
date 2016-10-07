package utility;

import message.Message;
import replica.Replica;
import replica.ReplicaThread;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class ConnectionManager {

    private final int timeout;
    private final ReplicaThread thread;
    public final Map<SocketAddress, Connection> connections = new HashMap<>();
    private final ServerSocket serverSocket;
    private final ConnectionInput input = new ConnectionInput();
    private final ConnectionOutput output = new ConnectionOutput();
    private volatile boolean finished;

    public ConnectionManager(Replica replica, ReplicaThread thread) throws IOException {
        timeout = replica.getTimeout();
        this.thread = thread;
        serverSocket = new ServerSocket();
        serverSocket.bind(replica.getConfiguration()[replica.getReplicaNumber()]);
    }

    public void start() {
        input.start();
        output.start();
    }

    public void close() {
        finished = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            synchronized (System.err) {
                System.err.println("Exception while closing server socket: " + e.getMessage());
            }
        }
        output.interrupt();
        synchronized (connections) {
            connections.values().forEach(Connection::close);
        }
    }

    private class ConnectionInput extends Thread {
        @Override
        public void run() {
            while (!finished) {
                try {
                    Socket socket = serverSocket.accept();
                    SocketAddress address = socket.getRemoteSocketAddress();
                    synchronized (connections) {
                        if (connections.containsKey(address)) {
                            connections.get(address).close();
                        }
                        Connection connection = new Connection(socket, thread);
                        connections.put(address, connection);
                        connection.start();
                    }
                } catch (IOException e) {
                }
            }
        }
    }

    private class ConnectionOutput extends Thread {
        @Override
        public void run() {
            while (!finished) {
                Message message;
                try {
                    message = thread.get();
                } catch (InterruptedException e) {
                    close();
                    return;
                }
                synchronized (connections) {
                    SocketAddress address = message.address;
                    if (!connections.containsKey(address) || connections.get(address).isClosed()) {
                        Socket socket = new Socket();
                        try {
                            socket.connect(address, timeout);
                            Connection connection = new Connection(socket, thread);
                            connections.put(address, connection);
                            connection.start();
                        } catch (IOException e) {
                        }
                    }
                    if (connections.get(address) != null) {
                        connections.get(address).send(message);
                    }
                }
            }
        }
    }
}
