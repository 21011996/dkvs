import message.ClientRequest;
import message.ServerResponse;
import message.Message;
import utility.Operation;
import utility.PropertiesParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Ilya239 on 07.09.2016.
 */
public class Client {
    public static void main(String[] args) throws IOException {
        PropertiesParser propertiesParser = new PropertiesParser().parseProperties("dkvs.properties");
        InetSocketAddress[] addresses = new InetSocketAddress[propertiesParser.getNumberOfReplicas()];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = new InetSocketAddress(propertiesParser.getAddressOfNode(i + 1).getKey(),
                    propertiesParser.getAddressOfNode(i + 1).getValue());
        }

        int requestNumber = 0;
        Scanner sc = new Scanner(System.in);
        Socket socket;
        boolean connected = false;
        Scanner input = null;
        PrintWriter output = null;
        while (true) {
            String cmd = sc.next();
            switch (cmd) {
                case "connect":
                    try {
                        if (connected) {
                            output.write("end");
                            output.close();
                            input.close();
                            connected = false;
                        }
                        socket = new Socket();
                        int replicaNumber = sc.nextInt();
                        if (replicaNumber < addresses.length) {
                            socket.connect(addresses[replicaNumber]);
                            input = new Scanner(socket.getInputStream());
                            output = new PrintWriter(socket.getOutputStream());
                            connected = true;
                            System.out.println("CONNECTED");
                        } else {
                            throw new IllegalArgumentException("Node number is greater then expected number of nodes");
                        }
                    } catch (IOException e) {
                        System.err.println("Error while connecting: " + e.getMessage());
                        System.err.flush();
                    }
                    break;
                case "exit":
                    if (connected) {
                        output.write("END");
                        output.close();
                        input.close();
                    }
                    return;
                case "ping":
                    if (connected) {
                        requestNumber++;
                        send(output, new ClientRequest(null, Operation.PING, null, null, new ArrayList<InetSocketAddress>(), requestNumber));
                        Message m = receive(input);
                        if (m != null && m instanceof ServerResponse && ((ServerResponse) m).success) {
                            System.out.println("PONG");
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "close":
                    if (connected) {
                        requestNumber++;
                        send(output, new ClientRequest(null, Operation.CLOSE, null, null, new ArrayList<InetSocketAddress>(), requestNumber));
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "get":
                    if (connected) {
                        requestNumber++;
                        send(output, new ClientRequest(null, Operation.GET, sc.next(), null, new ArrayList<InetSocketAddress>(), requestNumber));
                        Message m = receive(input);
                        if (m != null && m instanceof ServerResponse) {
                            if (((ServerResponse) m).success) {
                                System.out.println(((ServerResponse) m).result);
                            } else {
                                System.out.println((((ServerResponse) m).result.equals("?") ? "NOT_FOUND" : ((ServerResponse) m).result));
                            }
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "set":
                    if (connected) {
                        requestNumber++;
                        String key = sc.next();
                        String value = sc.nextLine().substring(1);
                        send(output, new ClientRequest(null, Operation.SET, key, value, new ArrayList<InetSocketAddress>(), requestNumber));
                        Message m = receive(input);
                        if (m != null && m instanceof ServerResponse) {
                            if (((ServerResponse) m).success) {
                                System.out.println("STORED");
                            } else {
                                requestNumber = checkReason((ServerResponse) m, Operation.SET, key, value, requestNumber, output, input);
                            }
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                case "delete":
                    if (connected) {
                        requestNumber++;
                        String key = sc.next();
                        send(output, new ClientRequest(null, Operation.DELETE, key, null, new ArrayList<InetSocketAddress>(), requestNumber));
                        Message m = receive(input);
                        if (m != null && m instanceof ServerResponse) {
                            if (((ServerResponse) m).success) {
                                System.out.println("DELETED");
                            } else {
                                requestNumber = checkReason((ServerResponse) m, Operation.DELETE, key, null, requestNumber, output, input);
                            }
                        } else {
                            System.out.println("Something bad has happened.");
                        }
                    } else {
                        System.out.println("Not connected!");
                    }
                    break;
                default:
                    System.out.println("Unrecognized command!");
            }
        }
    }

    private static int checkReason(ServerResponse response, Operation operation, String key, String value, int requestNumber, PrintWriter output, Scanner input) {
        int resultRequestNumber = requestNumber;
        String reason = response.result;
        boolean success = false;
        while (!success && reason.contains(":")) {
            resultRequestNumber = Integer.parseInt(reason.split(":")[1]) + 1;
            send(output, new ClientRequest(null, operation, key, value, new ArrayList<InetSocketAddress>(), resultRequestNumber));
            Message againMessage = receive(input);
            if (againMessage != null && againMessage instanceof ServerResponse) {
                success = ((ServerResponse) againMessage).success;
                reason = ((ServerResponse) againMessage).result;
            }
        }
        if (!success && !reason.contains(":")) {
            System.out.println((reason.equals("?") ? "NOT_FOUND" : reason));
        }
        if (success) {
            if (operation == Operation.SET) {
                System.out.println("STORED");
            } else {
                System.out.println("DELETED");
            }
        }
        return resultRequestNumber;
    }

    private static void send(PrintWriter writer, Message message) {
        writer.write("message " + message + "\n");
        writer.flush();
    }

    private static Message receive(Scanner sc) {
        String next = sc.next();
        if (next.equals("message")) {
            return Message.parseMessage(null, sc);
        } else {
            return null;
        }
    }
}
