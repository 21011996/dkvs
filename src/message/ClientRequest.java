package message;

import com.sun.istack.internal.Nullable;
import utility.Operation;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class ClientRequest extends Message {

    public final Operation operation;
    public final String key;
    public final String value;
    public final List<InetSocketAddress> redirections;
    public final int requestNumber;

    public ClientRequest(@Nullable SocketAddress address, Operation operation, @Nullable String key,
                         @Nullable String value, List<InetSocketAddress> redirections, int requestNumber) {
        super(address);
        if (operation != Operation.PING && operation != Operation.CLOSE && key == null) {
            throw new IllegalArgumentException("Key can be null only for Ping operation");
        } else if (operation == Operation.SET && value == null) {
            throw new IllegalArgumentException("No value for Set operation");
        }
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.redirections = redirections;
        this.requestNumber = requestNumber;
    }

    public static ClientRequest parseMessage(SocketAddress address, Scanner sc) {
        return new ClientRequest(address, Operation.valueOf(sc.next()), sc.next(), sc.nextLine().substring(1), getRedirections(sc), sc.nextInt());
    }

    private static List<InetSocketAddress> getRedirections(Scanner sc) {
        List<InetSocketAddress> redirections = new ArrayList<>();
        int n = sc.nextInt();
        for (int i = 0; i < n; i++) {
            redirections.add(new InetSocketAddress(sc.next(), sc.nextInt()));
        }
        return redirections;
    }

    @Override
    public String toBeautifulString() {
        String result = "ClientRequest { operation=" + operation +
                ", key=" + (key == null ? "?" : key) + ", value=" + (value == null ? "?" : value) + ", redirections=[";
        for (int i = 0; i < redirections.size(); i++) {
            result += redirections.get(i) + (i == redirections.size() - 1 ? "" : ", ");
        }
        return result + "] }";
    }

    @Override
    public String toString() {
        String result = "client_request " + operation + " " + (key == null ? "?" : key) +
                " " + (value == null ? "?" : value) + "\n" + redirections.size() + " ";
        for (InetSocketAddress redirection : redirections) {
            result += redirection.getHostName() + " " + redirection.getPort() + " ";
        }
        result += requestNumber;
        return result;
    }
}
