package message;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 05.09.2016.
 */
public abstract class Message {

    public final SocketAddress address;

    public Message(SocketAddress address) {
        this.address = address;
    }

    public static Message parseMessage(SocketAddress address, Scanner sc) {
        String s = sc.next();
        switch (s) {
            case "client_request":
                return ClientRequest.parseMessage(address, sc);
            case "server_response":
                return ServerResponse.parseMessage(address, sc);
            case "commit_message":
                return Commit.parseMessage(address, sc);
            case "do_view_change":
                return DoViewChange.parseMessage(address, sc);
            case "ping_message":
                return Ping.parseMessage(address, sc);
            case "prepare_message":
                return Prepare.parseMessage(address, sc);
            case "prepare_ok_message":
                return PrepareOk.parseMessage(address, sc);
            case "start_view_change":
                return StartViewChange.parseMessage(address, sc);
            case "start_view":
                return StartView.parseMessage(address, sc);
            case "recovery_message":
                return Recovery.parseMessage(address, sc);
            case "recovery_response":
                return RecoveryResponse.parseMessage(address, sc);
            case "dead_message":
                return Dead.parseMessage(address, sc);
            case "alive_message":
                return Alive.parseMessage(address, sc);
            default:
                return null;
        }
    }

    public abstract String toBeautifulString();
}
