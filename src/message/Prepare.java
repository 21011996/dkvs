package message;

import com.sun.istack.internal.Nullable;
import utility.Entry;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class Prepare extends Message {

    public int viewNumber;
    public ClientRequest message;
    public int opNumber;
    public int commitNumber;
    public int senderIndex;
    public List<Entry> log;

    public Prepare(@Nullable SocketAddress address, int viewNumber, ClientRequest message, int opNumber,
                   int commitNumber, int senderIndex, List<Entry> log) {
        super(address);
        this.viewNumber = viewNumber;
        this.message = message;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
        this.senderIndex = senderIndex;
        this.log = log;
    }

    public static Prepare parseMessage(SocketAddress address, Scanner sc) {
        int viewNumber = sc.nextInt();
        if (!sc.next().equals("client_request")) {
            throw new IllegalArgumentException("No client request for prepare message");
        }
        return new Prepare(address, viewNumber, ClientRequest.parseMessage(address, sc), sc.nextInt(), sc.nextInt(), sc.nextInt(), getLog(sc));
    }

    private static List<Entry> getLog(Scanner sc) {
        int size = sc.nextInt();
        List<Entry> log = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            log.add(Entry.parseEntry(sc));
        }
        return log;
    }

    @Override
    public String toBeautifulString() {
        return "Prepare { viewNumber=" + viewNumber + ", clientServerRequest=" + message.toBeautifulString() +
                ", opNumber=" + opNumber + ", commitNumber=" + commitNumber + ", senderIndex=" +
                senderIndex + ", log.size()=" + log.size() + " }";
    }

    @Override
    public String toString() {
        String result = "prepare_message " + viewNumber + " " + message + " " + opNumber + " " + commitNumber + " " + senderIndex + " " + log.size() + " ";
        for (Entry entry : log) {
            result += entry + "\n";
        }
        result += opNumber + " " + commitNumber;
        return result;
    }
}
