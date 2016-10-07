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
public class RecoveryResponse extends Message {

    public int viewNumber;
    public int nonce;
    public List<Entry> log;
    public int opNumber;
    public int commitNumber;
    public int replicaNumber;

    public RecoveryResponse(@Nullable SocketAddress address, int viewNumber, int nonce, List<Entry> log, int opNumber,
                            int commitNumber, int replicaNumber) {
        super(address);
        this.viewNumber = viewNumber;
        this.nonce = nonce;
        this.log = log;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
        this.replicaNumber = replicaNumber;
    }

    public static RecoveryResponse parseMessage(SocketAddress address, Scanner sc) {
        return new RecoveryResponse(address, sc.nextInt(), sc.nextInt(), getLog(sc), sc.nextInt(), sc.nextInt(), sc.nextInt());
    }

    @Override
    public String toString() {
        String result = "recovery_response " + viewNumber + " " + nonce + " ";
        if (log == null) {
            result += "null ";
        } else {
            result += log.size() + " ";
            for (Entry entry : log) {
                result += entry + "\n";
            }
        }
        result += opNumber + " " + commitNumber + " " + replicaNumber;
        return result;
    }

    @Override
    public String toBeautifulString() {
        return "RecoveryResponse { viewNumber=" + viewNumber + ", nonce=" + nonce + ", log.size()=" + (log == null ? "null" : log.size()) +
                ", opNumber=" + opNumber + ", commitNumber=" + commitNumber + ", replicaNumber=" + replicaNumber + " }";
    }

    private static List<Entry> getLog(Scanner sc) {
        String tmp = sc.next();
        if (tmp.equals("null")) {
            return null;
        } else {
            int size = Integer.parseInt(tmp);
            List<Entry> log = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                log.add(Entry.parseEntry(sc));
            }
            return log;
        }
    }
}
