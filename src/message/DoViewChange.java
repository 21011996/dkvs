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
public class DoViewChange extends Message {

    public int viewNumber;
    public List<Entry> log;
    public int latestNormalViewIndex;
    public int opNumber;
    public int commitNumber;
    public int replicaIndex;

    public DoViewChange(@Nullable SocketAddress address, int viewNumber, List<Entry> log, int latestNormalViewNumber,
                        int opNumber, int commitNumber, int replicaIndex) {
        super(address);
        this.viewNumber = viewNumber;
        this.log = log;
        this.latestNormalViewIndex = latestNormalViewNumber;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
        this.replicaIndex = replicaIndex;
    }

    public static DoViewChange parseMessage(SocketAddress address, Scanner sc) {
        return new DoViewChange(address, sc.nextInt(), getLog(sc), sc.nextInt(), sc.nextInt(), sc.nextInt(), sc.nextInt());
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
        return "DoViewChange { viewNumber=" + viewNumber + ", log.size()=" + log.size() +
                ", latestNormalViewIndex=" + latestNormalViewIndex + ", opNumber=" + opNumber +
                ", commitNumber=" + commitNumber + ", replicaIndex=" + replicaIndex + " }";
    }

    @Override
    public String toString() {
        String result = "do_view_change " + viewNumber + " " + log.size() + " ";
        for (Entry entry : log) {
            result += entry + "\n";
        }
        result += latestNormalViewIndex + " " + opNumber + " " + commitNumber + " " + replicaIndex;
        return result;
    }
}
