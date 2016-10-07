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
public class StartView extends Message {

    public int viewNumber;
    public int leaderIndex;
    public List<Entry> log;
    public int opNumber;
    public int commitNumber;

    public StartView(@Nullable SocketAddress address, int viewNumber, int leaderIndex, List<Entry> log, int opNumber, int commitNumber) {
        super(address);
        this.viewNumber = viewNumber;
        this.leaderIndex = leaderIndex;
        this.log = log;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }

    public static StartView parseMessage(SocketAddress address, Scanner sc) {
        return new StartView(address, sc.nextInt(), sc.nextInt(), getLog(sc), sc.nextInt(), sc.nextInt());
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
        return "StartView { viewNumber=" + viewNumber + ", leaderIndex" + leaderIndex + ", log.size()=" + log.size() +
                ", opNumber=" + opNumber + ", commitNumber=" + commitNumber + " }";
    }

    @Override
    public String toString() {
        String result = "start_view " + viewNumber + " " + leaderIndex + " " + log.size() + " ";
        for (Entry entry : log) {
            result += entry + "\n";
        }
        result += opNumber + " " + commitNumber;
        return result;
    }
}
