package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class Commit extends Message {

    public int viewNumber;
    public int commitNumber;

    public Commit(@Nullable SocketAddress address, int viewNumber, int commitNumber) {
        super(address);
        this.viewNumber = viewNumber;
        this.commitNumber = commitNumber;
    }

    public static Commit parseMessage(SocketAddress address, Scanner sc) {
        return new Commit(address, sc.nextInt(), sc.nextInt());
    }

    @Override
    public String toBeautifulString() {
        return "Commit { viewNumber=" + viewNumber + ", commitNumber=" + commitNumber + " }";
    }

    @Override
    public String toString() {
        return "commit_message " + viewNumber + " " + commitNumber;
    }
}
