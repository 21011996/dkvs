package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class Ping extends Message {

    public int leaderIndex;
    public int commitNumber;

    public Ping(@Nullable SocketAddress address, int leaderIndex, int commitNumber) {
        super(address);
        this.leaderIndex = leaderIndex;
        this.commitNumber = commitNumber;
    }

    public static Ping parseMessage(SocketAddress address, Scanner sc) {
        return new Ping(address, sc.nextInt(), sc.nextInt());
    }

    @Override
    public String toBeautifulString() {
        return "Ping";
    }

    @Override
    public String toString() {
        return "ping_message " + leaderIndex + " " + commitNumber;
    }
}
