package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class PrepareOk extends Message {

    public int viewNumber;
    public int opNumber;
    public int replicaNumber;

    public PrepareOk(@Nullable SocketAddress address, int viewNumber, int opNumber, int replicaNumber) {
        super(address);
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.replicaNumber = replicaNumber;
    }

    public static PrepareOk parseMessage(SocketAddress address, Scanner sc) {
        return new PrepareOk(address, sc.nextInt(), sc.nextInt(), sc.nextInt());
    }

    @Override
    public String toBeautifulString() {
        return "PrepareOk { viewNumber=" + viewNumber + ", opNumber=" + opNumber + ", replicaNumber=" + replicaNumber + " }";
    }

    @Override
    public String toString() {
        return "prepare_ok_message " + viewNumber + " " + opNumber + " " + replicaNumber;
    }
}
