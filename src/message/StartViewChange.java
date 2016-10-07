package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class StartViewChange extends Message {

    public int viewNumber;
    public int replicaIndex;

    public StartViewChange(@Nullable SocketAddress address, int viewNumber, int replicaIndex) {
        super(address);
        this.viewNumber = viewNumber;
        this.replicaIndex = replicaIndex;
    }

    public static StartViewChange parseMessage(SocketAddress address, Scanner sc) {
        return new StartViewChange(address, sc.nextInt(), sc.nextInt());
    }

    @Override
    public String toBeautifulString() {
        return "StartViewChange { viewNumber=" + viewNumber + ", replicaIndex=" + replicaIndex + " }";
    }

    @Override
    public String toString() {
        return "start_view_change " + viewNumber + " " + replicaIndex;
    }
}
