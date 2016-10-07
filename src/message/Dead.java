package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 02.10.2016.
 */
public class Dead extends Message {

    public int replicaNumber;

    public Dead(@Nullable SocketAddress address, int replicaNumber) {
        super(address);
        this.replicaNumber = replicaNumber;
    }

    public static Dead parseMessage(SocketAddress address, Scanner sc) {
        return new Dead(address, sc.nextInt());
    }

    @Override
    public String toBeautifulString() {
        return "Dead " + replicaNumber;
    }

    @Override
    public String toString() {
        return "dead_message " + replicaNumber;
    }
}