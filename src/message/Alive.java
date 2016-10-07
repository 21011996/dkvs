package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 07.10.2016.
 */
public class Alive extends Message {

    public int replicaNumber;

    public Alive(@Nullable SocketAddress address, int replicaNumber) {
        super(address);
        this.replicaNumber = replicaNumber;
    }

    public static Alive parseMessage(SocketAddress address, Scanner sc) {
        return new Alive(address, sc.nextInt());
    }

    @Override
    public String toBeautifulString() {
        return "Alive " + replicaNumber;
    }

    @Override
    public String toString() {
        return "alive_message " + replicaNumber;
    }
}
