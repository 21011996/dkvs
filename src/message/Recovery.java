package message;

import com.sun.istack.internal.Nullable;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Created by Ilya239 on 06.09.2016.
 */
public class Recovery extends Message {

    public int replicaNumber;
    public int nonce;

    public Recovery(@Nullable SocketAddress address, int replicaNumber, int nonce) {
        super(address);
        this.replicaNumber = replicaNumber;
        this.nonce = nonce;
    }

    public static Recovery parseMessage(SocketAddress address, Scanner sc) {
        return new Recovery(address, sc.nextInt(), sc.nextInt());
    }

    @Override
    public String toString() {
        return "recovery_message " + replicaNumber + " " + nonce;
    }

    @Override
    public String toBeautifulString() {
        return "Recovery { replicaNumber = " + replicaNumber + ", nonce = " + nonce + " }";
    }
}
