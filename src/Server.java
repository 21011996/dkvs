import replica.Replica;
import replica.ReplicaThread;
import utility.PropertiesParser;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Ilya239 on 07.09.2016.
 */
public class Server {
    private static final String FILENAME = "dkvs.properties";

    public static void main(String[] args) {
        if (args == null) {
            System.err.println("Usage: Sever numberOfNodes nodeNumber");
        } else if (args.length == 2) {
            int nodeNumber = Integer.parseInt(args[1]);
            runNode(FILENAME, nodeNumber);
        } else {
            System.err.println("Usage: Sever numberOfNodes nodeNumber");
        }
    }

    private static void runNode(String fileName, int nodeNumber) {
        try {
            PropertiesParser propertiesParser = new PropertiesParser().parseProperties(fileName);
            if (nodeNumber > propertiesParser.getNumberOfReplicas()) {
                throw new IllegalArgumentException("Node number is greater then expected number of nodes");
            }
            InetSocketAddress[] addresses = new InetSocketAddress[propertiesParser.getNumberOfReplicas()];
            for (int i = 0; i < addresses.length; i++) {
                addresses[i] = new InetSocketAddress(propertiesParser.getAddressOfNode(i + 1).getKey(),
                        propertiesParser.getAddressOfNode(i + 1).getValue());
            }
            Replica replica = new Replica(addresses, propertiesParser.getNumberOfReplicas(), nodeNumber, propertiesParser.getTimeout());
            ReplicaThread thread = new ReplicaThread(replica);
            thread.run();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            System.err.println("Can't parse properties file, got Exception: " + e.getMessage());
        }
    }
}
