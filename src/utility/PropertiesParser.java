package utility;

import javafx.util.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Ilya239 on 15.09.2016.
 */
public class PropertiesParser {

    private static final String NUMBER_OF_REPLICAS = "number";
    private static final String TIMEOUT = "timeout";

    private int numberOfReplicas;
    private List<Pair<String, Integer>> addresses = new ArrayList<>();
    private int timeout;

    public PropertiesParser parseProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        if (is != null) {
            properties.load(is);
        }
        numberOfReplicas = Integer.parseInt(properties.getProperty(NUMBER_OF_REPLICAS));
        for (int i = 0; i < numberOfReplicas; i++) {
            String address = properties.getProperty("node." + (i + 1));
            String[] hostAndPort = address.split(":");
            if (hostAndPort.length != 2) {
                throw new IllegalArgumentException("Address of node must be in format host:port");
            }
            addresses.add(new Pair<>(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        timeout = Integer.parseInt(properties.getProperty(TIMEOUT));
        return this;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public Pair<String, Integer> getAddressOfNode(int nodeNumber) {
        return addresses.get(nodeNumber - 1);
    }

    public int getTimeout() {
        return timeout;
    }

}
