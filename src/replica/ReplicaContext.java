package replica;

import message.ClientRequest;
import message.ServerResponse;
import utility.Entry;
import utility.Operation;

import java.io.*;
import java.util.*;

/**
 * Created by Ilya239 on 05.09.2016.
 */
public class ReplicaContext {

    private final Map<String, String> data = new HashMap<>();
    private List<Entry> log = new ArrayList<>();
    private final Writer writer;
    private final Map<Integer, ClientRequest> requests = new HashMap<>();
    private int commitNumber = 0;
    private int opNumber = 0;

    public ReplicaContext(Replica replica) throws IOException {
        String logFile = "dkvs_" + (replica.getReplicaNumber()) + ".log";
        writer = new FileWriter(logFile, true);
        readLog(logFile);
    }

    public void add(Entry entry, ClientRequest request) {
        log.add(entry);
        requests.put(log.size() - 1, request);
    }

    public void add(Entry entry) {
        log.add(entry);
    }

    public int size() {
        return log.size();
    }

    public List<Entry> getLog() {
        return log;
    }

    public void setLog(List<Entry> log) {
        this.log = log;
    }

    public boolean logContains(Entry entry) {
        return log.contains(entry);
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    public String get(String key) {
        return data.get(key);
    }

    public List<ServerResponse> commit(int lastApplied, int commitNext) throws IOException {
        List<ServerResponse> responses = new ArrayList<>();
        for (int i = lastApplied; i < commitNext; i++) {
            ServerResponse response = applyEntry(i);
            if (response != null) {
                responses.add(response);
            }
            writer.write(log.get(i) + "\n");
        }
        writer.flush();
        return responses;
    }

    public void close() throws IOException {
        writer.close();
    }

    private void readLog(String fileName) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(fileName));
        while (sc.hasNext()) {
            Entry entry = Entry.parseEntry(sc);
            log.add(entry);
            applyEntry(log.size() - 1);
            commitNumber++;
            if (entry.opNumber > opNumber) {
                opNumber = entry.opNumber;
            }
        }
        sc.close();
    }

    private ServerResponse applyEntry(int number) {
        Entry entry = log.get(number);
        if (entry.operation.equals(Operation.SET)) {
            data.put(entry.key, entry.value);
            if (requests.containsKey(number)) {
                return new ServerResponse(requests.get(number).address, requests.get(number).operation,
                        true, null, requests.get(number).redirections);
            }
        } else if (entry.operation.equals(Operation.DELETE)) {
            boolean success = data.containsKey(entry.key);
            data.remove(entry.key);
            if (requests.containsKey(number)) {
                return new ServerResponse(requests.get(number).address, requests.get(number).operation,
                        success, null, requests.get(number).redirections);
            }
        }
        return null;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    public int getOpNumber() {
        return opNumber;
    }
}
