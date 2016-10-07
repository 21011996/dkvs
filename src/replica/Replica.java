package replica;

import utility.Entry;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * Created by Ilya239 on 05.09.2016.
 */
public class Replica {

    private InetSocketAddress[] configuration;
    private int numberOfReplicas;
    private int replicaNumber;
    private int viewNumber;
    private ReplicaStatus replicaStatus;
    private int opNumber;
    private int commitNumber;
    private int clientTableNumber;
    private int timeout;
    private int leader;
    private long lastMessageTime;
    private ReplicaState replicaState;
    private Map<Integer, Integer> opNumberToPrepareOk;
    private int lastNormalViewNumber;
    private int numberOfStartViewChangeMessages;
    private Set<Integer> replicasStartViewChangeMessagesReceivedFrom;
    private int numberOfDoViewChangeMessages;
    private Set<Integer> replicasDoViewChangeMessagesReceivedFrom;
    private int maxNormalViewNumber;
    private int maxOpNumber;
    private List<Entry> logInViewChange;
    private int maxCommitNumber;
    private Map<Integer, Integer> nonce;
    private Map<Integer, List<Entry>> nonceToLog;
    private Map<Integer, Integer> nonceToCommitNumber;

    public Replica(InetSocketAddress[] configuration, int numberOfReplicas, int replicaNumber, int timeout) {
        this.configuration = configuration;
        this.numberOfReplicas = numberOfReplicas;
        this.replicaNumber = replicaNumber;
        viewNumber = 0;
        replicaStatus = ReplicaStatus.NORMAL;
        opNumber = 0;
        commitNumber = 0;
        clientTableNumber = 0;
        this.timeout = timeout;
        leader = -1;
        opNumberToPrepareOk = new HashMap<>();
        lastNormalViewNumber = 0;
        numberOfStartViewChangeMessages = 0;
        replicasStartViewChangeMessagesReceivedFrom = new HashSet<>();
        numberOfDoViewChangeMessages = 0;
        replicasDoViewChangeMessagesReceivedFrom = new HashSet<>();
        maxNormalViewNumber = 0;
        maxOpNumber = 0;
        logInViewChange = new ArrayList<>();
        maxCommitNumber = 0;
        nonce = new HashMap<>();
        nonceToLog = new HashMap<>();
        nonceToCommitNumber = new HashMap<>();
    }

    public InetSocketAddress[] getConfiguration() {
        return configuration;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public int getViewNumber() {
        return viewNumber;
    }

    public void setViewNumber(int viewNumber) {
        this.viewNumber = viewNumber;
    }

    public ReplicaStatus getReplicaStatus() {
        return replicaStatus;
    }

    public void setReplicaStatus(ReplicaStatus replicaStatus) {
        this.replicaStatus = replicaStatus;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public void setOpNumber(int opNumber) {
        this.opNumber = opNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    public void setCommitNumber(int commitNumber) {
        this.commitNumber = commitNumber;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public int getLeader() {
        return leader;
    }

    public long getLastMessageTime() {
        return lastMessageTime;
    }

    public void setLastMessageTime(long lastMessageTime) {
        this.lastMessageTime = lastMessageTime;
    }

    public ReplicaState getReplicaState() {
        return replicaState;
    }

    public void setReplicaState(ReplicaState replicaState) {
        this.replicaState = replicaState;
    }

    public int getClientTableNumber() {
        return clientTableNumber;
    }

    public void setClientTableNumber(int clientTableNumber) {
        this.clientTableNumber = clientTableNumber;
    }

    public void addPrepareOk(Integer opNumber) {
        if (!opNumberToPrepareOk.containsKey(opNumber)) {
            opNumberToPrepareOk.put(opNumber, 1);
        } else {
            opNumberToPrepareOk.put(opNumber, opNumberToPrepareOk.get(opNumber) + 1);
        }
    }

    public Integer getPrepareOk(Integer opNumber) {
        return opNumberToPrepareOk.get(opNumber);
    }

    public int getLastNormalViewNumber() {
        return lastNormalViewNumber;
    }

    public void setLastNormalViewNumber(int lastNormalViewNumber) {
        this.lastNormalViewNumber = lastNormalViewNumber;
    }

    public int getNumberOfStartViewChangeMessages() {
        return numberOfStartViewChangeMessages;
    }

    public void setNumberOfStartViewChangeMessages(int numberOfStartViewChangeMessages) {
        this.numberOfStartViewChangeMessages = numberOfStartViewChangeMessages;
    }

    public int getNumberOfDoViewChangeMessages() {
        return numberOfDoViewChangeMessages;
    }

    public void setNumberOfDoViewChangeMessages(int numberOfDoViewChangeMessages) {
        this.numberOfDoViewChangeMessages = numberOfDoViewChangeMessages;
    }

    public int getMaxNormalViewNumber() {
        return maxNormalViewNumber;
    }

    public void setMaxNormalViewNumber(int maxNormalViewNumber) {
        this.maxNormalViewNumber = maxNormalViewNumber;
    }

    public int getMaxOpNumber() {
        return maxOpNumber;
    }

    public void setMaxOpNumber(int maxOpNumber) {
        this.maxOpNumber = maxOpNumber;
    }

    public List<Entry> getLogInViewChange() {
        return logInViewChange;
    }

    public void setLogInViewChange(List<Entry> logInViewChange) {
        this.logInViewChange = logInViewChange;
    }

    public int getMaxCommitNumber() {
        return maxCommitNumber;
    }

    public void setMaxCommitNumber(int maxCommitNumber) {
        this.maxCommitNumber = maxCommitNumber;
    }

    public Set<Integer> getReplicasStartViewChangeMessagesReceivedFrom() {
        return replicasStartViewChangeMessagesReceivedFrom;
    }

    public void setReplicasStartViewChangeMessagesReceivedFrom(Set<Integer> replicasStartViewChangeMessagesReceivedFrom) {
        this.replicasStartViewChangeMessagesReceivedFrom = replicasStartViewChangeMessagesReceivedFrom;
    }

    public Set<Integer> getReplicasDoViewChangeMessagesReceivedFrom() {
        return replicasDoViewChangeMessagesReceivedFrom;
    }

    public void setReplicasDoViewChangeMessagesReceivedFrom(Set<Integer> replicasDoViewChangeMessagesReceivedFrom) {
        this.replicasDoViewChangeMessagesReceivedFrom = replicasDoViewChangeMessagesReceivedFrom;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public void addVoiceForNonce(int curNonce, List<Entry> curLog, int commitNumber) {
        nonce.putIfAbsent(curNonce, 0);
        nonceToCommitNumber.putIfAbsent(curNonce, 0);
        nonce.put(curNonce, nonce.get(curNonce) + 1);
        nonceToLog.putIfAbsent(curNonce, null);
        if (curLog != null) {
            nonceToLog.put(curNonce, curLog);
            nonceToCommitNumber.put(curNonce, commitNumber);
        }
    }

    public int getVoiceForNonce(int curNonce) {
        return nonce.get(curNonce);
    }

    public List<Entry> getLogForNonce(int curNonce) {
        return nonceToLog.get(curNonce);
    }

    public int getCommitNumberForNonce(int curNonce) {
        return nonceToCommitNumber.get(curNonce);
    }
}
