package replica;

import message.*;
import utility.ConnectionManager;
import utility.Entry;
import utility.Operation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Created by Ilya239 on 05.09.2016.
 */
public class ReplicaThread extends Thread {

    private HashMap<InetSocketAddress, Boolean> dead;
    private final Replica replica;
    private final ConnectionManager manager;
    private final Queue<Message> input = new ArrayDeque<>();
    private final Queue<Message> output = new ArrayDeque<>();

    private final ReplicaContext replicaContext;

    private volatile boolean isStopped;

    public ReplicaThread(Replica replica) throws IOException {
        this.dead = new HashMap<>();
        for (InetSocketAddress address : replica.getConfiguration()) {
            this.dead.put(address, false);
        }
        this.replica = replica;
        manager = new ConnectionManager(replica, this);
        replicaContext = new ReplicaContext(replica);
        this.replica.setCommitNumber(replicaContext.getCommitNumber());
        this.replica.setOpNumber(replicaContext.getOpNumber());
        this.replica.setLeader(0);
        this.replica.setReplicaState(ReplicaState.FOLLOWER);
        this.replica.setReplicaStatus(ReplicaStatus.NORMAL);
    }

    public void add(Message message) {
        synchronized (input) {
            input.add(message);
            input.notify();
        }
    }

    public Message get() throws InterruptedException {
        while (true) {
            synchronized (output) {
                if (!output.isEmpty()) {
                    return output.poll();
                }
                output.wait();
            }
        }
    }

    private Message getMessage() throws InterruptedException {
        int timeout = getTimeout();
        while (true) {
            synchronized (input) {
                if (!input.isEmpty()) {
                    return input.poll();
                } else if (System.currentTimeMillis() - replica.getLastMessageTime() >= timeout) {
                    return null;
                } else {
                    input.wait(timeout - (System.currentTimeMillis() - replica.getLastMessageTime()));
                }
            }
        }
    }

    @Override
    public void run() {
        manager.start();
        replica.setLastMessageTime(System.currentTimeMillis());
        for (int i = 0; i < replica.getNumberOfReplicas(); i++) {
            if (i != replica.getReplicaNumber()) {
                send(new Alive(replica.getConfiguration()[i], replica.getReplicaNumber()));
            }
        }
        while (!isStopped) {
            try {
                Message message = getMessage();
                if (message == null) {
                    afterTimeout();
                } else {
                    processMessage(message);
                }
            } catch (InterruptedException | IOException e) {
                try {
                    close();
                } catch (IOException e1) {
                    System.err.println("Error while closing replica context");
                }
                return;
            }
        }
    }

    private void processMessage(Message message) throws IOException {
        System.out.println(getStatus() + " Message received (" + message.address + "): " + message.toBeautifulString());
        if (message instanceof ClientRequest) {
            processClientServerRequest((ClientRequest) message);
        } else if (message instanceof ServerResponse) {
            processClientServerResponse((ServerResponse) message);
        } else if (message instanceof Prepare) {
            processPrepareMessage((Prepare) message);
        } else if (message instanceof PrepareOk) {
            processPrepareOkMessage((PrepareOk) message);
        } else if (message instanceof Commit) {
            processCommitMessage((Commit) message);
        } else if (message instanceof StartViewChange) {
            processStartViewChangeMessage((StartViewChange) message);
        } else if (message instanceof DoViewChange) {
            processDoViewChangeMessage((DoViewChange) message);
        } else if (message instanceof StartView) {
            processStartViewMessage((StartView) message);
        } else if (message instanceof Ping) {
            processPingMessage((Ping) message);
        } else if (message instanceof Recovery) {
            processRecoveryMessage((Recovery) message);
        } else if (message instanceof RecoveryResponse) {
            processRecoveryResponseMessage((RecoveryResponse) message);
        } else if (message instanceof Dead) {
            processDeadMessage((Dead) message);
        } else if (message instanceof Alive) {
            processAliveMessage((Alive) message);
        } else {
            throw new AssertionError();
        }
    }

    private void processDeadMessage(Dead message) {
        dead.put(replica.getConfiguration()[message.replicaNumber], true);
    }

    private void processAliveMessage(Alive message) {
        dead.put(replica.getConfiguration()[message.replicaNumber], false);
    }

    private void processRecoveryMessage(Recovery recovery) {
        if (replica.getReplicaStatus() != ReplicaStatus.NORMAL) {
            return;
        }
        if (replica.getReplicaState() != ReplicaState.LEADER) {
            RecoveryResponse message = new RecoveryResponse(recovery.address, replica.getViewNumber(),
                    recovery.nonce, null, 0, 0, replica.getReplicaNumber());
            send(message);
        } else {
            RecoveryResponse message = new RecoveryResponse(recovery.address, replica.getViewNumber(),
                    recovery.nonce, replicaContext.getLog(), replica.getOpNumber(), replica.getCommitNumber(),
                    replica.getReplicaNumber());
            send(message);
        }
    }

    private void processRecoveryResponseMessage(RecoveryResponse message) throws IOException {
        replica.addVoiceForNonce(message.nonce, message.log, message.commitNumber);
        if (replica.getVoiceForNonce(message.nonce) > replica.getNumberOfReplicas() / 2) {
            List<Entry> log = replica.getLogForNonce(message.nonce);
            if (log != null) {
                replicaContext.setLog(log);
                replicaContext.commit(replica.getCommitNumber(), replica.getCommitNumberForNonce(message.nonce));
                replica.setCommitNumber(replica.getCommitNumberForNonce(message.nonce));
                replica.setReplicaStatus(ReplicaStatus.NORMAL);
            }
        }
    }

    private void processClientServerRequest(ClientRequest request) throws IOException {
        if (replica.getReplicaStatus() != ReplicaStatus.NORMAL) {
            ClientRequest r = new ClientRequest(replica.getConfiguration()[replica.getReplicaNumber()],
                    request.operation, request.key, request.value, request.redirections, request.requestNumber);
            if (r.redirections.size() == 0) {
                r.redirections.add((InetSocketAddress) request.address);
            }
            send(r);
            return;
        }
        switch (request.operation) {
            case PING:
                send(new ServerResponse(request.address, Operation.PING, true, null, request.redirections));
                return;
            case GET:
                send(new ServerResponse(request.address, Operation.GET,
                        replicaContext.containsKey(request.key), replicaContext.get(request.key), request.redirections));
                return;
            case CLOSE:
                for (int i = 0; i < replica.getNumberOfReplicas(); i++) {
                    if (i != replica.getReplicaNumber()) {
                        send(new Dead(replica.getConfiguration()[i], replica.getReplicaNumber()));
                    }
                }
                this.close();
                return;
            case SET:
            case DELETE:
                if (replica.getReplicaState() == ReplicaState.LEADER) {
                    if (replica.getClientTableNumber() >= request.requestNumber) {
                        send(new ServerResponse(request.address, request.operation, false,
                                "Request number is less or equals then request number of previous processed request, previous number:" +
                                        replica.getClientTableNumber(), request.redirections));
                        return;
                    }
                    replica.setOpNumber(replica.getOpNumber() + 1);
                    replicaContext.add(new Entry(request.operation, request.key, request.value, replica.getOpNumber()), request);
                    replica.setClientTableNumber(request.requestNumber);
                    for (int i = 0; i < replica.getConfiguration().length; i++) {
                        Prepare prepare = new Prepare(replica.getConfiguration()[i], replica.getViewNumber(),
                                request, replica.getOpNumber(), replica.getCommitNumber(), replica.getReplicaNumber(), replicaContext.getLog());
                        send(prepare);
                    }
                } else if (replica.getLeader() != -1) {
                    ClientRequest r = new ClientRequest(replica.getConfiguration()[replica.getLeader()],
                            request.operation, request.key, request.value, request.redirections, request.requestNumber);
                    r.redirections.add((InetSocketAddress) request.address);
                    send(r);
                }
                return;
            default:
                throw new AssertionError();
        }
    }

    private void processClientServerResponse(ServerResponse response) {
        ServerResponse r = new ServerResponse(response.redirections.get(response.redirections.size() - 1),
                response.operation, response.success, response.result, response.redirections);
        r.redirections.remove(r.redirections.size() - 1);
        send(r);
    }

    private void processPingMessage(Ping ping) {
        replica.setLastMessageTime(System.currentTimeMillis());
        replica.setLeader(ping.leaderIndex);
        if (replica.getReplicaStatus() == ReplicaStatus.RECOVERING) {
            return;
        }
        if (replica.getCommitNumber() < ping.commitNumber) {
            replica.setReplicaStatus(ReplicaStatus.RECOVERING);
            int nonce = (int) System.currentTimeMillis();
            for (int i = 0; i < replica.getNumberOfReplicas(); i++) {
                if (i != replica.getReplicaNumber()) {
                    Recovery recovery = new Recovery(replica.getConfiguration()[i],
                            replica.getReplicaNumber(), nonce);
                    send(recovery);
                }
            }
        }
    }

    private void processPrepareMessage(Prepare prepare) {
        replica.setLastMessageTime(System.currentTimeMillis());
        if (replica.getReplicaStatus() != ReplicaStatus.NORMAL) {
            return;
        }
        if (replica.getReplicaNumber() != prepare.senderIndex) {
            if (replica.getViewNumber() < prepare.viewNumber) {
                replica.setViewNumber(prepare.viewNumber);
            }
            replica.setLeader(prepare.senderIndex);
            replica.setOpNumber(prepare.opNumber);
            List<Entry> entries = prepare.log;
            entries.stream().filter(entry -> entry.opNumber < prepare.opNumber && !replicaContext.logContains(entry)).forEach(replicaContext::add);
            replicaContext.add(new Entry(prepare.message.operation, prepare.message.key, prepare.message.value, replica.getOpNumber()), prepare.message);
            replica.setClientTableNumber(prepare.message.requestNumber);
        }
        PrepareOk prepareOk = new PrepareOk(replica.getConfiguration()[replica.getLeader()], replica.getViewNumber(),
                replica.getOpNumber(), replica.getReplicaNumber());
        send(prepareOk);
    }

    private void processPrepareOkMessage(PrepareOk prepareOkMessage) throws IOException {
        if (replica.getReplicaStatus() != ReplicaStatus.NORMAL) {
            return;
        }
        replica.addPrepareOk(prepareOkMessage.opNumber);
        int prepareOk = replica.getPrepareOk(prepareOkMessage.opNumber);
        if (prepareOk > replica.getNumberOfReplicas() / 2) {
            List<ServerResponse> responses = replicaContext.commit(replica.getCommitNumber(), prepareOkMessage.opNumber);
            //replicaContext.commit(replica.getCommitNumber(), prepareOkMessage.opNumber);
            replica.setCommitNumber(prepareOkMessage.opNumber);
            responses.forEach(this::send);
            for (int i = 0; i < replica.getConfiguration().length; i++) {
                if (i != replica.getReplicaNumber()) {
                    Commit commit = new Commit(replica.getConfiguration()[i], replica.getViewNumber(), prepareOkMessage.opNumber);
                    send(commit);
                }
            }
        }
    }

    private void processCommitMessage(Commit commit) throws IOException {
        replica.setLastMessageTime(System.currentTimeMillis());
        if (replica.getReplicaStatus() != ReplicaStatus.NORMAL) {
            return;
        }
        if (replica.getCommitNumber() < commit.commitNumber) {
            replicaContext.commit(replica.getCommitNumber(), commit.commitNumber);
            replica.setCommitNumber(commit.commitNumber);
        }
    }

    private void processStartViewChangeMessage(StartViewChange message) {
        if (replica.getReplicaStatus() == ReplicaStatus.RECOVERING) {
            return;
        }
        if (message.viewNumber > replica.getViewNumber()) {
            voteForLeader(message.viewNumber, message);
        } else if (message.viewNumber == replica.getViewNumber()) {
            if (!replica.getReplicasStartViewChangeMessagesReceivedFrom().contains(message.replicaIndex)) {
                replica.getReplicasStartViewChangeMessagesReceivedFrom().add(message.replicaIndex);
                replica.setNumberOfStartViewChangeMessages(replica.getNumberOfStartViewChangeMessages() + 1);
                if (replica.getNumberOfStartViewChangeMessages() > replica.getNumberOfReplicas() / 2) {
                    DoViewChange doViewChange = new DoViewChange(replica.getConfiguration()[(replica.getViewNumber() - 1) % replica.getNumberOfReplicas()],
                            replica.getViewNumber(), replicaContext.getLog(), replica.getLastNormalViewNumber(),
                            replica.getOpNumber(), replica.getCommitNumber(), replica.getReplicaNumber());
                    send(doViewChange);
                }
            }
        }
    }

    private void processDoViewChangeMessage(DoViewChange message) throws IOException {
        if (replica.getReplicaStatus() == ReplicaStatus.RECOVERING || replica.getReplicaState() == ReplicaState.LEADER) {
            return;
        }
        if (message.viewNumber > replica.getViewNumber()) {
            voteForLeader(message.viewNumber, message);
        } else if (message.viewNumber == replica.getViewNumber()) {
            if (message.latestNormalViewIndex > replica.getMaxNormalViewNumber()) {
                replica.setMaxNormalViewNumber(message.latestNormalViewIndex);
                replica.setMaxOpNumber(message.opNumber);
                replica.setLogInViewChange(message.log);
            } else if (message.latestNormalViewIndex == replica.getMaxNormalViewNumber() && message.opNumber > replica.getOpNumber()) {
                replica.setMaxOpNumber(message.opNumber);
                replica.setLogInViewChange(message.log);
            }
            if (message.commitNumber > replica.getMaxCommitNumber()) {
                replica.setMaxCommitNumber(message.commitNumber);
            }
            if (!replica.getReplicasDoViewChangeMessagesReceivedFrom().contains(message.replicaIndex)) {
                replica.getReplicasDoViewChangeMessagesReceivedFrom().add(message.replicaIndex);
                replica.setNumberOfDoViewChangeMessages(replica.getNumberOfDoViewChangeMessages() + 1);
                if (replica.getNumberOfDoViewChangeMessages() > replica.getNumberOfReplicas() / 2) {
                    if (replica.getViewNumber() < message.viewNumber) {
                        replica.setViewNumber(message.viewNumber);
                    }
                    if (replica.getLogInViewChange().size() > replicaContext.size()) {
                        replicaContext.setLog(replica.getLogInViewChange());
                    }
                    if (replica.getMaxOpNumber() > replica.getOpNumber()) {
                        replica.setOpNumber(replica.getMaxOpNumber());
                    }
                    replica.setReplicaState(ReplicaState.LEADER);
                    System.out.println("Becoming leader");
                    replica.setLeader(replica.getReplicaNumber());
                    replica.setReplicaStatus(ReplicaStatus.NORMAL);
                    for (int i = 0; i < replica.getNumberOfReplicas(); i++) {
                        StartView startView = new StartView(replica.getConfiguration()[i], replica.getViewNumber(),
                                replica.getReplicaNumber(), replica.getLogInViewChange(), replica.getOpNumber(), replica.getCommitNumber());
                        send(startView);
                    }
                    replicaContext.commit(replica.getCommitNumber(), replica.getMaxCommitNumber());
                    replica.setCommitNumber(replica.getMaxCommitNumber());
                }
            }
        }
    }

    private void processStartViewMessage(StartView message) {
        replica.setLastMessageTime(System.currentTimeMillis());
        if (replica.getReplicaStatus() == ReplicaStatus.NORMAL) {
            return;
        }
        if (message.log.size() > replicaContext.size()) {
            replicaContext.setLog(message.log);
        }
        if (message.opNumber > replica.getOpNumber()) {
            replica.setOpNumber(message.opNumber);
        }
        replica.setViewNumber(message.viewNumber);
        replica.setLeader(message.leaderIndex);
        replica.setReplicaStatus(ReplicaStatus.NORMAL);
        replica.setReplicaState(ReplicaState.FOLLOWER);
        if (message.commitNumber < message.log.size()) {
            PrepareOk prepareOk = new PrepareOk(replica.getConfiguration()[replica.getLeader()],
                    replica.getViewNumber(), message.log.get(message.commitNumber).opNumber, replica.getReplicaNumber());
            send(prepareOk);
        }
    }

    private void voteForLeader(int newViewNumber, Message reasonMessage) {
        if (replica.getReplicaStatus() == ReplicaStatus.NORMAL) {
            replica.setReplicaStatus(ReplicaStatus.VIEW_CHANGE);
            if (replica.getReplicaState() != ReplicaState.CANDIDATE) {
                replica.setReplicaState(ReplicaState.CANDIDATE);
                System.out.println("Converting to candidate.");
            }
            replica.setLeader(-1);
            replica.setLastNormalViewNumber(replica.getViewNumber());
        }
        replica.setViewNumber(newViewNumber);
        if (reasonMessage == null) {
            replica.setNumberOfStartViewChangeMessages(0);
            replica.setReplicasStartViewChangeMessagesReceivedFrom(new HashSet<>());
            replica.setNumberOfDoViewChangeMessages(0);
            replica.setReplicasDoViewChangeMessagesReceivedFrom(new HashSet<>());
            replica.setMaxCommitNumber(0);
            replica.setMaxOpNumber(0);
            replica.setMaxNormalViewNumber(0);
        } else if (reasonMessage instanceof StartViewChange) {
            StartViewChange startViewChange = (StartViewChange) reasonMessage;
            replica.setNumberOfStartViewChangeMessages(1);
            Set<Integer> receivedFrom = new HashSet<>();
            receivedFrom.add(startViewChange.replicaIndex);
            replica.setReplicasStartViewChangeMessagesReceivedFrom(receivedFrom);
            replica.setNumberOfDoViewChangeMessages(0);
            replica.setReplicasDoViewChangeMessagesReceivedFrom(new HashSet<>());
            replica.setMaxCommitNumber(0);
            replica.setMaxOpNumber(0);
            replica.setMaxNormalViewNumber(0);
        } else if (reasonMessage instanceof DoViewChange) {
            DoViewChange doViewChange = (DoViewChange) reasonMessage;
            Set<Integer> receivedFrom = new HashSet<>();
            receivedFrom.add(doViewChange.replicaIndex);
            replica.setNumberOfStartViewChangeMessages(1);
            replica.setReplicasStartViewChangeMessagesReceivedFrom(receivedFrom);
            replica.setNumberOfDoViewChangeMessages(1);
            replica.setReplicasDoViewChangeMessagesReceivedFrom(receivedFrom);
            replica.setMaxCommitNumber(doViewChange.commitNumber);
            replica.setMaxOpNumber(doViewChange.opNumber);
            replica.setMaxNormalViewNumber(doViewChange.latestNormalViewIndex);
        }
        for (int i = 0; i < replica.getNumberOfReplicas(); i++) {
            StartViewChange message = new StartViewChange(replica.getConfiguration()[i],
                    replica.getViewNumber(), replica.getReplicaNumber());
            send(message);
        }
    }

    private void afterTimeout() throws IOException {
        if (replica.getReplicaState() == ReplicaState.LEADER) {
            replica.setLastMessageTime(System.currentTimeMillis());
            for (int i = 0; i < replica.getNumberOfReplicas(); i++) {
                if (i != replica.getReplicaNumber()) {
                    send(new Ping(replica.getConfiguration()[i], replica.getReplicaNumber(), replica.getCommitNumber()));
                }
            }
        } else {
            voteForLeader(replica.getViewNumber() + 1, null);
        }
        replica.setLastMessageTime(System.currentTimeMillis());
    }

    private int getTimeout() {
        if (replica.getReplicaStatus() == ReplicaStatus.VIEW_CHANGE) {
            return replica.getTimeout() * 2;
        }
        return replica.getReplicaState() == ReplicaState.LEADER ? replica.getTimeout() / 4 : replica.getTimeout();
    }

    public void close() throws IOException {
        isStopped = true;
        manager.close();
        replicaContext.close();
        interrupt();
    }

    private void send(Message message) {
        if (!dead.containsKey(message.address) || !dead.get(message.address)) {
            System.out.println(getStatus() + " Sending message (" + message.address + "): " + message.toBeautifulString());
            synchronized (output) {
                output.add(message);
                output.notify();
            }
        }
    }

    private String getStatus() {
        return "[Replica " + replica.getReplicaNumber() + "][" + replica.getReplicaStatus() + "," + replica.getReplicaState() + ",VIEW_NUMBER " + replica.getViewNumber() + "]";
    }
}
