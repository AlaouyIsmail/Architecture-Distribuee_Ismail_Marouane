package org.example;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class Message {
    enum Type { REQUEST, REPLY, RELEASE }
    final Type type;
    final int senderId;
    final int timestamp;

    Message(Type type, int senderId, int timestamp) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }
}

class Processus extends Thread {
    private final int id;
    private int clock = 0;
    private List<Processus> autres;

    private final BlockingQueue<Message> fileMessages =
            new LinkedBlockingQueue<>();

    private final PriorityQueue<Message> fileDemandes =
            new PriorityQueue<>(
                    Comparator.comparingInt((Message m) -> m.timestamp)
                            .thenComparingInt(m -> m.senderId)
            );

    private final Set<Integer> reponsesRecues = new HashSet<>();
    private final AtomicInteger processusTermines;

    // Verification experimentale de l'exclusion mutuelle
    private static final AtomicInteger dansSC = new AtomicInteger(0);

    Processus(int id, AtomicInteger processusTermines) {
        this.id = id;
        this.processusTermines = processusTermines;
    }

    public void setAutres(List<Processus> autres) {
        this.autres = autres;
    }

    private int tick() {
        return ++clock;
    }

    private void majHorloge(int timestamp) {
        clock = Math.max(clock, timestamp) + 1;
    }

    public void recoitMessage(Message m) {
        fileMessages.add(m);
    }

    private void envoyer(Message.Type type, Processus dest) {
        int timestamp = tick();
        dest.recoitMessage(new Message(type, id, timestamp));
    }

    private void broadcast(Message.Type type, int timestamp) {
        for (Processus p : autres) {
            if (p.id != id) {
                p.recoitMessage(new Message(type, id, timestamp));
            }
        }
    }

    private Processus getProcessus(int pid) {
        return autres.get(pid);
    }

    private void traiterMessage(Message m) {
        majHorloge(m.timestamp);

        switch (m.type) {
            case REQUEST:
                fileDemandes.add(m);
                System.out.println("P" + id + " recoit REQUEST de P"
                        + m.senderId + " (t=" + clock + ")");
                envoyer(Message.Type.REPLY, getProcessus(m.senderId));
                break;

            case REPLY:
                reponsesRecues.add(m.senderId);
                System.out.println("P" + id + " recoit REPLY de P"
                        + m.senderId);
                break;

            case RELEASE:
                fileDemandes.removeIf(
                        r -> r.senderId == m.senderId
                );
                System.out.println("P" + id + " recoit RELEASE de P"
                        + m.senderId);
                break;
        }
    }

    private void traiterMessages() throws InterruptedException {
        Message m = fileMessages.poll(50, TimeUnit.MILLISECONDS);

        if (m != null) {
            traiterMessage(m);
        }
    }

    private boolean autoriseEntrer() {
        Message premier = fileDemandes.peek();

        return reponsesRecues.size() == autres.size()
                && premier != null
                && premier.senderId == id;
    }

    private void demandeSC() throws InterruptedException {
        int timestamp = tick();

        Message demande =
                new Message(Message.Type.REQUEST, id, timestamp);

        fileDemandes.add(demande);
        reponsesRecues.clear();
        reponsesRecues.add(id);

        System.out.println("\nP" + id + " demande SC (t="
                + timestamp + ")");

        broadcast(Message.Type.REQUEST, timestamp);

        while (!autoriseEntrer()) {
            traiterMessages();
        }

        entrerSC();
    }

    private void entrerSC() throws InterruptedException {
        int nombre = dansSC.incrementAndGet();

        if (nombre > 1) {
            System.err.println("ERREUR : exclusion mutuelle violee !");
        }

        System.out.println("==> P" + id
                + " ENTRE en SC (t=" + clock + ")");

        try {
            Thread.sleep(1000);
        } finally {
            sortirSC();
        }
    }

    private void sortirSC() {
        System.out.println("<== P" + id
                + " QUITTE la SC (t=" + clock + ")");

        dansSC.decrementAndGet();

        fileDemandes.removeIf(r -> r.senderId == id);

        int timestamp = tick();
        broadcast(Message.Type.RELEASE, timestamp);
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 1; i++) {
                long attente = 300
                        + ThreadLocalRandom.current().nextInt(600);

                long fin = System.currentTimeMillis() + attente;

                while (System.currentTimeMillis() < fin) {
                    traiterMessages();
                }

                demandeSC();
            }

            processusTermines.incrementAndGet();

            // Continuer a repondre aux autres processus
            while (processusTermines.get() < autres.size()
                    || !fileMessages.isEmpty()) {
                traiterMessages();
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

public class LamportExclusion {
    public static void main(String[] args)
            throws InterruptedException {

        int N = 3;
        AtomicInteger termines = new AtomicInteger(0);
        List<Processus> liste = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            liste.add(new Processus(i, termines));
        }

        for (Processus p : liste) {
            p.setAutres(liste);
        }

        System.out.println("=== TP3 : Algorithme de Lamport ===");

        for (Processus p : liste) {
            p.start();
        }

        for (Processus p : liste) {
            p.join();
        }

        System.out.println("\n=== Simulation terminee ===");
    }
}
