package org.example;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class LamportDistributedMutex {
    static int N = 3;
    static int pertePct = 0;
    static int pannePct = 100;
    static int dureeSec = 18;
    static int delaiMin = 300;
    static int delaiMax = 800;
    static boolean verbose = true;

    enum TypeMessage { REQUEST, REPLY, RELEASE }

    static class Message {
        final TypeMessage type;
        final int horloge;
        final int idSource;
        final int reference; // horloge du REQUEST concerne

        Message(TypeMessage type, int horloge, int idSource, int reference) {
            this.type = type;
            this.horloge = horloge;
            this.idSource = idSource;
            this.reference = reference;
        }
    }

    static class Demande {
        final int horloge;
        final int id;

        Demande(int horloge, int id) {
            this.horloge = horloge;
            this.id = id;
        }
    }

    static class CanalReseau {
        final AtomicInteger messagesEnvoyes = new AtomicInteger();
        final AtomicInteger messagesLivres = new AtomicInteger();
        final AtomicInteger messagesPerdus = new AtomicInteger();
        final Map<String, ExecutorService> liens = new HashMap<>();

        CanalReseau(int n) {
            // Un canal mono-thread par paire garantit l'ordre FIFO.
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (i != j) {
                        liens.put(i + "-" + j, Executors.newSingleThreadExecutor());
                    }
                }
            }
        }

        void envoyerMessage(Processus src, Processus dest, Message msg) {
            messagesEnvoyes.incrementAndGet();
            ExecutorService lien = liens.get(src.id + "-" + dest.id);
            lien.execute(() -> {
                try {
                    int delai = ThreadLocalRandom.current()
                            .nextInt(delaiMin, delaiMax + 1);
                    Thread.sleep(delai);
                    boolean perdu = ThreadLocalRandom.current().nextInt(100) < pertePct;
                    if (perdu || dest.enPanne) {
                        messagesPerdus.incrementAndGet();
                        if (verbose) {
                            System.out.printf("[RESEAU] P%d -> P%d %s PERDU (%d ms)%n",
                                    src.id, dest.id, msg.type, delai);
                        }
                    } else {
                        dest.recevoirMessage(msg);
                        messagesLivres.incrementAndGet();
                        if (verbose) {
                            System.out.printf("[RESEAU] P%d -> P%d %s livre (%d ms)%n",
                                    src.id, dest.id, msg.type, delai);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        void fermer() {
            for (ExecutorService lien : liens.values()) {
                lien.shutdownNow();
            }
        }
    }

    static class Processus implements Runnable {
        final int id;
        final CanalReseau canal;
        final List<Processus> autres;
        final BlockingQueue<Message> boite = new LinkedBlockingQueue<>();
        final TreeSet<Demande> fileRequetes = new TreeSet<>(
                Comparator.comparingInt((Demande d) -> d.horloge)
                        .thenComparingInt(d -> d.id));
        final Set<Integer> reponsesRecues = new HashSet<>();
        final AtomicInteger enSCGlobal;
        final AtomicInteger violations;
        final AtomicInteger nbEntrees;
        final AtomicLong cumulAttenteMs;

        volatile boolean enPanne = false;
        volatile boolean actif = true;
        int horlogeLamport = 0;
        Demande maDemande = null;
        boolean enSC = false;
        long debutAttente;
        long finSection;
        long prochaineDemande;
        long prochainAvertissement;

        Processus(int id, CanalReseau canal, List<Processus> autres,
                  AtomicInteger enSCGlobal, AtomicInteger violations,
                  AtomicInteger nbEntrees, AtomicLong cumulAttenteMs) {
            this.id = id;
            this.canal = canal;
            this.autres = autres;
            this.enSCGlobal = enSCGlobal;
            this.violations = violations;
            this.nbEntrees = nbEntrees;
            this.cumulAttenteMs = cumulAttenteMs;
            this.prochaineDemande = System.currentTimeMillis()
                    + 200 + ThreadLocalRandom.current().nextInt(400);
        }

        void recevoirMessage(Message msg) {
            if (!enPanne && actif) boite.offer(msg);
        }

        void setEnPanne(boolean panne) {
            enPanne = panne;
        }

        private int tick() {
            return ++horlogeLamport;
        }

        private void majHorloge(int h) {
            horlogeLamport = Math.max(horlogeLamport, h) + 1;
        }

        private void diffuser(TypeMessage type, int reference) {
            int horlogeEnvoi = type == TypeMessage.REQUEST
                    ? reference : tick();
            Message msg = new Message(type, horlogeEnvoi, id, reference);
            for (Processus p : autres) {
                if (p.id != id) canal.envoyerMessage(this, p, msg);
            }
        }

        private void afficherFile(String raison) {
            if (!verbose) return;
            StringJoiner sj = new StringJoiner(", ", "[", "]");
            for (Demande d : fileRequetes) {
                sj.add("P" + d.id + "@t=" + d.horloge);
            }
            System.out.println("[FILE P" + id + "] " + raison + " " + sj);
        }

        private void traiterMessage(Message msg) {
            majHorloge(msg.horloge);
            switch (msg.type) {
                case REQUEST:
                    fileRequetes.add(new Demande(msg.reference, msg.idSource));
                    afficherFile("REQUEST de P" + msg.idSource);
                    // Une reponse est un accuse de reception, pas une autorisation.
                    int ts = tick();
                    canal.envoyerMessage(this, autres.get(msg.idSource),
                            new Message(TypeMessage.REPLY, ts, id, msg.reference));
                    break;
                case REPLY:
                    if (maDemande != null && msg.reference == maDemande.horloge) {
                        reponsesRecues.add(msg.idSource);
                    }
                    break;
                case RELEASE:
                    fileRequetes.removeIf(d -> d.id == msg.idSource
                            && d.horloge == msg.reference);
                    afficherFile("RELEASE de P" + msg.idSource);
                    break;
            }
        }

        private void demanderSectionCritique() {
            int ts = tick();
            maDemande = new Demande(ts, id);
            fileRequetes.add(maDemande);
            reponsesRecues.clear();
            debutAttente = System.currentTimeMillis();
            prochainAvertissement = debutAttente + 4000;
            if (verbose) System.out.println("P" + id + " REQUEST SC @t=" + ts);
            afficherFile("demande locale");
            diffuser(TypeMessage.REQUEST, ts);
        }

        private boolean peutEntrer() {
            return maDemande != null
                    && reponsesRecues.size() == N - 1
                    && !fileRequetes.isEmpty()
                    && fileRequetes.first().id == id;
        }

        private void entrerSectionCritique() {
            long attenteMs = System.currentTimeMillis() - debutAttente;
            cumulAttenteMs.addAndGet(attenteMs);
            nbEntrees.incrementAndGet();
            int simultanes = enSCGlobal.incrementAndGet();
            if (simultanes > 1) {
                violations.incrementAndGet();
                System.err.println("ERREUR : exclusion mutuelle violee !");
            }
            enSC = true;
            finSection = System.currentTimeMillis()
                    + 700 + ThreadLocalRandom.current().nextInt(500);
            System.out.printf("==> P%d ENTRE en SC (t=%d, attente=%d ms)%n",
                    id, horlogeLamport, attenteMs);
        }

        private void sortirSectionCritique() {
            enSC = false;
            enSCGlobal.decrementAndGet();
            System.out.printf("<== P%d QUITTE SC (t=%d)%n", id, horlogeLamport);
            fileRequetes.removeIf(d -> d.id == id
                    && maDemande != null && d.horloge == maDemande.horloge);
            afficherFile("sortie locale");
            diffuser(TypeMessage.RELEASE, maDemande.horloge);
            maDemande = null;
            prochaineDemande = System.currentTimeMillis()
                    + 400 + ThreadLocalRandom.current().nextInt(600);
        }

        @Override
        public void run() {
            try {
                while (actif) {
                    if (enPanne) {
                        if (enSC) {
                            enSC = false;
                            enSCGlobal.decrementAndGet();
                            System.out.println("P" + id + " tombe en panne PENDANT SC");
                        }
                        boite.clear();
                        Thread.sleep(50);
                        continue;
                    }
                    Message m = boite.poll(60, TimeUnit.MILLISECONDS);
                    if (m != null) traiterMessage(m);
                    long maintenant = System.currentTimeMillis();
                    if (enSC && maintenant >= finSection) {
                        sortirSectionCritique();
                    }
                    if (!enSC && maDemande != null) {
                        if (peutEntrer()) {
                            entrerSectionCritique();
                        } else if (maintenant >= prochainAvertissement) {
                            System.out.println("P" + id
                                    + " ATTEND (REPLY manquant ou demande precedente)");
                            prochainAvertissement = maintenant + 4000;
                        }
                    }
                    if (!enSC && maDemande == null
                            && maintenant >= prochaineDemande) {
                        demanderSectionCritique();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (enSC) enSCGlobal.decrementAndGet();
            }
        }
    }

    static int valeur(String[] args, String nom, int defaut) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(nom)) return Integer.parseInt(args[i + 1]);
        }
        return defaut;
    }

    static void sauvegarderCsv(int entrees, double attente, CanalReseau canal)
            throws IOException {
        Path fichier = Paths.get("lamport_resultats.csv");
        if (!Files.exists(fichier)) {
            Files.write(fichier,
                    ("processus;duree_s;perte_pct;panne_pct;messages_envoyes;"
                            + "messages_livres;messages_perdus;entrees_SC;attente_moyenne_ms\n")
                            .getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        String ligne = String.format(Locale.US, "%d;%d;%d;%d;%d;%d;%d;%d;%.1f%n",
                N, dureeSec, pertePct, pannePct,
                canal.messagesEnvoyes.get(), canal.messagesLivres.get(),
                canal.messagesPerdus.get(), entrees, attente);
        Files.write(fichier, ligne.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public static void main(String[] args) throws Exception {
        N = valeur(args, "--n", 3);
        pertePct = valeur(args, "--perte", 0);
        pannePct = valeur(args, "--panne", 100);
        dureeSec = valeur(args, "--duree", 18);
        delaiMin = valeur(args, "--delai-min", 300);
        delaiMax = valeur(args, "--delai-max", 800);
        verbose = !Arrays.asList(args).contains("--silencieux");

        if (N < 3 || N > 10 || pertePct < 0 || pertePct > 100
                || pannePct < 0 || pannePct > 100 || dureeSec < 1
                || delaiMin < 0 || delaiMax < delaiMin) {
            throw new IllegalArgumentException("Parametres invalides : "
                    + "3 <= n <= 10, probabilites 0..100, duree >= 1");
        }

        CanalReseau canal = new CanalReseau(N);
        AtomicInteger enSCGlobal = new AtomicInteger();
        AtomicInteger violations = new AtomicInteger();
        AtomicInteger entrees = new AtomicInteger();
        AtomicLong sommeAttente = new AtomicLong();
        List<Processus> tous = new ArrayList<>();
        for (int i = 0; i < N; i++) tous.add(null);
        for (int i = 0; i < N; i++) {
            tous.set(i, new Processus(i, canal, tous,
                    enSCGlobal, violations, entrees, sommeAttente));
        }

        System.out.printf("TP3 Partie B | N=%d | perte=%d%% | panne=%d%% | duree=%ds%n",
                N, pertePct, pannePct, dureeSec);
        List<Thread> threads = new ArrayList<>();
        for (Processus p : tous) {
            Thread thread = new Thread(p, "Processus-" + p.id);
            threads.add(thread);
            thread.start();
        }

        if (dureeSec > 8) {
            Thread.sleep(8000);
            // Une panne possible de P1, a t=8 secondes.
            if (ThreadLocalRandom.current().nextInt(100) < pannePct) {
                tous.get(1).setEnPanne(true);
                System.out.println("\n!!! PANNE : P1 ne repond plus !!!\n");
            } else {
                System.out.println("\n[SIMULATION] Aucune panne declenchee.\n");
            }
            Thread.sleep((dureeSec - 8) * 1000L);
        } else {
            Thread.sleep(dureeSec * 1000L);
        }

        for (Processus p : tous) p.actif = false;
        for (Thread t : threads) t.join(1500);
        canal.fermer();

        int nb = entrees.get();
        double moyenne = nb == 0 ? 0 : (double) sommeAttente.get() / nb;
        int envoi = canal.messagesEnvoyes.get();
        int livres = canal.messagesLivres.get();
        int perdus = canal.messagesPerdus.get();
        System.out.println("\n===== STATISTIQUES =====");
        System.out.println("Nombre de processus : " + N);
        System.out.println("Messages envoyes    : " + envoi);
        System.out.println("Messages livres     : " + livres);
        System.out.println("Messages perdus     : " + perdus);
        System.out.println("En transit a l'arret: " + (envoi - livres - perdus));
        System.out.println("Entrees en SC       : " + nb);
        System.out.printf(Locale.US, "Attente moyenne     : %.1f ms%n", moyenne);
        System.out.println("Violations mutex    : " + violations.get());
        System.out.println("========================");
        sauvegarderCsv(nb, moyenne, canal);
        System.out.println("Resultats ajoutes a lamport_resultats.csv");
    }
}
