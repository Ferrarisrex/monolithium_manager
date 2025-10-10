package com.ferra;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;

/**
 * MonolithCoordinator
 *
 * Usage:
 *  Server: java -jar monolith.jar --server [--port 5555] [--chunk 1000000]
 *  Client: java -jar monolith.jar --client <server-ip> [--port 5555]
 *
 * Protocol:
 *   Client -> Server: "REQUEST\n"
 *   Server -> Client: "ASSIGN <start>\n"  OR "NO_MORE\n"
 *   Client runs Rust process with start and chunk size, collects lines beginning "json "
 *   Client -> Server: for each top monolith: "MONOLITH <json>\n"
 *   After all: "DONE\n"
 *
 * Server stores results in monoliths.xml (keeps largest 100 by area).
 */
public class MonolithCoordinator {

    // Configurable defaults
    static final int DEFAULT_PORT = 5555;
    static final long CHUNK_SIZE = 100_000_000L;
    static final String XML_FILE = "monoliths.xml";
    static final int KEEP_TOP = 100;

    public static void main(String[] args) throws Exception {
        boolean serverMode = false;
        boolean clientMode = false;
        String serverHost = null;
        int port = DEFAULT_PORT;
        long chunkSize = CHUNK_SIZE;


        // simple arg parse
        for (int i=0;i<args.length;i++){
            switch(args[i]){
                case "--server": serverMode = true; break;
                case "--client":
                    clientMode = true;
                    if (i+1 < args.length) { serverHost = args[++i]; } else {
                        System.err.println("Provide server ip after --client");
                        return;
                    }
                    break;
                case "--port":
                    if (i+1 < args.length) port = Integer.parseInt(args[++i]);
                    else { System.err.println("Missing port"); return; }
                    break;
                case "--chunk":
                    if (i+1 < args.length) chunkSize = Long.parseLong(args[++i]);
                    else { System.err.println("Missing chunk size"); return; }
                    break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
            }
        }

        if (serverMode == clientMode) {
            System.out.println("Run with either --server or --client <server-ip>");
            return;
        }

        if (serverMode) {
            System.out.println("Starting server on port " + port + " with chunkSize=" + chunkSize);
            new Server(port, chunkSize).start();
        } else {
            System.out.println("Starting client connecting to " + serverHost + ":" + port + " chunkSize=" + chunkSize);
            new Client(serverHost, port, chunkSize).runLoop();
        }
    }

    /* --------------------------------------
       Server Implementation
       -------------------------------------- */

    static class Server {
        private final int port;
        private final long chunkSize;
        private final Path completedFile = Paths.get("completed.txt");
        private final Set<Long> completed = Collections.synchronizedSet(new HashSet<>());

        private final ExecutorService pool = Executors.newCachedThreadPool();

        // server state
        private long nextStart = 0L; // next unassigned start
        private final Set<Long> inProgress = Collections.synchronizedSet(new HashSet<>());
        private final List<Monolith> topMonoliths = Collections.synchronizedList(new ArrayList<>());

        Server(int port, long chunkSize) {
            this.port = port;
            this.chunkSize = chunkSize;
            loadXml(); // load existing monoliths from file if present
            loadCompleted();
        }

        void start() throws IOException {
            ServerSocket ss = new ServerSocket(port);
            System.out.println("Server listening on " + port);
            while (true) {
                Socket s = ss.accept();
                System.out.println("Client connected: " + s.getRemoteSocketAddress());
                pool.submit(() -> handleClient(s));
            }
        }

        private void handleClient(Socket s) {
            try (
                Socket sock = s;
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
            ) {
                String line = in.readLine();
                if (line == null) return;
                if (!line.trim().equals("REQUEST")) {
                    out.write("ERR invalid\n"); out.flush(); return;
                }

                // assign next chunk
                long start;
                synchronized(this) {
                    // find next uncompleted range
                    while (completed.contains(nextStart)) {
                        nextStart += chunkSize;
                    }
                    start = nextStart;
                    nextStart += chunkSize;
                    inProgress.add(start);
                }

                out.write("ASSIGN " + start + "\n"); out.flush();
                System.out.println("Assigned start=" + start + " to " + sock.getRemoteSocketAddress());

                // now read MONOLITH <json> lines until DONE
                while (true) {
                    String r = in.readLine();
                    if (r == null) {
                        System.out.println("Client disconnected unexpectedly: " + sock.getRemoteSocketAddress());
                        // release assignment for reassign later (in-memory only)
                        inProgress.remove(start);
                        break;
                    }
                    if (r.equals("DONE")) {
                        inProgress.remove(start);
                        completed.add(start);
                        saveCompleted();
                        System.out.println("Client DONE for start=" + start + " from " + sock.getRemoteSocketAddress());
                        break;
                    }

                    if (r.startsWith("MONOLITH ")) {
                        String json = r.substring("MONOLITH ".length()).trim();
                        Monolith m = Monolith.fromJson(json);
                        if (m != null) {
                            addMonolithAndPrune(m);
                            System.out.println("Received monolith area=" + m.area + " seed=" + m.seed);
                        } else {
                            System.out.println("Failed parsing json: " + json);
                        }
                    } else {
                        System.out.println("Unknown message: " + r);
                    }
                }

            } catch (IOException e) {
                System.err.println("Client handler error: " + e.getMessage());
            }
        }

        // synchronized update to topMonoliths + persist to XML
        private synchronized void addMonolithAndPrune(Monolith m) {
            topMonoliths.add(m);
            topMonoliths.sort(Comparator.comparingLong(Monolith::getArea).reversed());
            if (topMonoliths.size() > KEEP_TOP) {
                topMonoliths.subList(KEEP_TOP, topMonoliths.size()).clear();
            }
            saveXml();
        }

        // XML load/save (simple format)
        private void loadXml() {
            Path p = Paths.get(XML_FILE);
            if (!Files.exists(p)) return;
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(p.toFile());
                NodeList list = doc.getElementsByTagName("monolith");
                for (int i=0;i<list.getLength();i++){
                    Element e = (Element) list.item(i);
                    Monolith m = new Monolith(
                            Long.parseLong(e.getAttribute("area")),
                            Long.parseLong(e.getAttribute("seed")),
                            Integer.parseInt(e.getAttribute("minx")),
                            Integer.parseInt(e.getAttribute("maxx")),
                            Integer.parseInt(e.getAttribute("minz")),
                            Integer.parseInt(e.getAttribute("maxz"))
                    );
                    topMonoliths.add(m);
                }
                topMonoliths.sort(Comparator.comparingLong(Monolith::getArea).reversed());
                if (topMonoliths.size() > KEEP_TOP) topMonoliths.subList(KEEP_TOP, topMonoliths.size()).clear();
                System.out.println("Loaded " + topMonoliths.size() + " monoliths from " + XML_FILE);
            } catch (Exception ex) {
                System.err.println("Failed reading " + XML_FILE + ": " + ex.getMessage());
            }
        }
        private void loadCompleted() {
            if (Files.exists(completedFile)) {
                try {
                    List<String> lines = Files.readAllLines(completedFile);
                    for (String line : lines) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        completed.add(Long.parseLong(line));
                    }
                    System.out.println("Loaded " + completed.size() + " completed ranges from " + completedFile);
                } catch (IOException e) {
                    System.err.println("Failed to read completed ranges: " + e.getMessage());
                }
            }
        }


        private void saveXml() {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.newDocument();
                Element root = doc.createElement("monoliths");
                doc.appendChild(root);

                for (Monolith m : topMonoliths) {
                    Element e = doc.createElement("monolith");
                    e.setAttribute("area", Long.toString(m.area));
                    e.setAttribute("seed", Long.toString(m.seed));
                    e.setAttribute("minx", Integer.toString(m.minx));
                    e.setAttribute("maxx", Integer.toString(m.maxx));
                    e.setAttribute("minz", Integer.toString(m.minz));
                    e.setAttribute("maxz", Integer.toString(m.maxz));
                    root.appendChild(e);
                }

                Transformer t = TransformerFactory.newInstance().newTransformer();
                t.setOutputProperty(OutputKeys.INDENT, "yes");

                // Write to a temporary file first for atomic replace
                Path xmlPath = Paths.get(XML_FILE);
                Path tmp = xmlPath.resolveSibling(XML_FILE + ".tmp");
                try (FileOutputStream fos = new FileOutputStream(tmp.toFile())) {
                    t.transform(new DOMSource(doc), new StreamResult(fos));
                }

                Files.move(tmp, xmlPath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);

                System.out.println("Saved " + topMonoliths.size() + " monoliths to " + XML_FILE);
            } catch (Exception ex) {
                System.err.println("Failed to save XML: " + ex.getMessage());
            }
        }

        private void saveCompleted() {
            try {
                List<String> lines = new ArrayList<>();
                for (Long s : completed) lines.add(s.toString());

                // Write to a temporary file first for atomic replace
                Path tmp = completedFile.resolveSibling("completed.tmp");
                Files.write(tmp, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Files.move(tmp, completedFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);

                System.out.println("Saved " + completed.size() + " completed ranges.");
            } catch (IOException e) {
                System.err.println("Failed to save completed ranges: " + e.getMessage());
            }
        }


    }

    /* --------------------------------------
       Client Implementation
       -------------------------------------- */

    static class Client {
        private final String host;
        private final int port;
        private final long chunkSize;

        Client(String host, int port, long chunkSize) {
            this.host = host;
            this.port = port;
            this.chunkSize = chunkSize;
        }
        void runLoop() {
            while (true) {
                runOnce();
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        // runs a single assignment cycle: request -> run rust -> send results -> exit.
        void runOnce() {
            try (Socket sock = new Socket(host, port);
                 BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                 BufferedWriter out = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));)
            {
                out.write("REQUEST\n"); out.flush();
                String resp = in.readLine();
                if (resp == null) { System.err.println("Server closed connection"); return; }
                if (resp.startsWith("ASSIGN ")) {
                    long start = Long.parseLong(resp.split("\\s+")[1]);
                    System.out.println("Assigned start=" + start);
                    // run rust tool
                    List<Monolith> results = runRustAndCollectTop3(start);
                    // send results
                    for (Monolith m : results) {
                        out.write("MONOLITH " + m.toJson() + "\n");
                    }
                    out.write("DONE\n"); out.flush();
                    System.out.println("Sent " + results.size() + " monoliths to server.");
                } else if (resp.startsWith("NO_MORE")) {
                    System.out.println("Server: NO_MORE work");
                } else {
                    System.err.println("Unexpected server response: " + resp);
                }
            } catch (IOException e) {
                System.err.println("Client error: " + e.getMessage());
            }
        }

        // Builds and runs the external Rust process and collects top 3 monoliths from its stdout
        private List<Monolith> runRustAndCollectTop3(long start) {
            List<String> cmd = buildRustCommand(start, chunkSize);
            System.out.println("Running rust command: " + String.join(" ", cmd));
            ProcessBuilder pb = new ProcessBuilder(cmd);
            File jarDir = new File(System.getProperty("java.class.path")).getAbsoluteFile().getParentFile();
            pb.directory(jarDir);

            pb.redirectErrorStream(true);
            List<Monolith> found = new ArrayList<>();
            try {
                Process p = pb.start();
                BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                Pattern ptn = Pattern.compile("json\\s+(\\{.*\\})");
                while ((line = r.readLine()) != null) {
                    System.out.println("[rust] " + line);
                    Matcher m = ptn.matcher(line);
                    if (m.find()) {
                        String json = m.group(1);
                        Monolith mon = Monolith.fromJson(json);
                        if (mon != null) {
                            found.add(mon);
                        }
                    }
                }
                p.waitFor();
            } catch (Exception ex) {
                System.err.println("Failed to run rust process: " + ex.getMessage());
            }
            // pick top 3 by area
            found.sort(Comparator.comparingLong(Monolith::getArea).reversed());
            if (found.size() > 3) return found.subList(0,3);
            return found;
        }

        // Build the `cargo run` command. Adjust this template if rust wrapper expects different args.
        private List<String> buildRustCommand(long start, long chunkSize) {
            String hostname;
            try {
                hostname = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostname = "unknown";
            }

            // Get directory where the running JAR is located
            String jarPath = new File(
                MonolithCoordinator.class.getProtectionDomain().getCodeSource().getLocation().getPath()
            ).getParent();

            // Create a unique target directory beside the JAR, e.g. "<jar dir>/cargo_build_HostA"
            String targetDir = new File(jarPath, "cargo_build_" + hostname).getAbsolutePath();

            return Arrays.asList(
                "cargo", "run", "--release",
                "--target-dir", targetDir,
                "--features", "fast", "--features", "candidates", "--",
                "spawn", "--chunks", "1000", "--radius", "262144", "--step", "2048",
                "--area", "2800000", "linear",
                "--start", Long.toString(start),
                "--total", Long.toString(chunkSize)
            );
        }

    }

    /* --------------------------------------
       Monolith data class + JSON helpers (very small parser)
       -------------------------------------- */
    static class Monolith {
        final long area;
        final long seed;
        final int minx, maxx, minz, maxz;
        Monolith(long area, long seed, int minx, int maxx, int minz, int maxz) {
            this.area = area; this.seed = seed; this.minx = minx; this.maxx = maxx; this.minz = minz; this.maxz = maxz;
        }
        long getArea(){ return area; }
        String toJson() {
            return String.format("{\"area\":%d,\"seed\":%d,\"minx\":%d,\"maxx\":%d,\"minz\":%d,\"maxz\":%d}", area, seed, minx, maxx, minz, maxz);
        }
        // very small JSON parser expecting exactly the keys present and numeric values
        static Monolith fromJson(String json) {
            try {
                Map<String,String> map = new HashMap<>();
                // remove braces
                String body = json.trim();
                if (body.startsWith("{")) body = body.substring(1);
                if (body.endsWith("}")) body = body.substring(0, body.length()-1);
                // split by commas - assumes values contain no commas (they don't)
                for (String kv : body.split(",")) {
                    String[] parts = kv.split(":",2);
                    if (parts.length<2) continue;
                    String k = parts[0].trim().replaceAll("^\"|\"$", "");
                    String v = parts[1].trim().replaceAll("^\"|\"$", "");
                    map.put(k,v);
                }
                long area = Long.parseLong(map.get("area"));
                long seed = Long.parseLong(map.get("seed"));
                int minx = Integer.parseInt(map.get("minx"));
                int maxx = Integer.parseInt(map.get("maxx"));
                int minz = Integer.parseInt(map.get("minz"));
                int maxz = Integer.parseInt(map.get("maxz"));
                return new Monolith(area, seed, minx, maxx, minz, maxz);
            } catch (Exception ex) {
                System.err.println("JSON parse error: " + ex.getMessage() + " json=" + json);
                return null;
            }
        }
    }
}
