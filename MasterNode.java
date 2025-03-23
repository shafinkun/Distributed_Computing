import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MasterNode implements Runnable {
    private static final int PORT = 5000;
    private final List<Socket> workerSockets = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    public void run() {
        startServer();
    }

    private void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Master Node is running on port " + PORT);

            while (true) {
                Socket workerSocket = serverSocket.accept();
                synchronized (workerSockets) {
                    workerSockets.add(workerSocket);
                }
                System.out.println("Worker connected: " + workerSocket.getInetAddress().getHostAddress());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getWorkerIPs() {
        synchronized (workerSockets) {
            return workerSockets.stream()
                    .map(socket -> socket.getInetAddress().getHostAddress())
                    .collect(Collectors.toList());
        }
    }

    public List<Integer> sortAndMergeData(List<Integer> data) {
        if (workerSockets.isEmpty()) {
            throw new IllegalStateException("No workers are connected.");
        }

        int chunkSize = (int) Math.ceil((double) data.size() / workerSockets.size());
        List<List<Integer>> chunks = new ArrayList<>();
        for (int i = 0; i < data.size(); i += chunkSize) {
            chunks.add(new ArrayList<>(data.subList(i, Math.min(i + chunkSize, data.size()))));
        }

        List<Future<List<Integer>>> futures = new ArrayList<>();
        AtomicLong totalCommunicationTime = new AtomicLong(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < chunks.size(); i++) {
            final Socket workerSocket = workerSockets.get(i);
            final List<Integer> chunk = chunks.get(i);

            futures.add(executor.submit(() -> {
                long commStartTime = System.currentTimeMillis();
                List<Integer> result = processChunk(workerSocket, chunk);
                long commEndTime = System.currentTimeMillis();

                totalCommunicationTime.addAndGet(commEndTime - commStartTime);
                return result;
            }));
        }

        PriorityQueue<Integer> minHeap = new PriorityQueue<>();
        for (Future<List<Integer>> future : futures) {
            try {
                minHeap.addAll(future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return Collections.emptyList();
            }
        }

        List<Integer> sortedData = new ArrayList<>();
        while (!minHeap.isEmpty()) {
            sortedData.add(minHeap.poll());
        }

        long endTime = System.currentTimeMillis();
        long computationTime = (endTime - startTime) - totalCommunicationTime.get();

        System.out.println("Total computation time: " + computationTime + " ms");
        return sortedData;
    }

    private List<Integer> processChunk(Socket workerSocket, List<Integer> chunk) {
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {

            out.writeObject(chunk);
            out.flush();

            return (List<Integer>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }
}
