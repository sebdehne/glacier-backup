package com.dehnes.glacier_cli;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import com.amazonaws.util.BinaryUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GlacierClient {

    final AmazonGlacierClient client;
    final int parallelUploads;
    final String vaultName;

    public GlacierClient(Regions region, int parallelUploads, String vaultName) {
        this.parallelUploads = parallelUploads;
        this.vaultName = vaultName;
        ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
        client = new AmazonGlacierClient(credentials);
        client.configureRegion(region);

        if (!client.listVaults(new ListVaultsRequest()).getVaultList().stream().filter(v -> Objects.equals(v.getVaultName(), vaultName)).findFirst().isPresent()) {
            CreateVaultRequest r = new CreateVaultRequest()
                    .withVaultName(vaultName);
            client.createVault(r);
            System.out.println("Created new vault " + vaultName);
        }
    }

    public String uploadNew(int partSize, String archiveFile, boolean tryRun) throws Exception {
        String uploadId = initiateMultipartUpload(partSize);
        String checksum = uploadParts(uploadId, partSize, archiveFile, tryRun);
        return completeMultiPartUpload(uploadId, checksum, archiveFile, tryRun);
    }

    public void deleteArchive(String archiveId) {
        DeleteArchiveRequest request = new DeleteArchiveRequest()
                .withVaultName(vaultName)
                .withArchiveId(archiveId);
        client.deleteArchive(request);
    }

    private String initiateMultipartUpload(int partSize) {

        // Initiate
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
                .withVaultName(vaultName)
                .withArchiveDescription("my archive " + (new Date()))
                .withPartSize(String.valueOf(partSize));

        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);

        return result.getUploadId();
    }

    private class CheckSumHolder implements Comparable<CheckSumHolder> {
        private final int id;
        private final byte[] checksum;

        public CheckSumHolder(int id, byte[] checksum) {
            this.id = id;
            this.checksum = checksum;
        }

        @Override
        public int compareTo(CheckSumHolder o) {
            return Integer.compare(id, o.id);
        }
    }

    private abstract class Task implements Callable<UploadMultipartPartResult> {
        private final int taskId;

        public Task(int taskId) {
            this.taskId = taskId;
        }
    }

    private String uploadParts(
            String uploadId,
            int partSize,
            String archiveFilePath,
            boolean tryRun) throws Exception {

        System.out.println("Uploading " + archiveFilePath + " using partSize " + partSize);

        long fileSize = new File(archiveFilePath).length();
        List<Task> tasks = new LinkedList<>();
        final List<CheckSumHolder> checkSumHolders = new CopyOnWriteArrayList<>();

        long position = 0;
        int taskIdCounter = 0;
        while (position < fileSize) {
            final long startPos = position;
            final int length = (int) Math.min(partSize, fileSize - startPos);
            position += length;
            int taskId = taskIdCounter++;

            tasks.add(new Task(taskId) {

                final AtomicReference<String> checkSum = new AtomicReference<String>();

                @Override
                public UploadMultipartPartResult call() throws Exception {

                    String contentRange = String.format("bytes %s-%s/*", startPos, startPos + length - 1);

                    // cal checkSum
                    if (checkSum.get() == null) {
                        byte[] binaryChecksum;
                        try (RandomAccessFile r = new RandomAccessFile(archiveFilePath, "r")) {
                            r.seek(startPos);
                            checkSum.set(TreeHashGenerator.calculateTreeHash(new RandomAccessWrapper(r, length)));
                            binaryChecksum = BinaryUtils.fromHex(checkSum.get());
                        }
                        checkSumHolders.add(new CheckSumHolder(taskId, binaryChecksum));
                    }

                    try (RandomAccessFile r = new RandomAccessFile(archiveFilePath, "r")) {
                        r.seek(startPos);

                        UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
                                .withVaultName(vaultName)
                                .withBody(new RandomAccessWrapper(r, length))
                                .withChecksum(checkSum.get())
                                .withRange(contentRange)
                                .withUploadId(uploadId);
                        if (!tryRun) {
                            return client.uploadMultipartPart(partRequest);
                        } else {
                            return null;
                        }
                    }
                }
            });

        }

        // execute tasks
        CountDownLatch c = new CountDownLatch(tasks.size());
        ExecutorService threadPool = Executors.newFixedThreadPool(parallelUploads);
        tasks.forEach(t -> threadPool.execute(() -> {
            while (true) {
                try {
                    t.call();
                    break;
                } catch (IOException | RequestTimeoutException i) {
                    // try again
                    System.out.println("Need to retry one task " + t.taskId + " because" + i.getMessage());
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException ignore) {
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    System.exit(1);
                }
            }
            c.countDown();
        }));

        while (true) {
            System.out.print("Chunks remaining: " + c.getCount() + "\r");
            Thread.sleep(1000);
            if (c.getCount() == 0) {
                break;
            }
        }
        System.out.println("Chunks remaining: " + c.getCount() + "\r");
        threadPool.shutdown();

        List<CheckSumHolder> l = new LinkedList<>(checkSumHolders);
        Collections.sort(l);

        return TreeHashGenerator.calculateTreeHash(l.stream().map(ch -> ch.checksum).collect(Collectors.toList()));
    }

    private String completeMultiPartUpload(
            String uploadId,
            String checksum,
            String archiveFilePath,
            boolean tryRun) throws Exception {

        File file = new File(archiveFilePath);

        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
                .withVaultName(vaultName)
                .withUploadId(uploadId)
                .withChecksum(checksum)
                .withArchiveSize(String.valueOf(file.length()));

        if (!tryRun) {
            CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
            return compResult.getArchiveId();
        } else {
            return "FAKE_ID";
        }
    }

    public void getInventory() {
        JobParameters jobParameters = new JobParameters()
                .withType("inventory-retrieval");
        InitiateJobRequest request = new InitiateJobRequest()
                .withJobParameters(jobParameters)
                .withVaultName(vaultName);
        InitiateJobResult job = client.initiateJob(request);
        System.out.println(job);
        System.out.println(job.getJobId());
    }

    public static void main(String[] args) {
        GlacierClient c = new GlacierClient(Regions.EU_CENTRAL_1, 1, "testvault");
        c.getInventory();
    }


}
