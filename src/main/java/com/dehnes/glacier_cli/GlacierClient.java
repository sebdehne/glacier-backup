package com.dehnes.glacier_cli;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.*;
import com.amazonaws.util.BinaryUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class GlacierClient {

    private final AmazonGlacierClient client;
    private final int parallelUploads;
    private final String vaultName;

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

    public String initiateMultipartUpload(int partSize) {

        // Initiate
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
                .withVaultName(vaultName)
                .withArchiveDescription("my archive " + (new Date()))
                .withPartSize(String.valueOf(partSize));

        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);

        return result.getUploadId();
    }

    public void deleteArchive(String archiveId) {
        DeleteArchiveRequest request = new DeleteArchiveRequest()
                .withVaultName(vaultName)
                .withArchiveId(archiveId);
        client.deleteArchive(request);
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


    public String uploadParts(
            String uploadId,
            int partSize,
            String archiveFilePath) throws Exception {

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
                @Override
                public UploadMultipartPartResult call() throws Exception {
                    byte[] buffer = new byte[length];

                    try (RandomAccessFile r = new RandomAccessFile(archiveFilePath, "r")) {
                        r.seek(startPos);
                        int read = r.read(buffer);
                        if (read != length) {
                            throw new RuntimeException("Could not read from file in taskId " + taskId);
                        }

                        String contentRange = String.format("bytes %s-%s/*", startPos, startPos + length - 1);
                        String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(buffer));
                        byte[] binaryChecksum = BinaryUtils.fromHex(checksum);
                        checkSumHolders.add(new CheckSumHolder(taskId, binaryChecksum));

                        UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
                                .withVaultName(vaultName)
                                .withBody(new ByteArrayInputStream(buffer))
                                .withChecksum(checksum)
                                .withRange(contentRange)
                                .withUploadId(uploadId);
                        return client.uploadMultipartPart(partRequest);
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
                } catch (IOException i) {
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

    public String completeMultiPartUpload(
            String uploadId,
            String checksum,
            String archiveFilePath) throws Exception {

        File file = new File(archiveFilePath);

        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
                .withVaultName(vaultName)
                .withUploadId(uploadId)
                .withChecksum(checksum)
                .withArchiveSize(String.valueOf(file.length()));

        CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
        return compResult.getArchiveId();
    }


}
