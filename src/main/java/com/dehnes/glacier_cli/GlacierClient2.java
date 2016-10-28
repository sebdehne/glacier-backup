package com.dehnes.glacier_cli;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;

import java.io.File;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.event.ProgressEventType.HTTP_REQUEST_COMPLETED_EVENT;

public class GlacierClient2 extends GlacierClient {

    public GlacierClient2(Regions region, String vaultName) {
        super(region, 0, vaultName);
    }

    @Override
    public String uploadNew(int partSize, String archiveFile, boolean tryRun) {
        AtomicInteger counter = new AtomicInteger();
        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, new ProfileCredentialsProvider());
            UploadResult result = atm.upload(null, vaultName, "my backup " + (new Date()), new File(archiveFile), progressEvent -> {
                if (progressEvent.getEventType() != ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT) {
                    System.out.println(progressEvent);
                }
                if (progressEvent.getEventType() == HTTP_REQUEST_COMPLETED_EVENT) {
                    System.out.print("Completed chunk: " + counter.incrementAndGet() + "\r");
                }
            });
            return result.getArchiveId();

        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(1);
            return null;
        }

    }
}
