package com.dehnes.glacier_cli;

import com.amazonaws.services.simpledb.model.Attribute;
import com.dehnes.glacier_cli.dto.Backup;
import com.google.gson.Gson;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

public class BackupService {

    //private static int partSize = (int) Math.pow(2, 30); // 1073741824 / 1073MB
    private static int partSize = (int) Math.pow(2, 27); //    134217728 /  134MB
    private final int maxRetention;
    private final GlacierClient glacierClient;
    private final SimpleDbClient simpleDbClient;
    private final Gson gson = new Gson();

    public BackupService(int maxRetention, GlacierClient glacierClient, SimpleDbClient simpleDbClient) {
        this.maxRetention = maxRetention;
        this.glacierClient = glacierClient;
        this.simpleDbClient = simpleDbClient;
    }

    public long uploadBackup(String archiveFile, boolean tryRun) throws Exception {
        long startedAt = System.currentTimeMillis();

        List<Backup> s = getBackupState();
        if (!tryRun) {
            while (s.size() >= maxRetention) {
                Backup toBeDeleted = s.get(s.size() - 1);
                glacierClient.deleteArchive(toBeDeleted.getArchiveId());
                s.remove(s.size() - 1);
                save(s);
                s = getBackupState();
                System.out.println("Deleted old backup from " + Instant.ofEpochMilli(toBeDeleted.getCreatedAt()).atZone(ZoneId.systemDefault()).toLocalDateTime());
            }
        }

        String archiveId = uploadNew(archiveFile, tryRun);
        Backup e = new Backup();
        e.setArchiveId(archiveId);
        e.setCreatedAt(System.currentTimeMillis());
        s.add(e);
        if (!tryRun) {
            save(s);
        }

        return System.currentTimeMillis() - startedAt;
    }

    private List<Backup> getBackupState() {
        return simpleDbClient.get("backupstate").stream().map(a -> gson.fromJson(a.getValue(), Backup.class)).collect(Collectors.toList());
    }

    private void save(List<Backup> state) {
        simpleDbClient.set("backupstate", state.stream().map(b -> new Attribute("json", gson.toJson(b))).collect(Collectors.toList()));
    }

    private String uploadNew(String archiveFile, boolean tryRun) throws Exception {
        String uploadId = glacierClient.initiateMultipartUpload(partSize);
        String checksum = glacierClient.uploadParts(uploadId, partSize, archiveFile, tryRun);
        return glacierClient.completeMultiPartUpload(uploadId, checksum, archiveFile, tryRun);
    }

}
