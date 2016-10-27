package com.dehnes.glacier_cli.dto;

import java.util.LinkedList;
import java.util.List;

public class BackupState {

    private List<Backup> backups;

    public List<Backup> getBackups() {
        if (backups == null) {
            backups = new LinkedList<>();
        }
        return backups;
    }

    public void setBackups(List<Backup> backups) {
        this.backups = backups;
    }
}
