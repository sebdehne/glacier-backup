package com.dehnes.glacier_cli;


import com.amazonaws.regions.Regions;

public class Main {

    private static final Regions GLACIER_REGION = Regions.EU_CENTRAL_1;
    private static final String GLACIER_VAULT = "mybackups";
    private static final Regions SDB_REGION = Regions.EU_WEST_1;
    private static final String SDB_DOMAIN = "mybackups";

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: [archiveFile] [tryRun]");
            System.exit(1);
        }

        long took = new BackupService(
                3,
                new GlacierClient2(GLACIER_REGION, GLACIER_VAULT),
                new SimpleDbClient(SDB_DOMAIN, SDB_REGION))
                .uploadBackup(
                        args[0].trim(),
                        Boolean.parseBoolean(args[1].trim())
                );

        System.out.println("Spent " + took);
    }

}