package com.dehnes.glacier_cli;

import com.amazonaws.regions.Regions;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DeleteArchives {

    public static void main(String[] args) throws IOException {
        GlacierClient client = new GlacierClient(Regions.EU_CENTRAL_1, 1, "testvault");
        String content = new String(Files.readAllBytes(Paths.get(args[0])));
        JSONObject o = new JSONObject(content);
        JSONArray list = o.getJSONArray("ArchiveList");
        for (int i = 0; i < list.length(); i++) {
            JSONObject a = list.getJSONObject(i);
            String archiveId = a.getString("ArchiveId");
            if (archiveId != null && archiveId.length() > 0) {
                System.out.println("deleting " + archiveId);
                client.deleteArchive(archiveId);
            }
        }

    }

}
