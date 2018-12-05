package org.cpqd.iotagent;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;
import java.lang.System;

/**
 * This class abstracts everything related to the image-manager, it should have no knowledge of anything LWM2M related
 */

public class ImageDownloader {
    private Logger mLogger = Logger.getLogger(ImageDownloader.class);

    private String imageURI;
    private String fileServerAddress;

    public ImageDownloader(String imageManagerURI) {
        this.imageURI = imageManagerURI + "/image";

        if(System.getenv("FILE_SERVER_ADDRESS") != null ){
            this.fileServerAddress = System.getenv("FILE_SERVER_ADDRESS");
        } else {
            this.fileServerAddress = "10.202.21.44";
        }

        try {
            Path path = FileSystems.getDefault().getPath("./data/");
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
        } catch (Exception e) {
        }
    }

    public String ImageURI(String service, String imageLabel, String version) {
        // TODO(jsiloto): Sanity check and return empty
        String imageID = FetchImage(service, imageLabel, version);
        String fileServerURI = "coap://" + this.fileServerAddress + ":5693/data/";
        String fileURI = fileServerURI + imageID + ".hex";
        return fileURI;
    }

    public String FetchImage(String service, String imageLabel, String version) {
        String token = TenancyManager.GetJwtToken(service);
        String imageID = GetImageId(imageLabel, version, token);
        DownloadImage(imageID, token);
        return imageID;
    }

    private String GetImageId(String imageLabel, String version, String token) {
        try {

            HttpResponse<JsonNode> response = Unirest.get(imageURI)
                    .header("Authorization", "Bearer " + token).asJson();

            JsonNode imageList = response.getBody();
            JSONArray images = imageList.getArray();
            for (int i = 0; i < images.length(); i++) {
                JSONObject image = images.getJSONObject(i);
                String d = image.getString("label");
                String f = image.getString("fw_version");
                Boolean haveBinary = image.getBoolean("confirmed");
                if (d.equals(imageLabel) && f.equals(version) && haveBinary) {
                    return image.getString("id");
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
            mLogger.error(e);
        }
        throw new NoSuchElementException("Image not on Database");
    }

    private void DownloadImage(String imageId, String token) {
        try {
            HttpResponse<InputStream> fwInStream = Unirest.get(imageURI + "/" + imageId + "/binary")
                    .header("Authorization", "Bearer " + token)
                    .asBinary();

            InputStream in = fwInStream.getBody();
            Path path = FileSystems.getDefault().getPath("./data/" + imageId + ".hex");
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
            Files.copy(in, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            e.printStackTrace();
            mLogger.error(e);
        }
    }


}
