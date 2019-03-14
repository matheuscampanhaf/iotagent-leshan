package org.cpqd.iotagent;

import org.cpqd.iotagent.Device;
import br.com.dojot.utils.Services;

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
import com.cpqd.app.auth.Auth;
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

    public String ImageURI(String service, String deviceId, String imageId) {
        // TODO(jsiloto): Sanity check and return empty
        String protocol = "coap";
        String port = "5693";
        Device device;
        Services iotAgent = Services.getInstance();
        JSONObject deviceJson = iotAgent.getDevice(deviceId, service);
        try {
            device = new Device(deviceJson);
        } catch (Exception e) {
            mLogger.warn("Invalid json to create device object");
            return "";
        }
        

        if(device.isSecure()){
            mLogger.warn("Device is using DTLS");
            protocol = "coaps";
            port = "5694";
        } 
        
        String token = Auth.getInstance().getToken(service);
        DownloadImage(imageId, token);

        String fileServerURI = protocol + "://" + this.fileServerAddress + ":" + port + "/data/";
        String fileURI = fileServerURI + imageId + ".hex";
        return fileURI;
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
            mLogger.info("Image " + imageId + " successfully downloaded.");
        } catch (Exception e) {
            e.printStackTrace();
            mLogger.error(e);
        }
    }


}
