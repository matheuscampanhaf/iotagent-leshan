package org.cpqd.iotagent;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.eclipse.leshan.core.model.LwM2mModel;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.model.ResourceModel;
import org.apache.log4j.Logger;
import org.eclipse.leshan.server.registration.Registration;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class DeviceManager {
    private Logger mLogger = Logger.getLogger(DeviceManager.class);

    private String deviceUrl;
    private String templateUrl;
    private DynamicModelProvider modelProvider;
    private BiMap<String, String> paths2labels = HashBiMap.create();
    private Map<String, Registration> Devices = new HashMap<String, Registration>();
    private Map<String, String> Lwm2mDevices = new HashMap<String, String>();
    private Map<String, String> deviceService = new HashMap<String, String>();



    public DeviceManager(String deviceManagerUrl, DynamicModelProvider modelProvider) {
        this.deviceUrl = deviceManagerUrl + "/device";
        this.templateUrl = deviceManagerUrl + "/template";
        this.modelProvider = modelProvider;
    }

    ObjectModel UpdateModel(ObjectModel oldModel, LinkedList<ResourceModel> newResources){
        HashMap<Integer, ResourceModel> jointModelResources = new HashMap<Integer, ResourceModel>(oldModel.resources);

        for(ResourceModel resource: newResources){
            jointModelResources.put(resource.id, resource);
        }
        jointModelResources.putAll(oldModel.resources);


        ObjectModel jointModel = new ObjectModel(oldModel.id, oldModel.name, oldModel.description, oldModel.version,
                oldModel.multiple,oldModel.mandatory, new ArrayList<>(jointModelResources.values()));

        return jointModel;
    }



    public void RegisterModel(Device device) {
        Map<Integer, LinkedList<ResourceModel>> newModels = new HashMap<Integer, LinkedList<ResourceModel>>();
        LinkedList<ResourceModel> resources;

        String deviceLabel = device.label;

        // Generate a map with new Object models
        for (DeviceAttribute attr : device.attributes) {
            if (attr.isLwm2mAttr()) {
                paths2labels.put(attr.path, attr.label);
                ResourceModel attrModel = attr.getLwm2mResourceModel();
                int objectId = attr.getLwm2mPath()[0];

                // If object model does not exist in map initialize
                if (!newModels.containsKey(objectId)) {
                    newModels.put(objectId, new LinkedList<ResourceModel>());
                }

                newModels.get(objectId).add(attr.getLwm2mResourceModel());
            }
        }


        // TODO(jsiloto): Should models be updated everytime?
        // Iterate over discovered models, add if not already in the provider
        for (Map.Entry<Integer, LinkedList<ResourceModel>> resourceList : newModels.entrySet()) {
            Integer objectId = resourceList.getKey();
            LwM2mModel model = modelProvider.getObjectModel(null);
            ObjectModel oldModel = model.getObjectModel(objectId);

            ObjectModel objectModel;

            if (oldModel == null) {
                objectModel = new ObjectModel(objectId, deviceLabel,
                        "", "1", true, false, resourceList.getValue());
            }
            else{
                objectModel = UpdateModel(oldModel, resourceList.getValue());
            }

            modelProvider.addObjectModel(objectModel);



//            //TODO(jsiloto): Can't we just update the model with new attributes?
//            if (oldModel == null) {
//                ObjectModel objectModel = new ObjectModel(objectId, deviceLabel,
//                        "", "1", false, false, resourceList.getValue());
//
//                for(ResourceModel resource: resourceList.getValue()){
//                    String label = resource.name;
//                    Integer resourceId = resource.id;
//                    String path = objectId.toString() +"/0/"+resourceId.toString();
//                    paths2labels.put(path, label);
//                }
//
//                modelProvider.addObjectModel(objectModel);
//            }
        }
    }


    /**
     * Retrieves device data from device-manager based on the serial number, if no device is found returns null
     */
    public JsonElement GetDeviceFromDeviceManager(String service, String deviceModel, String serialNumber) {
        String token = TenancyManager.GetJwtToken(service);
        String query = "?attr=device_type=" + deviceModel + "&attr=serial_number=" + serialNumber;
        String url = this.deviceUrl + query;

        try {
            HttpResponse<JsonNode> response = Unirest.get(url).header("Authorization", "Bearer " + token).asJson();
            if (response.getStatus() >= 300) {
                return null;
            }
            JsonNode r = response.getBody();
            JSONArray devices = r.getObject().getJSONArray("devices");
            if (devices.length() == 0) {
                return null;
            }
            JSONObject device = devices.getJSONObject(0);
            JsonParser jsonParser = new JsonParser();
            JsonObject gsonDevice = (JsonObject) jsonParser.parse(device.toString());
            return gsonDevice;

        } catch (Exception e) {
            e.printStackTrace();
            mLogger.error(e);
        }
        return null;
    }

    public String GetTemplateLabel(String service, String templateId){
        String token = TenancyManager.GetJwtToken(service);
        String query = "?attr=template_id=" + templateId;
        String url = this.templateUrl + query;

        try {
            HttpResponse<JsonNode> response = Unirest.get(url).header("Authorization", "Bearer " + token).asJson();
            if (response.getStatus() >= 300) {
                return null;
            }
            JsonNode r = response.getBody();

            JSONArray templates = r.getObject().getJSONArray("templates");
            if (templates.length() == 0) {
                return null;
            }
            JSONObject device = templates.getJSONObject(0);
            String label = device.getString("label");
            return label;

        } catch (Exception e) {
            e.printStackTrace();
            mLogger.error(e);
        }
        return null;
    }


    public void RegisterDevice(Device device, String service, String lwm2mId, String deviceModel, String serialNumber, Registration registration) {
        RegisterModel(device);
        Devices.put(device.deviceId, registration);
        Lwm2mDevices.put(lwm2mId, device.deviceId);
        mLogger.debug(device.deviceId);
    }

    public Registration getDeviceRegistration(String id) {
        return Devices.get(id);
    }

    public Registration getLwm2mRegistration(String id) {
        return Devices.get(Lwm2mDevices.get(id));
    }

    public String getDeviceId(String lwm2mId) {
        return Lwm2mDevices.get(lwm2mId);
    }

    public void DeregisterDevice(String lwm2mId) {
        String deviceId = Lwm2mDevices.get(lwm2mId);
        Devices.remove(deviceId);
        Lwm2mDevices.remove(lwm2mId);
    }

    public String getLabelFromPath(String path) {
        return paths2labels.get(path);
    }

    public Integer[] getPathFromLabel(String label) {
        String path = paths2labels.inverse().get(label);
        return DeviceAttribute.getIdsfromPath(path);
    }

    public String getDeviceService(String deviceId){
        // TODO(jsiloto): implement deviceService populate mechanism
        return deviceService.get(deviceId);
    }




}
