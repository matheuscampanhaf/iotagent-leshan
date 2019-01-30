package org.cpqd.iotagent;

import java.util.LinkedList;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import com.cpqd.app.auth.Auth;

public class Device {
	Logger logger = Logger.getLogger(Device.class);
    String label;
    String deviceId;
    String endpoint;
    public LinkedList<DeviceAttribute> lwm2mReadableAttributes;
    public Hashtable<String, DeviceAttribute> mapLwm2mAttributesByLabel;
    public Hashtable<String, DeviceAttribute> mapLwm2mAttributesByPath;

    public Device(JSONObject device) throws Exception {
    	this.deviceId = device.getString("id");
    	this.label = device.getString("label");
    	
        this.lwm2mReadableAttributes = new LinkedList<DeviceAttribute>();
        this.mapLwm2mAttributesByLabel = new Hashtable<String, DeviceAttribute>();
        this.mapLwm2mAttributesByPath = new Hashtable<String, DeviceAttribute>();
    	
        JSONObject templates = device.getJSONObject("attrs");
        JSONArray templatesIds = device.getJSONObject("attrs").names();
    	for (int i = 0; i < templatesIds.length(); ++i) {
    		JSONArray templateAttributes = templates.getJSONArray(templatesIds.getString(i));
    		for (int j = 0; j < templateAttributes.length(); ++j) {
    			DeviceAttribute devAttr = new DeviceAttribute(templateAttributes.getJSONObject(j));
	    		if (devAttr.getLabel().equals("client_endpoint")) {
	    			this.endpoint = (String)devAttr.getStaticValue();
	    		}
	    		if (!devAttr.isLwm2mAttr()) {
//	    			logger.debug("Skipping " + devAttr.getLabel() + " is not a lwm2m attr");
	    			// skip this attribute
	    			continue;
	    		}
	            if (devAttr.isReadable()) {
	            	this.lwm2mReadableAttributes.add(devAttr);
	            }
//	            logger.debug("adding " + devAttr.getLabel());
	            this.mapLwm2mAttributesByLabel.put(devAttr.getLabel(), devAttr);
	            this.mapLwm2mAttributesByPath.put(devAttr.getLwm2mPath(), devAttr);
	         }
    	}
    	
    	if (this.endpoint == null) {
    		throw new Exception();
    	}
    }

    public Boolean isSecure(){
        DeviceAttribute pskAttr = this.getAttributeByPath("/0/0/5");
        logger.info(pskAttr);
        if(pskAttr != null){
            if (!pskAttr.getValueType().equals("psk")) {
				//todo
				logger.error("device " + this.deviceId + ": invalid psk value type, it must be 'psk'");
				return false;
			}
            String psk = (String) pskAttr.getStaticValue();
            if(psk == null){
                logger.error("device " + this.deviceId + ": missing psk value. Have you configured it?");
		    		return false;
            }
            DeviceAttribute pskIdentityAttr = this.getAttributeByPath("/0/0/3");
            if(pskIdentityAttr == null) {
                logger.error("device " + this.deviceId + ": psk is present, but psk identity not");
		    		return false;
            }
            if(!pskIdentityAttr.getValueType().equals("string")){
                logger.error("device " + this.deviceId + ": invalid psk identity value type, it must be 'string'");
		    		return false;
            }
            String pskIdentity = (String) pskIdentityAttr.getStaticValue();
            if (pskIdentity == null){
                logger.error("device " + this.deviceId + ": missing psk identity configuration. Have you configured it?");
		    		return false;
            }
            return true;
        } else {
            return false;
        }
    }
    
    public DeviceAttribute getAttributeByLabel(String label) {
        return this.mapLwm2mAttributesByLabel.get(label);
    }
    
    public DeviceAttribute getAttributeByPath(String path) {
        return this.mapLwm2mAttributesByPath.get(path);
    }
    
    public String getDeviceId() {
        return this.deviceId;
    }
    
    public String getClientEndpoint() {
        return this.endpoint;
    }

    public LinkedList<DeviceAttribute> getReadableAttributes() {
    	return this.lwm2mReadableAttributes;
    }

}
