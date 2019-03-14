package org.cpqd.iotagent;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.leshan.core.model.ResourceModel;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import java.util.Arrays;

/*
  This class represents a generic attribute.
  It should be used as intermediary object when converting between device-manager and Lwm2m models
 */
public class DeviceAttribute {
    private String label;
    private String type;
    private String valueType;
    private Object staticValue;
    private String path;
    private ResourceModel.Operations operations;
    private Boolean isLwM2MAttr;
    Logger logger = Logger.getLogger(Device.class);

    public DeviceAttribute(JSONObject json) {
    	this.label = json.getString("label");
    	this.isLwM2MAttr = false;
        this.valueType = json.getString("value_type");
            	
    	this.type = json.getString("type");
        if (type.equals("static")) {
        	this.staticValue = json.opt("static_value");
        	this.operations = ResourceModel.Operations.NONE;
        } else if (type.equals("dynamic")) {
        	this.operations = ResourceModel.Operations.R;
        } else if (type.equals("actuator")) {
        	this.operations = ResourceModel.Operations.RW;
        } else {
        	this.operations = ResourceModel.Operations.NONE;
        }
        
        
        JSONArray meta = json.optJSONArray("metadata");
    	if (meta == null) {
    		return;
    	}
    	for (int i = 0; i < meta.length(); ++i) {
    		JSONObject metaAttr = meta.getJSONObject(i);
            String metaLabel = metaAttr.getString("label");
            logger.info("This is the label 01: " + metaLabel);
            if(metaLabel.startsWith("dojot:firmware_update")){
                addPathOpMeta(metaLabel);
            };
    		if (metaLabel.equals("path")) {
    			this.path = metaAttr.getString("static_value");
    			this.isLwM2MAttr = true;
    		} else if (metaLabel.equals("operations")) {
    			if (metaAttr.getString("static_value").equals("e")) {
    				this.operations = ResourceModel.Operations.E;
    			}
    		}
    	}
    }

    private void addPathOpMeta(String label){
        logger.info("This is the label: " + label);
        switch(label){
            case "dojot:firmware_update:state":
                logger.info("state, adding path and setting islwm2mattr to true");
                this.path="/5/0/3";
                this.isLwM2MAttr = true;
                break;
            case "dojot:firmware_update:update_result":
                logger.info("result, adding path and setting islwm2mattr to true");
                this.path="/5/0/5";
                this.isLwM2MAttr = true;
                break;
            case "dojot:firmware_update:update":
                logger.info("update, adding path and setting islwm2mattr to true");
                this.path="/5/0/2";
                this.isLwM2MAttr = true;
                this.operations = ResourceModel.Operations.E;
                break;
            case "dojot:firmware_update:desired_version":
                logger.info("update, adding path and setting islwm2mattr to true");
                this.path="/5/0/1";
                this.isLwM2MAttr = true;
            default:
                logger.info("Didnt match any dojot:firmware_update");
        }
    }

    public boolean isLwm2mAttr() {
        return this.isLwM2MAttr;
    }
    
    public String getLwm2mPath() {
        return this.path;
    }
    
    public String getLabel() {
        return this.label;
    }
    
    public Object getStaticValue() {
    	return this.staticValue;
    }
    
    public String getValueType() {
    	return this.valueType;
    }

    public static Integer[] getIdsfromPath(String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        String[] p = StringUtils.stripStart(path, "/").split("/");
        Integer[] result = Arrays.stream(p).map(s -> Integer.valueOf(s)).toArray(Integer[]::new);
        return result;
    }

    public boolean isReadable() {
        return this.operations == ResourceModel.Operations.RW || this.operations == ResourceModel.Operations.R;
    }

    public boolean isWritable() {
    	return this.operations == ResourceModel.Operations.RW || this.operations == ResourceModel.Operations.W;
    }

    public boolean isExecutable() {
    	return this.operations == ResourceModel.Operations.E;
    }
    
    public String toString() {
    	StringBuffer objStr = new StringBuffer("");
    	
    	objStr.append("Label: " + this.label);
    	objStr.append("\n type: " + this.type);    	
    	if (this.valueType != null) {
    		objStr.append("\n valueType: " + this.valueType);
    	}
    	if (this.staticValue != null) {
    		objStr.append("\n staticValue: " + this.staticValue);
    	}
    	if (this.isLwM2MAttr) {
    		objStr.append("\n path: " + this.path);
    		objStr.append("\n operations: " + this.operations);
    	}
    	
    	return objStr.toString();
    }

}

