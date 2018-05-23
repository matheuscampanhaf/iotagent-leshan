package org.cpqd.iotagent;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.cpqd.iotagent.Device;
import org.eclipse.leshan.server.registration.Registration;

public class DeviceMapper {
	Logger logger = Logger.getLogger(Device.class);

	final class DeviceControlStructure {
		Registration registration;
		String deviceId;
		String tenant;
		
		public boolean isNorthboundAssociate() {
			return this.deviceId != null && this.tenant != null;
		}
		public boolean isSouthboundAssociate() {
			return this.registration != null;
		}		
	}
	
    private Map<String, DeviceControlStructure> deviceControlStruct = new HashMap<String, DeviceControlStructure>();
	
    public DeviceControlStructure addSouthboundAssociation(String clientEndpoint, Registration registration) {
    	DeviceControlStructure controlStruct = this.deviceControlStruct.get(clientEndpoint);
    	if (controlStruct == null) {
    		controlStruct = new DeviceControlStructure();
    	}
    	logger.debug("associating south: " + clientEndpoint);
    	controlStruct.registration = registration;
    	this.deviceControlStruct.put(clientEndpoint, controlStruct);
    	return controlStruct;
    }
    
    public DeviceControlStructure addNorthboundAssociation(String clientEndpoint, String deviceId, String tenant) {
    	DeviceControlStructure controlStruct = this.deviceControlStruct.get(clientEndpoint);
    	if (controlStruct == null) {
    		controlStruct = new DeviceControlStructure();
    	}
    	logger.debug("associating north: " + clientEndpoint);
    	controlStruct.deviceId = deviceId;
    	controlStruct.tenant = tenant;
    	this.deviceControlStruct.put(clientEndpoint, controlStruct);
    	return controlStruct;
    }
    
    public DeviceControlStructure getDeviceControlStructure(String clientEndpoint) {
    	DeviceControlStructure controlStruct = this.deviceControlStruct.get(clientEndpoint);
    	if (controlStruct == null) {
    		return null;
    	}
    	return controlStruct;
    }
    
    public boolean removeSouthboundAssociation(String clientEndpoint) {
    	DeviceControlStructure controlStruct = this.deviceControlStruct.get(clientEndpoint);
    	if (controlStruct == null) {
    		return false;
    	}
    	controlStruct.registration = null;
    	
    	if (!controlStruct.isNorthboundAssociate()) {
    		this.deviceControlStruct.remove(clientEndpoint);
    	}
    	return true;
    }
    
    public boolean removeNorthboundAssociation(String clientEndpoint) {
    	DeviceControlStructure controlStruct = this.deviceControlStruct.get(clientEndpoint);
    	if (controlStruct == null) {
    		return false;
    	}
    	controlStruct.deviceId = null;
    	controlStruct.tenant = null;
    	
    	if (!controlStruct.isSouthboundAssociate()) {
    		this.deviceControlStruct.remove(clientEndpoint);
    	}
    	return true;
    }
}
