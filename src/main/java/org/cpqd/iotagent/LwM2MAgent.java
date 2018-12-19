package org.cpqd.iotagent;

import java.util.List;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.cpqd.iotagent.Device;
import org.cpqd.iotagent.DeviceAttribute;
import org.cpqd.iotagent.ImageDownloader;
import org.cpqd.iotagent.LwM2mHandler;
import org.cpqd.iotagent.DeviceMapper.DeviceControlStructure;
import org.eclipse.leshan.core.node.LwM2mNode;
import org.eclipse.leshan.core.node.LwM2mSingleResource;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeDecoder;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeEncoder;
import org.eclipse.leshan.core.observation.Observation;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.server.californium.LeshanServerBuilder;
import org.eclipse.leshan.server.californium.impl.LeshanServer;
import org.eclipse.leshan.server.impl.InMemorySecurityStore;
import org.eclipse.leshan.server.observation.ObservationListener;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.server.registration.RegistrationListener;
import org.eclipse.leshan.server.registration.RegistrationUpdate;
import org.eclipse.leshan.server.security.NonUniqueSecurityInfoException;
import org.eclipse.leshan.server.security.SecurityInfo;
import org.eclipse.leshan.util.Hex;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import br.com.dojot.config.Config;
import br.com.dojot.kafka.Manager;
import br.com.dojot.utils.Services;


public class LwM2MAgent implements Runnable {
    private Logger logger = Logger.getLogger(LwM2MAgent.class);
    private ImageDownloader imageDownloader;
    private DeviceMapper deviceMapper;
    private LwM2mHandler requestHandler;
    private LeshanServer server;
    private Manager eventHandler;
    private InMemorySecurityStore securityStore;
	private String fwUpdateLabel;

    public LwM2MAgent() {
        this.eventHandler = new Manager();
        this.deviceMapper = new DeviceMapper();
        
        this.securityStore = new InMemorySecurityStore();
        this.fwUpdateLabel = "desirable_fw_version";

        Config dojotConfig = Config.getInstance();
		imageDownloader = new ImageDownloader("http://" + dojotConfig.getImageManagerAddress());

        // register the callbacks to treat the events
        this.eventHandler.addCallback("create", this::on_create);
        this.eventHandler.addCallback("update", this::on_create);
        this.eventHandler.addCallback("remove", this::on_remove);
        this.eventHandler.addCallback("configure", this::on_actuate);
    }

    /**
     * @brief The bootstrap procedure retrieves information about devices previously created from to device manager to
     * build a updated vision about the devices
     */
    public boolean bootstrap() {
    	logger.debug("Bootstrap iotagent leshan: started");
    	Services iotAgent = Services.getInstance();
    	
    	List<String> tenants = iotAgent.listTenants();
    	if (tenants == null) {
    		logger.error("Fail to retrieve tenants");
    		return false;
    	}
    	for (String tenant: tenants) {
    		logger.debug("Requesting devices from tenant: " + tenant);
    		List<String> devicesId = iotAgent.listDevices(tenant);
        	if (devicesId == null) {
        		logger.error("Fail to retrieve devices");
        		return false;
        	}
    		for (String deviceId: devicesId) {
    			logger.debug("Requesting device with device id: " + deviceId);
    			JSONObject deviceJson = iotAgent.getDevice(deviceId, tenant);
            	if (deviceJson == null) {
            		logger.error("Fail to retrieve device");
            		return false;
            	}
    			Device device;
				try {
					device = new Device(deviceJson);
				} catch (Exception e) {
					//just skip this device, it probably is not a LWM2M device
					continue;
				}
    			this.deviceMapper.addNorthboundAssociation(device.getClientEndpoint(), deviceId, tenant);
    		}
    	}
    	
    	logger.debug("Bootstrap iotagent leshan: finished");
    	return true;
    }
	/**
	 * This method is part of Firmware Update. The first thing to do in a firmware update in LwM2m protocol
	 * is send the Package URI to the device. The next step, is wait until the device send that his state
	 * has changed to downloaded (state 2), and, then, actuate on the attribute "FWUpdate-Update", that
	 * will trigger the Firmware Update on the device. 
	 * @param registration, newFwVersion, tenant 
	 * @return
	 */
	private Integer sendsURItoDevice(Registration registration, String newFwVersion, String tenant, String deviceId){
		logger.debug("Will try to send URI to device");

		//Verification if the fw version is really changing.
		String currentFwVersion = requestHandler.ReadResource(registration, "/3/0/3");
		logger.debug("Current FW version: " + currentFwVersion);
		logger.debug("Desirable FW version: " + newFwVersion);
		//Gets URL to give it to device if the version is actual changing
		if(!currentFwVersion.equals(newFwVersion)){
			logger.debug("Versions have actual changed");
			String fileURI = imageDownloader.ImageURI(tenant, deviceId, "Template_lwm2m", newFwVersion);
			logger.debug("Got the file URI: " + fileURI);
			logger.debug("Will write URI in resource package URI");
			requestHandler.WriteResource(registration, "/5000/0/1", fileURI);
		}
		else {
			logger.debug("Device already up-to-date");
		}
		return 0;
	}
    /**
     * @brief This method is a callback and it is called every time a new device is created. It creates a device 
     * representation, register the security key, if any, and can trigger the observation procedure if applicable
     * @param message
     * @return
     */
    private Integer on_create(JSONObject message) {
        logger.debug("on_create: " + message.toString());
        
        // try to build a device representation
        String tenant = message.getJSONObject("meta").getString("service");
        Device device;
		try {
			device = new Device(message.getJSONObject("data"));
		} catch (JSONException e) {
			logger.error("Invalid json");
			return 0;
		} catch (Exception e) {
			// this it not a lwm2m device, just skip it
			return 0;
		}
		
        String deviceId = device.getDeviceId();
        String clientEndpoint = device.getClientEndpoint();

        // let's check if there are any PSK configured into device, if it exists, so removes any others key previously
        // registered with this client endpoint and adds a new entry with the received data

        // '/0/0/5' is the standard path to pre-shared key value
		DeviceAttribute pskAttr = device.getAttributeByPath("/0/0/5");
		if (pskAttr != null) {
			if (!pskAttr.getValueType().equals("psk")) {
				//todo
				logger.error("device " + deviceId + ": invalid psk value type, it must be 'psk'");
				return 0;
			}
			String psk = (String) pskAttr.getStaticValue();
			if (psk == null) {
				logger.error("device " + deviceId + ": missing psk value. Have you configured it?");
				return 0;
			}
			// '/0/0/3' is the standard path to the pre-shared key identity
			DeviceAttribute pskIdentityAttr = device.getAttributeByPath("/0/0/3");
			if (pskIdentityAttr == null) {
				logger.error("device " + deviceId + ": psk is present, but psk identity not");
				return 0;
			}
			if (!pskIdentityAttr.getValueType().equals("string")) {
				logger.error("device " + deviceId + ": invalid psk identity value type, it must be 'string'");
				return 0;
			}
			String pskIdentity = (String) pskIdentityAttr.getStaticValue();
			if (pskIdentity == null) {
				logger.error("device " + deviceId + ": missing psk identity configuration. Have you configured it?");
				return 0;
			}
			SecurityInfo securityInfo = SecurityInfo.newPreSharedKeyInfo(clientEndpoint,
																		 pskIdentity,
																		 Hex.decodeHex(psk.toCharArray()));
			try {
				this.securityStore.remove(clientEndpoint);
				this.securityStore.add(securityInfo);
				logger.debug("Adding a psk to device: " + deviceId);
			} catch (NonUniqueSecurityInfoException e) {
				e.printStackTrace();
				return 0;
			}			
		} else {
			logger.debug("device: " + deviceId + " is not using DTLS");
		}
        
        DeviceControlStructure controlStructure = this.deviceMapper.addNorthboundAssociation(clientEndpoint,
        																					 deviceId,
        																					 tenant);
        if (controlStructure.isSouthboundAssociate()) {
        	logger.debug("Observing some attributes");
        	this.requestHandler.CancelAllObservations(controlStructure.registration);
        	observeResources(device.getReadableAttributes(), controlStructure.registration);
        } else {
        	logger.debug("skipping observing, southbound is not registered yet");
        }
        
        return 0;
    }
    
    private void observeResources(LinkedList<DeviceAttribute> readableAttrs, Registration registration) {
    	for (DeviceAttribute attr : readableAttrs) {
          String path = attr.getLwm2mPath();
          logger.debug("Observing: " + attr.getLabel());
          requestHandler.ObserveResource(registration, path);
          //TODO: update the attributes data with the observation's return
		}
    }
    
    private Integer on_remove(JSONObject message) {
        logger.debug("on_remove: " + message.toString());
        
        Device device;
		try {
			device = new Device(message.getJSONObject("data"));
		} catch (JSONException e) {
			logger.error("Invalid json");
			return 0;
		} catch (Exception e) {
			// this it not a lwm2m device, just skip it
			return 0;
		}
        
        String clientEndpoint = device.getClientEndpoint();
        DeviceControlStructure controlStructure = this.deviceMapper.getDeviceControlStructure(clientEndpoint);
        if (controlStructure.isSouthboundAssociate()) {
        	this.requestHandler.CancelAllObservations(controlStructure.registration);
        }
        this.deviceMapper.removeNorthboundAssociation(clientEndpoint);
        return 0;
    }

    private Integer on_actuate(JSONObject message) {
        logger.debug("on_actuate: " + message.toString());

        String tenant = message.getJSONObject("meta").getString("service");
        String deviceId = message.getJSONObject("data").getString("id");

        DeviceAttribute devAttr;

        Services iotAgent = Services.getInstance();
        JSONObject deviceJson = iotAgent.getDevice(deviceId, tenant);
        Device device;
		try {
			device = new Device(deviceJson);
		} catch (Exception e) {
			return 0;
		}

        DeviceControlStructure controlStruture = this.deviceMapper.getDeviceControlStructure(device.getClientEndpoint());

        if ((controlStruture == null) || (!controlStruture.isSouthboundAssociate())) {
        	//TODO: maybe send some alarm here?
        	logger.error("Device: " + device.getDeviceId() + " is not registered");
        	return 0;
        }
        JSONObject attrs = message.getJSONObject("data").getJSONObject("attrs");
        JSONArray targetAttrs = attrs.names();

        for (int i = 0; i < targetAttrs.length(); ++i) {
        	String targetAttr = targetAttrs.getString(i);
			if(targetAttr.equals(fwUpdateLabel)){
				sendsURItoDevice(controlStruture.registration, attrs.getString(targetAttr), tenant, deviceId);
			}
        	else {
				devAttr = device.getAttributeByLabel(targetAttr);
        		if (devAttr != null) {
					logger.debug("actuating on attribute: " + devAttr.getLabel());
        			String path = devAttr.getLwm2mPath();
        			if (devAttr.isExecutable()) {
        				logger.debug("excuting");
        				requestHandler.ExecuteResource(controlStruture.registration, path, attrs.getString(targetAttr));
        			} else if (devAttr.isWritable()) {
        				logger.debug("writing");
        				requestHandler.WriteResource(controlStruture.registration, path, attrs.get(targetAttr));
        			}
        		} else {
        			logger.warn("skipping attribute: " + targetAttr + ". Not found.");
        		}
			}
		}

        return 0;
    }

    private final RegistrationListener registrationListener = new RegistrationListener() {
    	
    	@Override
        public void registered(Registration registration,
        					   Registration previousReg,
                               Collection<Observation> previousObsersations) {
        	logger.debug("registered: " + registration.toString());
        	
        	DeviceControlStructure controlStructure = deviceMapper.addSouthboundAssociation(registration.getEndpoint(),
        																					registration);
        	if (controlStructure.isNorthboundAssociate()) {
            	logger.debug("Observing some attributes");
            	
        		Services iotAgent = Services.getInstance();
        		JSONObject cachedDev = iotAgent.getDevice(controlStructure.deviceId, controlStructure.tenant);
        		Device device;
				try {
					device = new Device(cachedDev);
				} catch (Exception e) {
					logger.error("Unexpected situation");
					return;
				}
        		
        		requestHandler.CancelAllObservations(controlStructure.registration);
        		observeResources(device.getReadableAttributes(), controlStructure.registration);
        	} else {
            	logger.debug("skpping observing, northbound is not registered yet");
        	}
        	
        }

    	@Override
        public void updated(RegistrationUpdate update, Registration updatedReg, Registration previousReg) {        	
        	logger.debug("updated: " + update.toString());
        	
        	deviceMapper.addSouthboundAssociation(updatedReg.getEndpoint(), updatedReg);
        }

    	@Override
        public void unregistered(Registration registration,
        						 Collection<Observation> observations,
        						 boolean expired,
                                 Registration newReg) {        	
            logger.debug("device left: " + registration.getEndpoint());
            
            deviceMapper.removeSouthboundAssociation(registration.getEndpoint());
        }
    };

    private final ObservationListener observationListener = new ObservationListener() {
        @Override
        public void cancelled(Observation observation) {
        }

        @Override
        public void onResponse(Observation observation, Registration registration, ObserveResponse response) {
        	logger.debug("Received notification from [" + observation.getPath() + "]");

        	//retrieve response content
        	LwM2mNode lwm2mNode = response.getContent();
        	if (lwm2mNode == null) {
        		logger.warn("Response is null. Skipping it");
        		return;
        	}
        	if (!(lwm2mNode instanceof LwM2mSingleResource)) {
        		logger.warn("Unsuported content object.");
        		return;
        	}
        	LwM2mSingleResource resource = (LwM2mSingleResource)lwm2mNode;
        	
        	//retrieve device's attribute information
            DeviceControlStructure controlStruture = deviceMapper.getDeviceControlStructure(registration.getEndpoint());
            if (controlStruture == null) {
            	logger.warn("Unknown endpoint: " + registration.getEndpoint());
            	return;
            }
            if (!controlStruture.isNorthboundAssociate()) {
            	logger.warn("There is not device associate yet with the endpoint: " + registration.getEndpoint());
            	return;
            }
            Services iotAgent = Services.getInstance();
            //TODO: what happens if a device is not found?
            JSONObject deviceJson = iotAgent.getDevice(controlStruture.deviceId, controlStruture.tenant);
            if (deviceJson == null) {
            	logger.warn("Device " + controlStruture.deviceId + " has not found");
            	return; 
            }
            
            Device device;
			try {
				device = new Device(deviceJson);
			} catch (Exception e) {
				logger.error("Unexpected situation");
				return;
			}
            DeviceAttribute attr = device.getAttributeByPath(observation.getPath().toString());
            if (attr == null) {
            	logger.warn("Attribute with path " + observation.getPath().toString() + " is not mapped");
            	return;
            }
            
            JSONObject attrJson = new JSONObject();
            String valueType = attr.getValueType();
            
            switch (resource.getType()) {
				case BOOLEAN:
					attrJson.put(attr.getLabel(), (Boolean)resource.getValue());
					break;
				case FLOAT:
					attrJson.put(attr.getLabel(), (Double)resource.getValue());
					break;
				case INTEGER:
					attrJson.put(attr.getLabel(), (Long)resource.getValue());
					break;
				case STRING:
					attrJson.put(attr.getLabel(), (String)resource.getValue());
					break;
				case TIME:
					//TODO: is correct transform date to string?
					attrJson.put(attr.getLabel(), ((Date)resource.getValue()).toString());
					break;
				case OPAQUE:
					byte [] data = (byte[])resource.getValue();
		            if (valueType.equals("interger")) {
		            	switch(data.length) {
		            		case 1:
		            			Byte b = data[0];
		            			attrJson.put(attr.getLabel(), b.intValue());
		            			break;
		            		case 2:
		            			attrJson.put(attr.getLabel(), ByteBuffer.wrap(data).getShort());
		            			break;
		            		case 4:
		            			attrJson.put(attr.getLabel(), ByteBuffer.wrap(data).getInt());
		            			break;
		            		case 8:
		            			attrJson.put(attr.getLabel(), ByteBuffer.wrap(data).getLong());
		            			break;
		            		default:
		            			logger.error("Attribute " + attr.getLwm2mPath() + " mapped as integer but received " + 
		            						 data.length + " bytes.");
		            			return;
		            	}		            	
		            } else if (valueType.equals("boolean")) {
		            	if (data.length != 1) {
	            			logger.error("Attribute " + attr.getLwm2mPath() + " mapped as boolean but received " + 
           						 		 data.length + " bytes.");
		            		return;
		            	}
		            	if (data[0] == 1) {
		            		attrJson.put(attr.getLabel(), true);
		            	} else {
		            		attrJson.put(attr.getLabel(), false);
		            	}
		            } else if (valueType.equals("float")) {
		            	switch(data.length) {
			            	case 4:
			            		attrJson.put(attr.getLabel(), ByteBuffer.wrap(data).getFloat());
			            		break;
			            	case 8:
			            		attrJson.put(attr.getLabel(), ByteBuffer.wrap(data).getDouble());
			            		break;
		            		default:
		            			logger.error("Attribute " + attr.getLwm2mPath() + " mapped as float but received " + 
	            						 	 data.length + " bytes.");
		            			return;
		            	}		            	
		            } else { // we are assuming the others type are string compatible (is it safe?)
		            	attrJson.put(attr.getLabel(), new String(data));
		            }
					break;
				default:
					logger.error("Unsupported resource type: " + resource.getType().toString());
					return;
            }
			
            eventHandler.updateAttrs(controlStruture.deviceId, controlStruture.tenant, attrJson, null);
        }

        @Override
        public void onError(Observation observation, Registration registration, Exception error) {
            logger.error("Unable to handle notification of" + observation.getRegistrationId().toString() + ": " + observation.getPath());
        }

        @Override
        public void newObservation(Observation observation, Registration registration) {
        }
    };


    @Override
    public void run() {
        try {
            LeshanServerBuilder builder = new LeshanServerBuilder();

            // Set encoder/decoders
            builder.setEncoder(new DefaultLwM2mNodeEncoder());
            builder.setDecoder(new DefaultLwM2mNodeDecoder());
            
            builder.setSecurityStore(this.securityStore);
            
            // Start Server
            server = builder.build();
            server.start();

            // Add Registration Treatment
            server.getRegistrationService().addListener(registrationListener);
            server.getObservationService().addListener(observationListener);

            // Initialize Request Handler
            requestHandler = new LwM2mHandler(server);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
        }
    }
	
}
