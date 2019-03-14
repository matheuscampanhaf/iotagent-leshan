package org.eclipse.leshan.client.demo;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.leshan.client.request.ServerIdentity;
import org.eclipse.leshan.client.resource.BaseInstanceEnabler;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.model.ResourceModel.Type;
import org.eclipse.leshan.core.node.LwM2mResource;
import org.eclipse.leshan.core.response.ExecuteResponse;
import org.eclipse.leshan.core.response.ReadResponse;
import org.eclipse.leshan.core.response.WriteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirmwareUpdateObject extends BaseInstanceEnabler {
    
    static final int STATE_UPDATING = 3;
    static final int STATE_IDLE = 0;
    static final int STATE_DOWNLOADING = 1;
    static final int STATE_DOWNLOADED = 2;
    static final int RESOURCE_STATE = 3;
    
    static final int RESOURCE_UPDATE_RESULT = 5;
    static final int UR_INITIAL_VALUE = 0;
    static final int UR_SUCCESS = 1;
    static final int UR_FAIL_DOWNLOAD = 4;

    private static final Logger LOG = LoggerFactory.getLogger(FirmwareUpdateObject.class);

    private static final List<Integer> supportedResources = Arrays.asList(1, 2, 3, 5);
    public FirmwareUpdateObject() {
        URI = "";
    }

    @Override
    public ReadResponse read(ServerIdentity identity, int resourceid) {
        LOG.info("Read on Device Resource " + resourceid);
        switch (resourceid) {
        case 1:
            return ReadResponse.success(resourceid, getURI());
        case 3:
            return ReadResponse.success(resourceid, getState());
        case 5:
            return ReadResponse.success(resourceid, getUpdateResult());
        default:
            return super.read(identity, resourceid);
        }
    }

    @Override
    public ExecuteResponse execute(ServerIdentity identity, int resourceid, String params) {
        LOG.info("Execute on Firmware Update resource " + resourceid);
        if (params != null && params.length() != 0)
            System.out.println("\t params " + params);
        switch(resourceid){
            case 2:
                setState(STATE_UPDATING);
                fireResourcesChange(RESOURCE_STATE);
                Runnable r1 = new Runnable() {

                    public void run (){
                        try {
                            Thread.sleep(5000);
                            setState(STATE_IDLE);
                            fireResourcesChange(RESOURCE_STATE);
                            setUpdateResult(UR_SUCCESS);
                            fireResourcesChange(RESOURCE_UPDATE_RESULT);
                        } catch (Exception error) {
                            System.out.println(error);
                        }
    
                    }
                };
                Thread t1 = new Thread(r1);
                t1.start();
        }
        return ExecuteResponse.success();
    }

    @Override
    public WriteResponse write(ServerIdentity identity, int resourceid, LwM2mResource value) {
        LOG.info("Write on Device Resource " + resourceid + " value " + value);
        switch (resourceid) {
        case 1:
            resetUpdateResult();
            setURI((String) value.getValue());
            fireResourcesChange(resourceid);
            setState(STATE_DOWNLOADING);
            fireResourcesChange(RESOURCE_STATE);
            Runnable r1 = new Runnable() {

                public void run (){
                    try {
                        Thread.sleep(5000);
                        if(checkURI(getURI())){
                            setState(STATE_DOWNLOADED);
                            fireResourcesChange(RESOURCE_STATE);
                        } else {
                            setState(STATE_IDLE);
                            fireResourcesChange(RESOURCE_STATE);
                            setUpdateResult(UR_FAIL_DOWNLOAD);
                            fireResourcesChange(RESOURCE_UPDATE_RESULT);
                        }
                    } catch (Exception error) {
                        System.out.println(error);
                    }

                }
            };
            Thread t1 = new Thread(r1);
            t1.start();
            return WriteResponse.success();
        default:
            return super.write(identity, resourceid, value);
        }
    }

    private void resetUpdateResult(){
        setUpdateResult(UR_INITIAL_VALUE);
        fireResourcesChange(RESOURCE_UPDATE_RESULT);
    }

    private Boolean checkURI(String URI) {
        return URI.endsWith(".hex");
    }


    private int updateResult = 0;

    private int getUpdateResult(){
        return updateResult;
    }

    private void setUpdateResult(int r){
        updateResult = r;
    }

    private String URI;
    
    private String getURI(){
        return URI;
    }

    private void setURI(String u) {
        URI = u;
    }

    private int state = 0;

    private int getState(){
        return state;
    }

    private void setState(int s){
        state = s;
    }

    @Override
    public List<Integer> getAvailableResourceIds(ObjectModel model) {
        return supportedResources;
    }
}
