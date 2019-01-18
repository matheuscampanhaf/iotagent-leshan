package org.cpqd.iotagent;

import java.io.File;

import org.apache.log4j.Logger;
import org.cpqd.iotagent.LwM2MAgent;
import org.eclipse.californium.scandium.dtls.pskstore.InMemoryPskStore;

public class LwM2MIoTAgent {

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(LwM2MIoTAgent.class);
        logger.info("Starting LwM2M IoTAgent...");

        InMemoryPskStore securityStore = new InMemoryPskStore();
        // we need to share the securityStore with the agent <- done ????
        LwM2MAgent agent = new LwM2MAgent(securityStore);

        boolean bootstraped = agent.bootstrap();
        if (!bootstraped) {
        	logger.error("Failed on bootstrap");
        	System.exit(1);
        }

        File coapConfigFile = new File(new String("fileServerCoAP.properties"));
        SimpleFileServer fileServer = new SimpleFileServer(coapConfigFile, securityStore);

        fileServer.start();
        // we need to share the path with the ImageDownloader
        fileServer.addNewResource(new String("data"), new File(new String("data")));
        (new Thread(agent)).start();

        while (true) {
            logger.info("Running LwM2M IoTAgent");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException exception) {
                logger.error("Exception: " + exception.toString());
            }
        }
    }
}

