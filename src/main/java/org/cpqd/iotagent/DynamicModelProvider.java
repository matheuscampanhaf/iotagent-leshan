package org.cpqd.iotagent;

import org.eclipse.leshan.core.model.LwM2mModel;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.server.model.LwM2mModelProvider;
import org.eclipse.leshan.server.registration.Registration;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A LwM2mModelProvider which uses only one model for all registered clients and allow new ObjectModels
 * This is a modification of StaticModelProvider that allows update to the LwM2mModel
 */
public class DynamicModelProvider implements LwM2mModelProvider {
    private LwM2mModel model;

    public DynamicModelProvider(Collection<ObjectModel> objects) {
        this(new LwM2mModel(objects));
    }

    public DynamicModelProvider(LwM2mModel model) {
        this.model = model;
    }

    public void addObjectModel(ObjectModel objectModel) {
        LinkedList<ObjectModel> models = new LinkedList<ObjectModel>(model.getObjectModels());
        models.add(objectModel);
        this.model = new LwM2mModel(models);
    }

    @Override
    public LwM2mModel getObjectModel(Registration registration) {
        // same model for all clients
        return model;
    }


}

