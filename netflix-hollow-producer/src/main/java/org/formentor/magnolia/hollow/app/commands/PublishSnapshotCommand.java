package org.formentor.magnolia.hollow.app.commands;

import info.magnolia.cms.security.Permission;
import info.magnolia.commands.impl.BaseRepositoryCommand;
import info.magnolia.context.Context;
import org.formentor.magnolia.hollow.application.ContentSnapshotPublisher;

import javax.jcr.Node;

public class PublishSnapshotCommand extends BaseRepositoryCommand {
    private final ContentSnapshotPublisher contentSnapshotPublisher;

    public PublishSnapshotCommand(ContentSnapshotPublisher contentSnapshotPublisher) {
        this.contentSnapshotPublisher = contentSnapshotPublisher;
    }

    @Override
    public boolean execute(Context context) throws Exception {
        Node node = getJCRNode(context, Permission.READ);
        contentSnapshotPublisher.publish(node);

        return true;
    }
}
