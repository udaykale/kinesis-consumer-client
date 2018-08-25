package com.udaykale.kinesisconsumerclient.slave;

/**
 * Manages the state of individual clients/slaves
 */
public interface SlaveClientManager extends AutoCloseable {

    // TODO: documentation
    String getGroupId();

    // TODO: documentation
    String getClientId();

    // TODO: documentation
    SlaveStateReadResponse readState();

    // TODO: documentation
    SlaveStateWriteResponse writeState(SlaveStateWriteRequest slaveStateWriteRequest);

    /**
     * An implementation of this storage will try to become a master of the clients within a group id.
     * If the master exists then the client will become a slave.
     *
     * @return true if this client gets registered as a master
     */
    boolean register();

    /**
     * An implementation of this method will unregister the current client with the master.
     * It will handle all the necessary cleanups.
     */
    void unregister();
}
