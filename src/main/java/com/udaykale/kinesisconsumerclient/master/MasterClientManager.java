package com.udaykale.kinesisconsumerclient.master;

/**
 * Manages the state of all the clients/slaves
 */
public interface MasterClientManager extends AutoCloseable {

    MasterStateReadResponse readState();

    MasterStateWriteResponse writeState(MasterStateWriteRequest masterStateWriteRequest);
}
