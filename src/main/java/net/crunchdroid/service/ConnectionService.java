package net.crunchdroid.service;

import net.crunchdroid.model.Connection;

import java.util.List;

public interface ConnectionService {

    List<Connection> findAll();

    Connection findOne(Long id);

    Connection save(Connection connection);

    void delete(Long id);

    List<Connection> findByConnectionNameContainingIgnoreCase(String value);


}
