package net.crunchdroid.service;

import net.crunchdroid.model.Connection;
import net.crunchdroid.repository.ConnectionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;


import java.util.List;

@Service("connectionService")
@Component
public class ConnnectionServiceImpl implements ConnectionService {

    @Autowired
    private ConnectionRepository connectionRepository;


    @Override
    public List<Connection> findAll() {
        return connectionRepository.findAll();
    }

    @Override
    public Connection findOne(Long id) {
        return connectionRepository.findOne(id);
        //return connectionRepository.findById(id);
    }

    @Override
    public net.crunchdroid.model.Connection save(Connection connection) {
        return connectionRepository.save(connection);
    }

    @Override
    public void delete(Long id) {
        //connectionRepository.deleteById(id);
        connectionRepository.delete(id);
    }

    @Override
    public List<Connection> findByConnectionNameContainingIgnoreCase(String value) {
        return connectionRepository.findByConnectionNameContainingIgnoreCase(value);
    }
}
