package net.crunchdroid.service;

import net.crunchdroid.model.DataBaseType;
import net.crunchdroid.repository.DataBaseTypeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
@Component
public class DataBaseTypeServiceImp implements DataBaseTypeService {

    @Autowired
    private DataBaseTypeRepository repository;

    @Override
    public DataBaseType findOne(Long id) {
        //return repository.findById(id);
        return repository.findOne(id);
    }
}
