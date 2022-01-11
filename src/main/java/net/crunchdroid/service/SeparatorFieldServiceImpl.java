package net.crunchdroid.service;

import net.crunchdroid.model.SeparatorField;
import net.crunchdroid.repository.SeparatorFieldRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Component
public class SeparatorFieldServiceImpl implements SeparatorFieldService {

    @Autowired
    private SeparatorFieldRepository separatorFieldRepository;

    @Override
    public List<SeparatorField> findAll() {
        return separatorFieldRepository.findAll();
    }

    @Override
    public SeparatorField findOne(Long id) {
        return separatorFieldRepository.findOne(id);
        //return separatorFieldRepository.findById(id);
    }

    @Override
    public SeparatorField save(SeparatorField separatorField) {
        return separatorFieldRepository.save(separatorField);
    }

    @Override
    public void delete(Long id) {
        //separatorFieldRepository.delete(id);
        separatorFieldRepository.deleteById(id);
    }
}
