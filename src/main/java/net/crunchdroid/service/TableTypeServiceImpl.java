package net.crunchdroid.service;

import net.crunchdroid.model.TableType;
import net.crunchdroid.repository.TableTypeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Component
public class TableTypeServiceImpl implements TableTypeService {

    @Autowired
    private TableTypeRepository tableTypeRepository;

    @Override
    public List<TableType> findAll() {
        return tableTypeRepository.findAll();
    }

    @Override
    public TableType findOne(Long id) {
        return tableTypeRepository.findOne(id);
    }

    @Override
    public TableType save(TableType tableType) {
        return tableTypeRepository.save(tableType);
    }

    @Override
    public void delete(Long id) {
        //tableTypeRepository.delete(id);
        tableTypeRepository.deleteById(id);
    }

    @Override
    public TableType findByDescription(String description) {
        return tableTypeRepository.findByDescription(description);
    }
}
