package net.crunchdroid.service;

import net.crunchdroid.model.TableType;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface TableTypeService {

    List<TableType> findAll();

    TableType findOne(Long id);

    TableType save(TableType tableType);

    void delete(Long id);

    TableType findByDescription(String description);

}
