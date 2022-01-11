package net.crunchdroid.repository;

import net.crunchdroid.model.TableType;
import net.crunchdroid.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TableTypeRepository extends JpaRepository<TableType, Long> {

    void deleteById(Long id);

    TableType findByDescription(String description);

}
