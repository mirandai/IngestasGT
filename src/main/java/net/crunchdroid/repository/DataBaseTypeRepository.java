package net.crunchdroid.repository;

import net.crunchdroid.model.DataBaseType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataBaseTypeRepository extends JpaRepository<DataBaseType, Long> {
}
