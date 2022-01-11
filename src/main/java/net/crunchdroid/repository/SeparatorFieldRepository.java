package net.crunchdroid.repository;

import net.crunchdroid.model.SeparatorField;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SeparatorFieldRepository extends JpaRepository<SeparatorField, Long> {

    void deleteById(Long id);

}
