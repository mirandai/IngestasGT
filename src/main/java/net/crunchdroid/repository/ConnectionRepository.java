package net.crunchdroid.repository;

import net.crunchdroid.model.Connection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ConnectionRepository extends JpaRepository<Connection, Long> {

    List<Connection> findByConnectionNameContainingIgnoreCase(String value);

    void deleteById(Long id);

}
