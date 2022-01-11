package net.crunchdroid.repository;

import net.crunchdroid.model.Flow;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FlowRepository extends JpaRepository<Flow, Long> {

    boolean existsByName(String name);

    void deleteById(Long id);

    List<Flow> findByNameContainingIgnoreCase(String value);

    Flow findByJobId(String jodId);

    Flow findByName(String name);


    @Query(value = "SELECT * FROM flow f WHERE f.is_directory ='carpeta' ",
            nativeQuery = true)
    List<Flow> getFlowsInDirectory();

    @Query(value = "SELECT variables2 FROM flow as f WHERE f.name=?",
            nativeQuery = true)
    String getVariables2(String name);

    @Query(value = "SELECT variables FROM flow as f WHERE f.name=?",
            nativeQuery = true)
    String getVariables(String name);

}
