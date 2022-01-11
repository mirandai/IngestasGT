package net.crunchdroid.repository;

import net.crunchdroid.model.Wf_bundles;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BundleRepository extends JpaRepository<Wf_bundles, String> {

    boolean existsByNombre(String nombre);

    void deleteByNombre(String nombre);

    Wf_bundles findByNombre(String nombre);

}
