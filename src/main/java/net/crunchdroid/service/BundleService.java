package net.crunchdroid.service;


import net.crunchdroid.model.Flow;
import net.crunchdroid.model.Wf_bundles;


import java.util.List;

public interface BundleService {

    void deleteByNombre(String nombre);

    List<Wf_bundles> findAll();

    boolean existsByNombre(String nombre);

    Wf_bundles findOne(String nombre);

    Wf_bundles findByNombre(String nombre);

}
