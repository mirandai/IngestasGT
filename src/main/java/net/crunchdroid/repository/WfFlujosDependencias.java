package net.crunchdroid.repository;

import net.crunchdroid.model.Wf_flujos_dependencias;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WfFlujosDependencias extends JpaRepository<Wf_flujos_dependencias,String> {

    void deleteByNombre(String nombre);
}
