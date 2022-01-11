package net.crunchdroid.service;

import net.crunchdroid.model.Wf_bundles;
import net.crunchdroid.model.Wf_flujos_dependencias;
import net.crunchdroid.repository.WfFlujosDependencias;

import java.util.List;

public interface WfFlujosDepeService {
    void deleteByNombre(String nombre);

    List<Wf_flujos_dependencias> findAll();

   Wf_flujos_dependencias findOne(String nombre);
}
