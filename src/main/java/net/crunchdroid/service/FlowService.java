package net.crunchdroid.service;

import net.crunchdroid.model.Flow;

import java.util.List;

public interface FlowService {

    List<Flow> findAll();

    Flow findOne(Long id);

    Flow save(Flow flow);

    void delete(Long id);

    boolean existsByName(String name);

    List<Flow> findByNameContainingIgnoreCase(String value);

    Flow findByJobId(String jodId);

    Flow findByName(String name);

    String getVariables2(String name);

    String getVariables(String name);

    List<Flow> getFlowsInDirectory();
}
