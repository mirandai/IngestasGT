package net.crunchdroid.service;

import net.crunchdroid.model.Flow;
import net.crunchdroid.repository.FlowRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Component
public class FlowServiceImpl implements FlowService {

    @Autowired
    private FlowRepository flowRepository;

    @Override
    public List<Flow> findAll() {
        return flowRepository.findAll();
    }

    @Override
    public Flow findOne(Long id) {
        return flowRepository.findOne(id);
    }

    @Override
    public Flow save(Flow flow) {
        return flowRepository.save(flow);
    }

    @Override
    public void delete(Long id) {
        flowRepository.delete(id);
    }

    @Override
    public boolean existsByName(String name) {
        return flowRepository.existsByName(name);
    }

    @Override
    public List<Flow> findByNameContainingIgnoreCase(String value) {
        return flowRepository.findByNameContainingIgnoreCase(value);
    }

    @Override
    public Flow findByJobId(String jodId) {
        return flowRepository.findByJobId(jodId);
    }

    @Override
    public List<Flow> getFlowsInDirectory() {
        return flowRepository.getFlowsInDirectory();
    }


    @Override
    public Flow findByName(String name) {
        return flowRepository.findByName(name);
    }

    @Override
    public String getVariables2(String name) {
        return flowRepository.getVariables2(name);
    }

    @Override
    public String getVariables(String name) {
        return flowRepository.getVariables(name);
    }

}
