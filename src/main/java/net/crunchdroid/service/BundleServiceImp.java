package net.crunchdroid.service;


import net.crunchdroid.model.Wf_bundles;
import net.crunchdroid.repository.BundleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@Transactional//hay que utilizar esta anotacion cuando se hacen metodos para eliminar o actualizar;
@Service
@Component
public class BundleServiceImp implements BundleService {

    @Autowired
    BundleRepository bundleRepository;

    @Override
    public boolean existsByNombre(String nombre) {
        return bundleRepository.existsByNombre(nombre);
    }

    @Override
    public Wf_bundles findOne(String nombre) {
        return bundleRepository.findOne(nombre);
    }

    @Override
    public Wf_bundles findByNombre(String nombre) {
        return bundleRepository.findByNombre(nombre);
    }

    @Override
    public void deleteByNombre(String nombre) {
        bundleRepository.deleteByNombre(nombre);
    }

    @Override
    public List<Wf_bundles> findAll() {
        return bundleRepository.findAll();
    }


}
