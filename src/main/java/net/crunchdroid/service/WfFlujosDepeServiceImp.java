package net.crunchdroid.service;

import net.crunchdroid.model.Wf_flujos_dependencias;
import net.crunchdroid.repository.WfFlujosDependencias;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Transactional//hay que utilizar esta anotacion cuando se hacen metodos para eliminar o actualizar;
@Service
@Component
public class WfFlujosDepeServiceImp implements WfFlujosDepeService {

    @Autowired
    WfFlujosDependencias wfFlujosDependencias;


    @Override
    public void deleteByNombre(String nombre) {
        wfFlujosDependencias.deleteByNombre(nombre);
    }

    @Override
    public List<Wf_flujos_dependencias> findAll() {
        return wfFlujosDependencias.findAll();
    }

    @Override
    public Wf_flujos_dependencias findOne(String nombre) {
        return wfFlujosDependencias.findOne(nombre);
    }


}
