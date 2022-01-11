package net.crunchdroid.service;

import net.crunchdroid.model.WfBalamBitacoraEjecucion;
import net.crunchdroid.repository.WfBalamBitacora;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("WfbalamBitacoraService")
@Component
public class WfBalamBitacoraImpl implements WfBalamBitacoraService {

    @Autowired
    private WfBalamBitacora WfBalamBitacoraRepository;

    @Override
    public List<WfBalamBitacoraEjecucion> findAll() {
        return WfBalamBitacoraRepository.findAll();
    }
}
