package net.crunchdroid.repository;

import net.crunchdroid.model.WfBalamBitacoraEjecucion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WfBalamBitacora extends JpaRepository<WfBalamBitacoraEjecucion,String> {
}
