package net.crunchdroid.model;

import lombok.*;
import org.joda.time.DateTime;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;
import java.sql.Date;

@Entity
@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@ToString
public class WfBalamBitacoraEjecucion implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)

    private String fecha;
    private String nombre_wf;
    private String hora;
    private String estado;
    private String descripcion;
}
