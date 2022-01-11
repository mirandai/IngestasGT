package net.crunchdroid.pojo;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@ToString
public class WfBalamBitacora {

    private String fecha;
    private String nombre_wf;
    private String hora;
    private String estado;
    private String descripcion;
}
