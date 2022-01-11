package net.crunchdroid.model;


import lombok.*;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;

@Entity
@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@ToString

public class Wf_bundles implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)

    String bundle_id;
    String nombre;
    String cadena;
    String minuto;
    String hora;
    String diaSemana;
    String mes;
    String diaMes;
    String fecha_inicio;
    String fecha_fin;
    String frecuencia;

    public Wf_bundles(String bundle_id, String nombre, String cadena, String minuto, String hora, String diaSemana, String mes, String diaMes, String fecha_inicio, String fecha_fin, String frecuencia) {
        this.bundle_id = bundle_id;
        this.nombre = nombre;
        this.cadena = cadena;
        this.minuto = minuto;
        this.hora = hora;
        this.diaSemana = diaSemana;
        this.mes = mes;
        this.diaMes = diaMes;
        this.fecha_inicio = fecha_inicio;
        this.fecha_fin = fecha_fin;
        this.frecuencia = frecuencia;
    }


/*public Wf_bundles(String nombre, String cadena, String minuto, String hora, String diaSemana, String mes, String diaMes, String fecha_inicio, String fecha_fin, String frecuencia) {
        this.nombre = nombre;
        this.cadena = cadena;
        this.minuto = minuto;
        this.hora = hora;
        this.diaSemana = diaSemana;
        this.mes = mes;
        this.diaMes = diaMes;
        this.fecha_inicio = fecha_inicio;
        this.fecha_fin = fecha_fin;
        this.frecuencia = frecuencia;
    }*/
}
