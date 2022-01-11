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
public class Wf_flujos_dependencias implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)

    private String nombre;
    private String coordinator;
    private int posicion;
    private String dependencia;
    private int max;
    private int col;


    public Wf_flujos_dependencias(String nombre, String coordinator, int posicion, String dependencia, int max, int col) {
      this.nombre=nombre;
        this.coordinator = coordinator;
        this.posicion = posicion;
        this.dependencia = dependencia;
        this.max = max;
        this.col = col;
    }
}
