package net.crunchdroid.pojo;

import lombok.*;
import org.apache.oozie.client.CoordinatorJob;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@ToString

public class ControlDependencies {

    CoordinatorJob coordinator;
    boolean dependencia;


    public ControlDependencies(CoordinatorJob coordinator, boolean dependencia) {
        this.coordinator = coordinator;
        this.dependencia = dependencia;

    }
}
