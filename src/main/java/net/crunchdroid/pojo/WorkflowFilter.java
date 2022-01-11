package net.crunchdroid.pojo;

import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@ToString
public class WorkflowFilter {

    /*private String startDate;
    private String endDate;*/
    private String name;
    private String status;
    private String country;
    private String connection;
    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm")
    private LocalDateTime startDate;
    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm")
    private LocalDateTime endDate;
    private String typeFlow;

}
