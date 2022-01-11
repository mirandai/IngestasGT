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
public class Flow implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String name;
    private String description;
    private String country;
    private Long connectionId;
    private String query;
    private String filename;
    private String minute;
    private String hour;
    private String weekday;
    private String month;
    private String monthday;
    private String startDate;
    private String endDate;
    private String percentageToleranceRecords;
    private Boolean alertByEmail;
    private String emails;
    private String isDirectory;
    private String directory;
    private Boolean comulative;
    private Boolean override;
    private String schemaDatabase;
    private String tablename;
    private String type;
    private String separator_;
    private String split;
    private String mappers;
    private String tableSize;
    private String fileSize;
    private Boolean particioned;
    private String particionedField;
    private String queue;
    private String queueSqoop;
    private String jobId;
    private String sourceDirectory;
    private Boolean scheduleNow;
    private String isSchedule;
    private String variables;
    private String variables2;

}
