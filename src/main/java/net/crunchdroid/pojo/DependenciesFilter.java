package net.crunchdroid.pojo;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@ToString

public class DependenciesFilter {

    private String startDate;
    private String endDate;
    private String name;
    private String extract;
    private String country;
    private String minute;
    private String hour;
    private String weekday;
    private String month;
    private String monthday;
    private String bundle_id;
    private String status;




   /* public DependenciesFilter(String startDate, String endDate, String name, String extract, String country) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.name = name;
        this.extract = extract;
        this.country = country;
    }*/

    public DependenciesFilter(String startDate, String endDate, String name, String extract, String country, String minute, String hour, String weekday, String month, String monthday, String bundle_id, String status) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.name = name;
        this.extract = extract;
        this.country = country;
        this.minute = minute;
        this.hour = hour;
        this.weekday = weekday;
        this.month = month;
        this.monthday = monthday;
        this.bundle_id = bundle_id;
        this.status = status;
    }
}
