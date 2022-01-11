package net.crunchdroid.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
public class Connection implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String connectionName;
    private int dataBase;
    private String dbUser;
    private String password;
    private String host;
    private String port;
    private String sid;
    private String unixUser;
    private String unixPath;
    private String type;


}
