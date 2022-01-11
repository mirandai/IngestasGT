package net.crunchdroid.controller.rest;

import net.crunchdroid.model.Connection;
import net.crunchdroid.model.SeparatorField;
import net.crunchdroid.model.TableType;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.service.SeparatorFieldService;
import net.crunchdroid.service.TableTypeService;
import net.crunchdroid.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@RestController
public class Auxiliar {

    Logger logger = LoggerFactory.getLogger(Auxiliar.class);

    Utils utils = new Utils();

    @Autowired
    ConnectionService connectionService;

    @Autowired
    SeparatorFieldService separatorFieldService;

    @Autowired
    TableTypeService tableTypeService;

    @GetMapping("/auxiliar/whites/{query}/{separator}/{connectionId}")
    public String whites(@PathVariable("query") String query, @PathVariable("separator") String separator_, @PathVariable("connectionId") Long connectionId) {

        logger.info(query);
        logger.info(separator_);
        logger.info(connectionId.toString());

        Connection conn = connectionService.findOne(connectionId);
        return utils.formatQueryOracle(
                query,
                separator_,
                conn.getHost(),
                conn.getPort(),
                conn.getSid(),
                conn.getDbUser(),
                conn.getPassword(),
                conn.getType()
        );
    }

    @GetMapping("/auxiliar/formatofecha/{query}/{connectionId}")
    public String formatDate(@PathVariable("query") String query, @PathVariable("connectionId") Long connectionId) {

        logger.info(query);

        logger.info(connectionId.toString());

        Connection conn = connectionService.findOne(connectionId);
        return utils.formatFechaOracle(
                query,
                conn.getHost(),
                conn.getPort(),
                conn.getSid(),
                conn.getDbUser(),
                conn.getPassword(),
                conn.getType()
        );
    }

    @GetMapping("/auxiliar/tablesize/{description}")
    @Produces(MediaType.APPLICATION_JSON)
    public TableType tableSizeById(@PathVariable("description") String description) {
        System.out.println("Descripción: " + description);
        return tableTypeService.findByDescription(description);

    }

}
