package net.crunchdroid.controller.monitor;

import net.crunchdroid.model.Flow;
import net.crunchdroid.model.WfBalamBitacoraEjecucion;
import net.crunchdroid.oozie.Monitor;
import net.crunchdroid.oozie.OozieClientConfig;
import net.crunchdroid.pojo.DependenciesFilter;
import net.crunchdroid.pojo.WfBalamBitacora;
import net.crunchdroid.pojo.WorkflowFilter;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.service.FlowService;
import net.crunchdroid.service.WfBalamBitacoraService;
import net.crunchdroid.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Controller
@RequestMapping("/monitor")
public class WorkflowMonitorController {

    Logger logger = LoggerFactory.getLogger(WorkflowFilter.class);

    OozieClientConfig oozieClientConfig = new OozieClientConfig();


    Monitor monitor=new Monitor();

    private Monitor m = new Monitor();


    @Autowired
    private ConnectionService connectionService;


    @GetMapping("/workflow")
    public String workflow(WorkflowFilter filter, Flow flow, Model model) {
        if (filter == null) {
            model.addAttribute("filter", new WorkflowFilter());
            if (filter.getName() == null || filter.getName() == "") {
                filter.setName("*");
            }

            if (filter.getStartDate() == null) {
                filter.setStartDate(LocalDateTime.now().withHour(0).withMinute(0));
            }

            if (filter.getEndDate() == null) {
                filter.setEndDate(LocalDateTime.now().withHour(23).withMinute(59));
            }
            if (filter.getStatus() == null || filter.getStatus() == "") {
                filter.setStatus("*");
            }
            if (filter.getCountry() == null || filter.getCountry() == "") {
                filter.setCountry("*");
            }
            if (filter.getConnection() == null || filter.getConnection() == "") {
                filter.setConnection("*");
            }
            if (filter.getTypeFlow() == null || filter.getTypeFlow() == "") {
                filter.setTypeFlow("*");
            }

        } else {
            if (filter.getName() == null || filter.getName() == "") {
                filter.setName("*");
            }

            if (filter.getStartDate() == null) {
                filter.setStartDate(LocalDateTime.now().withHour(0).withMinute(0));
            }

            if (filter.getEndDate() == null) {
                filter.setEndDate(LocalDateTime.now().withHour(23).withMinute(59));
            }
            if (filter.getStatus() == null || filter.getStatus() == "") {
                filter.setStatus("*");
            }
            if (filter.getCountry() == null || filter.getCountry() == "") {
                filter.setCountry("*");
            }
            if (filter.getConnection() == null || filter.getConnection() == "") {
                filter.setConnection("*");
            }
            if (filter.getTypeFlow() == null || filter.getTypeFlow() == "") {
                filter.setTypeFlow("*");
            }
        }

        logger.info(filter.toString());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:dd");

        Date dd = java.sql.Timestamp.valueOf(filter.getStartDate());
        logger.info(sdf.format(dd));


        model.addAttribute("filter", filter);
        List<Monitor.WorkflowJobShow> workflows = m.getWorkflows(
                sdf.format(java.sql.Timestamp.valueOf(filter.getStartDate())),
                sdf.format(java.sql.Timestamp.valueOf(filter.getEndDate())),
                filter.getStatus(),
                filter.getName(),
                filter.getCountry(),
                filter.getConnection(),
                filter.getTypeFlow());
        model.addAttribute("getWorkflow", workflows);
        model.addAttribute("getconnection", connectionService.findAll());

        Monitor.Status statusList = m.countByStatus(
                sdf.format(java.sql.Timestamp.valueOf(filter.getStartDate())),
                sdf.format(java.sql.Timestamp.valueOf(filter.getEndDate())),
                filter.getStatus(),
                filter.getName(),
                filter.getCountry(),
                filter.getConnection(),
                filter.getTypeFlow());
        int rowCount = (int) 8;
        Object[][] result = new Object[rowCount][2];
        result[0][0] = "Status";
        result[0][1] = "Size";
        result[1][0] = "KILLED";
        result[1][1] = statusList.getKilled();
        result[2][0] = "SUCCEEDED";
        result[2][1] = statusList.getSucceeded();
        result[3][0] = "FAILED";
        result[3][1] = statusList.getFailed();
        result[4][0] = "PREP";
        result[4][1] = statusList.getPrep();
        result[5][0] = "SUSPENDED";
        result[5][1] = statusList.getSuspended();
        result[6][0] = "RUNNING";
        result[6][1] = statusList.getRunning();
        result[7][0] = "SUCCEEDED_ALERT";
        result[7][1] = statusList.getSucceededAlert();
        model.addAttribute("data", result);

        return "monitor/workflow-monitor";
    }


    @GetMapping("/coordinator")
    public String coordinator(WorkflowFilter filter,
                              Model model) {
        if (filter == null) {
            model.addAttribute("filter", new WorkflowFilter());
            if (filter.getName() == null || filter.getName() == "") {
                filter.setName("*");
            }

            if (filter.getStartDate() == null) {
                //filter.setStartDate(LocalDateTime.now().withHour(0).withMinute(0));
                filter.setStartDate(LocalDateTime.of(2018, 11, 18, 0, 0));
            }

            if (filter.getEndDate() == null) {
                filter.setEndDate(LocalDateTime.now().withHour(23).withMinute(59));
            }
            if (filter.getCountry() == null || filter.getCountry() == "") {
                filter.setCountry("*");
            }
            if (filter.getConnection() == null || filter.getConnection() == "") {
                filter.setConnection("*");
            }
            if (filter.getTypeFlow() == null || filter.getTypeFlow() == "") {
                filter.setTypeFlow("*");
            }
        } else {
            if (filter.getName() == null || filter.getName() == "") {
                filter.setName("*");
            }

            if (filter.getStartDate() == null) {
                //filter.setStartDate(LocalDateTime.now().withHour(0).withMinute(0));
                filter.setStartDate(LocalDateTime.of(2018, 11, 18, 0, 0));
            }

            if (filter.getEndDate() == null) {
                filter.setEndDate(LocalDateTime.now().withHour(23).withMinute(59));
            }
            if (filter.getCountry() == null || filter.getCountry() == "") {
                filter.setCountry("*");
            }
            if (filter.getConnection() == null || filter.getConnection() == "") {
                filter.setConnection("*");
            }
            if (filter.getTypeFlow() == null || filter.getTypeFlow() == "") {
                filter.setTypeFlow("*");
            }
        }
        logger.info(filter.toString());
        model.addAttribute("filter", filter);
        model.addAttribute("getConnection", connectionService.findAll());

        List<Monitor.CoordinatorJobShow> coordinators = m.getWorkflowsCoordinators(
                filter.getStartDate().toString(),
                filter.getEndDate().toString(),
                filter.getName(),
                filter.getCountry(),
                filter.getConnection(),
                filter.getTypeFlow());

        model.addAttribute("getCoordinator", coordinators);
        return "monitor/coordinator-monitor";
    }

    @GetMapping("/dependencies")
    public String dependencies(DependenciesFilter filter, Model model) {
        Utils utils = new Utils();

        if (filter == null) {
            model.addAttribute("filter", new DependenciesFilter());

            if (filter.getName() == null || filter.getName() == "") {
                filter.setName("*");
            }
            if (filter.getExtract() == null || filter.getExtract() == "") {
                filter.setExtract("*");
            }

            if (filter.getStartDate() == null || filter.getStartDate() == "") {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                filter.setStartDate("2019-01-28");
            }

            if (filter.getEndDate() == null || filter.getEndDate() == "") {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DAY_OF_YEAR, 8);
                filter.setEndDate(sdf.format(cal.getTime()));
            }

            if (filter.getCountry() == null || filter.getCountry() == "") {
                filter.setCountry("*");
            }

        } else {
            if (filter.getName() == null || filter.getName() == "") {
                filter.setName("*");
            }
            if (filter.getExtract() == null || filter.getExtract() == "") {
                filter.setExtract("*");
            }
            if (filter.getStartDate() == null || filter.getStartDate() == "") {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                filter.setStartDate("2019-01-28");
            }
            if (filter.getEndDate() == null || filter.getEndDate() == "") {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DAY_OF_YEAR, 8);
                filter.setEndDate(sdf.format(cal.getTime()));
            }
            if (filter.getCountry() == null || filter.getCountry() == "") {
                filter.setCountry("*");
            }

        }
        logger.info(filter.toString());
        model.addAttribute("filter", filter);
        model.addAttribute("listBundle", oozieClientConfig.filter_Dependences(filter.getStartDate(), filter.getEndDate(), filter.getName(), filter.getExtract(), filter.getCountry()));
        return "monitor/dependencies-monitor";
    }


    @GetMapping("seeDependences")
    public String seeDependences(@RequestParam("p_nombre") String p_nombre, Model model) {
        model.addAttribute("getDep_flows", monitor.getflows_dependences(p_nombre));
        return "flow/execution-dependencies";
    }

    @GetMapping("/country")
    public String flowsCountry(Model model) {
        model.addAttribute("flowByCountry", monitor.getWFbyCountry("GUATEMALA,COSTA RICA,NICARAGUA,PANAMA"));

        return "monitor/country-monitor";
    }

    @GetMapping("/connection")
    public String connectionCountry(Model model) {
        model.addAttribute("connectionsByCountry", monitor.getWFbyConn());

        return "monitor/connection-monitor";
        //return  pagina que genera graficas de conexion por pais
    }

}
