package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Connection;
import net.crunchdroid.model.Flow;
import net.crunchdroid.oozie.OozieClientConfig;
import net.crunchdroid.pojo.ControlDependencies;
import net.crunchdroid.repository.ConnectionRepository;
import net.crunchdroid.repository.FlowRepository;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.service.FlowService;
import net.crunchdroid.service.SeparatorFieldService;
import net.crunchdroid.shell.hdfs.HdfsBalam;
import net.crunchdroid.shell.local.CreaLoc;
import net.crunchdroid.shell.local.GeneralesLoc;
import net.crunchdroid.shell.remote.ConexionRem;
import net.crunchdroid.shell.remote.CreaRem;
import net.crunchdroid.shell.remote.GeneralesRem;
import net.crunchdroid.util.FlowValidation;
import net.crunchdroid.util.Utils;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

import lombok.*;

@Controller
@RequestMapping("/flows-cluster")
public class FlowClusterController {
    Logger logger = LoggerFactory.getLogger(SpoolFlowController.class);

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private FlowService flowService;

    @Autowired
    private SeparatorFieldService separatorFieldService;

  /*  @Autowired
    private Environment env;*/

  /*  @Value("${ingestas.home}")
    private String INGESTAS_HOME;*/

    private OozieClientConfig client = new OozieClientConfig();
    private Utils utils = new Utils();


    @GetMapping("/list2")
    public String list2(@RequestParam(name = "value", required = false) String value,
                        Model model) throws OozieClientException {

        List<ControlDependencies> coordJobsInfo = client.getCoordJobsInfo("Status=RUNNING;Status=PREP;Status=SUSPENDED;Status=PREPSUSPENDED;", 0, 2000);
        if (value != null) {
            model.addAttribute("key", value);
            model.addAttribute("flowList", coordJobsInfo);

        } else {
            model.addAttribute("flowList", coordJobsInfo);
        }
        return "flow/list-flow-cluster";
    }

    @GetMapping("/list")
    public String list(@RequestParam(name = "value", required = false) String value,
                       Model model) throws OozieClientException {

        List<ControlDependencies> coordJobsInfo = client.getCoordJobsInfo("Status=RUNNING;Status=PREP;Status=SUSPENDED;Status=PREPSUSPENDED;", 0, 2000);

        List<Flow> flows = flowService.findAll();
        Map<Long, String> flows_parameter = new HashMap<>();
        List<FlowAux> aux = new ArrayList<>();
        for (Flow flow : flows) {
            String FlowParameters = flowService.findOne(flow.getId()).getVariables2();
            flows_parameter.put(flow.getId(), FlowParameters);

            if (flow.getVariables2() != null) {
                String[] variables = flow.getVariables2().split("\\,");

                List<FlowParameters> tmpList = new ArrayList<>();
                int count = 0;
                for (String var : variables) {
                    count++;
                    tmpList.add(new FlowParameters(String.valueOf(count), var));
                }
                for (ControlDependencies job : coordJobsInfo) {
                    if (job.getCoordinator().getAppName().equalsIgnoreCase(flow.getName())) {
                        aux.add(new FlowAux(flow, job.getCoordinator(), tmpList, job.isDependencia()));
                    }
                }
            } else {
                for (ControlDependencies job : coordJobsInfo) {
                    if (job.getCoordinator().getAppName().equalsIgnoreCase(flow.getName())) {
                        aux.add(new FlowAux(flow, job.getCoordinator(), null, job.isDependencia()));
                    }
                }
            }
        }

        if (value != null) {
            model.addAttribute("key", value);
            model.addAttribute("flowList", coordJobsInfo);
            model.addAttribute("aux", aux);

        } else {
            model.addAttribute("flowList", coordJobsInfo);
            model.addAttribute("aux", aux);

        }

        return "flow/list-flow-cluster";
    }

    @GetMapping("/edit")
    public String edit(@RequestParam("id") Long id, Model model) {
        Flow flow = flowService.findOne(id);
        model.addAttribute("flow", flow);
        model.addAttribute("connectionList", connectionService.findOne(flow.getConnectionId()));
        return "flow/edit-flow";
    }

    @GetMapping("/parameters")
    public String editParameters(@RequestParam("name") String name, Model model) {
        //find con ese id flow= repo.findOne(flow);
        Flow flow = flowService.findByName(name);
        logger.info("parametros-flow: " + flow.toString());

        model.addAttribute("flow", flow);
        model.addAttribute("Variables2", flowService.getVariables2(name));

        return "flow/edit-parameters-flow";
    }


    @GetMapping("/delete")
    public String delete(@RequestParam("id") Long id, Model model) throws OozieClientException {
        Flow flow = new Flow();
        client.kill(id.toString());
        flowService.delete(id);
        return "redirect:/flows/list";

    }


    @GetMapping("/suspended")
    public String suspended(@RequestParam("idJob") String idJob, Model model) throws OozieClientException {
        Flow flow = new Flow();

        try {
            if (client.getStatusWF(flow.getName())) {
                logger.info("El flujo esta en ejucion");
                model.addAttribute("flowList", client.getCoordJobsInfo("Status=RUNNING;Status=PREP;Status=SUSPENDED;Status=PREPSUSPENDED;", 0, 2000));
                model.addAttribute("mensaje", flow.getName() + " ,aún esta en ejecución.");
                return "flow/list-flow-cluster";
            }
        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        client.suspend(idJob);
        model.addAttribute("mensaje", "El flujo ha sido detenido!");
        return "redirect:/flows-cluster/list";
    }


    @GetMapping("/resume")
    public String resume(String idJob, Model model) {

        logger.info("resume");
        logger.info(idJob);

        Flow flow = flowService.findByJobId(idJob);
        logger.info(flow.toString());

        Connection conn;

        if (flow.getType().equalsIgnoreCase("archivo")) {
            conn = null;
            logger.info("Conexión es nula");
        } else {
            conn = connectionService.findOne(flow.getConnectionId());
            logger.info(conn.toString());

            if (conn == null) {
                logger.info("Error conexión fallida");
                return "flow/list-flow-cluster";
            }
        }

        String alertByMailResult = "si";
        if (flow.getAlertByEmail() == null || flow.getAlertByEmail() == false) {
            alertByMailResult = "no";
            flow.setPercentageToleranceRecords("0");
        }

        String cumlativeResult = "SI";
        if (flow.getComulative() == null || flow.getComulative() == false) {
            cumlativeResult = "NO";
        }

        String overrideResult = "NO";
        if (flow.getOverride() == null || flow.getOverride() == false) {
            overrideResult = "SI";
        }

        String partitionedResult = "SI";
        String partitionedFieldResult = "";
        if (flow.getParticioned() == null || flow.getParticioned() == false) {
            partitionedResult = "NO";
            partitionedFieldResult = "N/A";
        } else {
            partitionedFieldResult = flow.getParticionedField();
        }

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
        Date now = new Date();
        String startDate = sdfDate.format(now);
        System.out.println(startDate + " hora");

        Date fechaHoy = null;
        Date fechaFlujo = null;
        try {
            fechaHoy = sdfDate.parse(startDate);
            fechaFlujo = sdfDate.parse(flow.getStartDate());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (fechaFlujo.before(fechaHoy)) {
            startDate = sdfDate.format(fechaHoy);
        } else {
            startDate = sdfDate.format(fechaFlujo);
        }

        try {
            client.kill(flow.getJobId());
        } catch (OozieClientException e) {
            e.printStackTrace();
        }

        logger.info("Creando Coordinator ------------------");
        try {

            String jobId = "";

            if (flow.getType().equalsIgnoreCase("spool")) {
                jobId = client.CreateCoordinator(
                        flow.getEmails(),
                        flow.getType(),
                        flow.getName(),
                        flow.getIsDirectory(),
                        alertByMailResult,
                        flow.getPercentageToleranceRecords(),
                        flow.getMinute().concat(" ").
                                concat(flow.getHour()).
                                concat(" ").
                                concat(flow.getWeekday()).
                                concat(" ").
                                concat(flow.getMonth()).
                                concat(" ").
                                concat(flow.getMonthday()),
                        conn.getUnixUser().concat("@").concat(conn.getHost()),
                        conn.getUnixPath().concat("/BALAM_INGESTAS"),
                        conn.getDbUser(),
                        conn.getPassword(),
                        conn.getSid(),
                        flow.getFilename(),
                        "A,P9",
                        //"/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
                        utils.get_pathconf() + "/salida/",
                        flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                        "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                        flow.getQueue(),
                        flow.getSchemaDatabase(),
                        flow.getTablename(),
                        partitionedResult,
                        partitionedFieldResult,
                        overrideResult,
                        "N/A",
                        flow.getEndDate(),
                        startDate,
                        (flow.getSeparator_().equalsIgnoreCase("|") ? "\\|" : flow.getSeparator_()),
                        utils.get_dateformat("ORACLE").replace(" ", "_"),
                        conn);

            } else if (flow.getType().equalsIgnoreCase("sqoop")) {
                jobId = client.CreateCoordinator(
                        flow.getEmails(),
                        flow.getType(),
                        flow.getName(),
                        flow.getIsDirectory(),
                        alertByMailResult,
                        flow.getPercentageToleranceRecords(),
                        flow.getMinute().concat(" ").
                                concat(flow.getHour()).
                                concat(" ").
                                concat(flow.getWeekday()).
                                concat(" ").
                                concat(flow.getMonth()).
                                concat(" ").
                                concat(flow.getMonthday()),
                        conn.getUnixUser().concat("@").concat(conn.getHost()),
                        conn.getUnixPath().concat("/BALAM_INGESTAS"),
                        conn.getDbUser(),
                        conn.getPassword(),
                        conn.getSid(),
                        flow.getFilename(),
                        flow.getVariables2() != null ? flow.getVariables2() : "N/A",
                        //"/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
                        utils.get_pathconf() + "/salida/",
                        flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                        "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                        flow.getQueueSqoop(),
                        flow.getSchemaDatabase(),
                        flow.getTablename(),
                        partitionedResult,
                        partitionedFieldResult,
                        overrideResult,
                        "N/A",
                        flow.getEndDate(),
                        startDate,
                        (flow.getSeparator_().equalsIgnoreCase("|") ? "\\|" : flow.getSeparator_()),
                        utils.get_dateformat("ORACLE").replace(" ", "_"),
                        conn);
            } else if (flow.getType().equalsIgnoreCase("archivo")) {
                jobId = client.CreateCoordinator(
                        flow.getEmails(),
                        flow.getType(),
                        flow.getName(),
                        flow.getIsDirectory(),
                        alertByMailResult,
                        flow.getPercentageToleranceRecords(),
                        flow.getMinute().concat(" ").
                                concat(flow.getHour()).
                                concat(" ").
                                concat(flow.getWeekday()).
                                concat(" ").
                                concat(flow.getMonth()).
                                concat(" ").
                                concat(flow.getMonthday()),
                        "NA",
                        "NA",
                        "NA",
                        "NA",
                        "NA",
                        flow.getFilename(),
                        flow.getVariables2() != null ? flow.getVariables2() : "N/A",
                        flow.getSourceDirectory(),
                        flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                        "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                        flow.getQueue(),
                        flow.getSchemaDatabase(),
                        flow.getTablename(),
                        partitionedResult,
                        partitionedFieldResult,
                        overrideResult,
                        "N/A",
                        flow.getEndDate(),
                        startDate,
                        "NA",
                        utils.get_dateformat("ORACLE").replace(" ", "_"),
                        conn);
            }

            logger.info(jobId);
            if (jobId != null) {
                flow.setJobId(jobId);
            }

        } catch (OozieClientException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "flow/list-flow-cluster";
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            return "flow/list-flow-cluster";
        }

        Flow persistFlow = flowService.save(flow);


        if (persistFlow.getId() == null) {
            model.addAttribute("message", "No se pudo crear el flujo en base de datos, pero si se ha configurado en Oozie.");
            return "flow/spool-flow";
        }

        model.addAttribute("mensaje", flow.getName() + " ,ha sido lanzado exitosamente!.");
        //return "flow/list-flow-cluster";
        return "redirect:/flows-cluster/list";
    }


    @GetMapping("/reRun")
    public String RunNow(String idJob, String coord, Model model) throws OozieClientException {
        /* Inicia Agregado*/
        List<ControlDependencies> coordJobsInfo = client.getCoordJobsInfo("Status=RUNNING;Status=PREP;Status=SUSPENDED;Status=PREPSUSPENDED;", 0, 2000);
        List<Flow> flows = flowService.findAll();
        Map<Long, String> flows_parameter = new HashMap<>();
        List<FlowAux> aux = new ArrayList<>();
        for (Flow flow : flows) {
            String FlowParameters = flowService.findOne(flow.getId()).getVariables2();
            flows_parameter.put(flow.getId(), FlowParameters);


            if (flow.getVariables2() != null) {
                String[] variables = flow.getVariables2().split("\\,");

                List<FlowParameters> tmpList = new ArrayList<>();
                int count = 0;
                for (String var : variables) {
                    count++;
                    tmpList.add(new FlowParameters(String.valueOf(count), var));
                }
                for (ControlDependencies job : coordJobsInfo) {
                    if (job.getCoordinator().getAppName().equalsIgnoreCase(flow.getName())) {
                        aux.add(new FlowAux(flow, job.getCoordinator(), tmpList, job.isDependencia()));
                    }
                }
            } else {
                for (ControlDependencies job : coordJobsInfo) {
                    if (job.getCoordinator().getAppName().equalsIgnoreCase(flow.getName())) {
                        aux.add(new FlowAux(flow, job.getCoordinator(), null, job.isDependencia()));
                    }
                }
            }
        }
        /*Finaliza Agregado*/


        Connection conn;
        Flow flow = flowService.findByJobId(idJob);
        if (flow != null) {
            //   Connection conn;
            if (flow.getType().equalsIgnoreCase("archivo")) {
                conn = null;
                logger.info("Conexión es nula");
            } else {
                conn = connectionService.findOne(flow.getConnectionId());
                logger.info(conn.toString());

                if (conn == null) {
                    logger.info("Error conexión fallida");
                    return "flow/list-flow-cluster";
                }
            }

            if (client.getStatusWF(flow.getName())) {
                logger.info("El flujo esta en ejucion");
                model.addAttribute("flowList", client.getCoordJobsInfo("Status=RUNNING;Status=PREP;Status=SUSPENDED;Status=PREPSUSPENDED;", 0, 2000));
                model.addAttribute("mensaje", flow.getName() + " ,aún esta en ejecución.");
                model.addAttribute("aux", aux);
                return "flow/list-flow-cluster";
            }

            String alertByMailResult = "si";
            if (flow.getAlertByEmail() == null || flow.getAlertByEmail() == false) {
                alertByMailResult = "no";
                flow.setPercentageToleranceRecords("0");
            }

            String cumlativeResult = "SI";
            if (flow.getComulative() == null || flow.getComulative() == false) {
                cumlativeResult = "NO";
            }

            String overrideResult = "NO";
            if (flow.getOverride() == null || flow.getOverride() == false) {
                overrideResult = "SI";
            }

            String partitionedResult = "SI";
            String partitionedFieldResult = "";
            if (flow.getParticioned() == null || flow.getParticioned() == false) {
                partitionedResult = "NO";
                partitionedFieldResult = "N/A";
            } else {
                partitionedFieldResult = flow.getParticionedField();
            }

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }

            logger.info("Creando Flujo ------------------");
            try {
                String jobId = "";
                if (flow.getType().equalsIgnoreCase("spool")) {


                    jobId = client.CreateWorkflow(
                            flow.getEmails(),
                            flow.getType(),
                            flow.getName(),
                            flow.getIsDirectory(),
                            alertByMailResult,
                            flow.getPercentageToleranceRecords(),
                            flow.getMinute().concat(" ").
                                    concat(flow.getHour()).
                                    concat(" ").
                                    concat(flow.getWeekday()).
                                    concat(" ").
                                    concat(flow.getMonth()).
                                    concat(" ").
                                    concat(flow.getMonthday()),
                            conn.getUnixUser().concat("@").concat(conn.getHost()),
                            conn.getUnixPath().concat("/BALAM_INGESTAS"),
                            conn.getDbUser(),
                            conn.getPassword(),
                            conn.getSid(),
                            flow.getFilename(),
                            flow.getVariables2() != null ? flow.getVariables2() : "N/A",
                            //"/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
                            utils.get_pathconf() + "/salida/",
                            flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                            "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                            flow.getQueue(),
                            flow.getSchemaDatabase(),
                            flow.getTablename(),
                            partitionedResult,
                            partitionedFieldResult,
                            overrideResult,
                            "N/A",
                            flow.getEndDate(),
                            (flow.getSeparator_().equalsIgnoreCase("|") ? "\\|" : flow.getSeparator_()),
                            utils.get_dateformat("ORACLE").replace(" ", "_"),
                            conn);

                    model.addAttribute("flowList", coordJobsInfo);
                    model.addAttribute("aux", aux);
                    model.addAttribute("mensajeSuccess", "Flujo lanzado exitosamente!.");
                    return "flow/list-flow-cluster";


                } else if (flow.getType().equalsIgnoreCase("sqoop")) {
                    if (flow.getVariables2() != null) {
                        utils.Oracle_query_sqoop(conn.getHost(), conn.getPort(), conn.getSid(), conn.getDbUser(), conn.getPassword(), flow.getQuery(), prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/"), flow.getName() + "_reproceso",
                                flow.getSeparator_(), "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()), flow.getFileSize(),
                                flow.getSplit(),
                                flow.getMappers(),
                                flow.getVariables2(),
                                flow.getTableSize(),
                                conn.getType());

                        HdfsBalam hdfs = new HdfsBalam();
                        try {
                            hdfs.copyFromLocal(prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(flow.getName().concat("_reproceso.txt")), "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/");

                        } catch (IOException e) {
                            logger.error(e.getMessage());
                            model.addAttribute("message", "Error al copiar archivo " + prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(flow.getName().concat(".txt") + " hacia /user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/"));
                            return "flow/list-flow-cluster";
                        }

                        jobId = client.CreateWorkflow2(
                                flow.getEmails(),
                                flow.getType(),
                                flow.getName(),
                                flow.getIsDirectory(),
                                alertByMailResult,
                                flow.getPercentageToleranceRecords(),
                                flow.getMinute().concat(" ").
                                        concat(flow.getHour()).
                                        concat(" ").
                                        concat(flow.getWeekday()).
                                        concat(" ").
                                        concat(flow.getMonth()).
                                        concat(" ").
                                        concat(flow.getMonthday()),
                                conn.getUnixUser().concat("@").concat(conn.getHost()),
                                conn.getUnixPath().concat("/BALAM_INGESTAS"),
                                conn.getDbUser(),
                                conn.getPassword(),
                                conn.getSid(),
                                flow.getFilename(),
                                flow.getVariables2() != null ? flow.getVariables2() : "N/A",
                                utils.get_pathconf() + "/salida/",
                                flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                                "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                                flow.getQueueSqoop(),
                                flow.getSchemaDatabase(),
                                flow.getTablename(),
                                partitionedResult,
                                partitionedFieldResult,
                                overrideResult,
                                "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()),
                                flow.getEndDate(),
                                flow.getSeparator_(),
                                utils.get_dateformat("ORACLE"),
                                conn);

                        model.addAttribute("flowList", coordJobsInfo);
                        model.addAttribute("aux", aux);
                        model.addAttribute("mensajeSuccess", "Flujo lanzado exitosamente!.");
                        return "flow/list-flow-cluster";

                    } else {
                        jobId = client.CreateWorkflow(
                                flow.getEmails(),
                                flow.getType(),
                                flow.getName(),
                                flow.getIsDirectory(),
                                alertByMailResult,
                                flow.getPercentageToleranceRecords(),
                                flow.getMinute().concat(" ").
                                        concat(flow.getHour()).
                                        concat(" ").
                                        concat(flow.getWeekday()).
                                        concat(" ").
                                        concat(flow.getMonth()).
                                        concat(" ").
                                        concat(flow.getMonthday()),
                                conn.getUnixUser().concat("@").concat(conn.getHost()),
                                conn.getUnixPath().concat("/BALAM_INGESTAS"),
                                conn.getDbUser(),
                                conn.getPassword(),
                                conn.getSid(),
                                flow.getFilename(),
                                flow.getVariables2() != null ? flow.getVariables2() : "N/A",
                                utils.get_pathconf() + "/salida/",
                                flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                                "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                                flow.getQueueSqoop(),
                                flow.getSchemaDatabase(),
                                flow.getTablename(),
                                partitionedResult,
                                partitionedFieldResult,
                                overrideResult,
                                "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()),
                                flow.getEndDate(),
                                flow.getSeparator_(),
                                utils.get_dateformat("ORACLE"),
                                conn);

                        model.addAttribute("flowList", coordJobsInfo);
                        model.addAttribute("aux", aux);
                        model.addAttribute("mensajeSuccess", "Flujo lanzado exitosamente!.");
                        return "flow/list-flow-cluster";
                    }
                } else if (flow.getType().equalsIgnoreCase("archivo")) {
                    jobId = client.CreateWorkflow(
                            flow.getEmails(),
                            flow.getType(),
                            flow.getName(),
                            flow.getIsDirectory(),
                            alertByMailResult,
                            flow.getPercentageToleranceRecords(),
                            flow.getMinute().concat(" ").
                                    concat(flow.getHour()).
                                    concat(" ").
                                    concat(flow.getWeekday()).
                                    concat(" ").
                                    concat(flow.getMonth()).
                                    concat(" ").
                                    concat(flow.getMonthday()),
                            "NA",
                            "NA",
                            "NA",
                            "NA",
                            "NA",
                            flow.getFilename(),
                            flow.getVariables2() != null ? flow.getVariables2() : "N/A",
                            flow.getSourceDirectory(),
                            flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                            "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib",
                            flow.getQueue(),
                            flow.getSchemaDatabase(),
                            flow.getTablename(),
                            partitionedResult,
                            partitionedFieldResult,
                            overrideResult,
                            "N/A",
                            flow.getEndDate(),
                            "NA",
                            utils.get_dateformat("ORACLE").replace(" ", "_"),
                            conn);

                    model.addAttribute("flowList", coordJobsInfo);
                    model.addAttribute("aux", aux);
                    model.addAttribute("mensajeSuccess", "Flujo lanzado exitosamente!.");
                    return "flow/list-flow-cluster";

                }
                logger.info(jobId);

            } catch (OozieClientException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", e.getMessage());
                return "flow/list-flow-cluster";
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                model.addAttribute("flowList", coordJobsInfo);
                model.addAttribute("aux", aux);
                return "flow/list-flow-cluster";
            }
        } else {
            String mensaje = null;
            Flow flow2 = flowService.findByName(coord);
// if ((flow2.getVariables2() != null || flow2.getVariables2().equals(""))) {
            String variables2 = flow2.getVariables2();
            if ((variables2 != null)) {
                if (flow2.getType().equals("sqoop")) {
                    mensaje = client.createWorkflowDependencias2(coord);
                    model.addAttribute("flowList", coordJobsInfo);
                    model.addAttribute("aux", aux);
                } else {
                    mensaje = client.createWorkflowDependencias3(coord);
                    model.addAttribute("flowList", coordJobsInfo);
                    model.addAttribute("aux", aux);

                }

            } else {
                mensaje = client.createWorkflowDependencias(coord);
                model.addAttribute("flowList", coordJobsInfo);
                model.addAttribute("aux", aux);
            }

            if (mensaje != null) {
                model.addAttribute("flowList", coordJobsInfo);
                model.addAttribute("aux", aux);
                model.addAttribute("mensaje", mensaje);
                return "flow/list-flow-cluster";
            } else {
                model.addAttribute("flowList", coordJobsInfo);
                model.addAttribute("aux", aux);
                model.addAttribute("mensajeSuccess", "Flujo lanzado exitosamente!");
                return "flow/list-flow-cluster";
            }
        }

        return "redirect:/flows-cluster/list";

    }


    public class FlowParameters {
        private String key_;
        private String value_;

        public FlowParameters(String key_, String value_) {
            this.key_ = key_;
            this.value_ = value_;
        }

        public String getKey_() {
            return key_;
        }

        public void setKey_(String key_) {
            this.key_ = key_;
        }

        public String getValue_() {
            return value_;
        }

        public void setValue_(String value_) {
            this.value_ = value_;
        }
    }

    public class FlowAux {
        private Flow flow;
        private CoordinatorJob coordinatorJob;
        boolean dependencia;
        private List<FlowParameters> flowParameters;


        public FlowAux(Flow flow, CoordinatorJob coordinatorJob, List<FlowParameters> flowParameters, boolean dependencia) {
            this.flow = flow;
            this.coordinatorJob = coordinatorJob;
            this.flowParameters = flowParameters;
            this.dependencia = dependencia;
        }

        public Flow getFlow() {
            return flow;
        }

        public void setFlow(Flow flow) {
            this.flow = flow;
        }

        public List<FlowParameters> getFlowParameters2() {
            return flowParameters;
        }

        public void setFlowParameters(List<FlowParameters> flowParameters) {
            this.flowParameters = flowParameters;
        }

        public CoordinatorJob getCoordinatorJob() {
            return coordinatorJob;
        }

        public void setCoordinatorJob(CoordinatorJob coordinatorJob) {
            this.coordinatorJob = coordinatorJob;
        }

        public boolean getDependencia() {
            return dependencia;
        }

        public void setDependencia(boolean dependencia) {
            this.dependencia = dependencia;
        }
    }


    @PostMapping("/save")

    public String save(Flow flow, Model model) {
        logger.info("capturando datos: " + flow.toString());

        Flow flowPersist = flowService.findByName(flow.getName());
        flowPersist.setVariables2(flow.getVariables2());

        System.out.println("Flow Persist" + flowPersist);
        logger.info("combinar datos: " + flowPersist.toString());

        // flowService.save(flow);
        flowService.save(flowPersist);
        model.addAttribute("mensaje", "Se han editado los parámetros satisfactoriamente!");


        return "redirect:/flows-cluster/list";
    }

}

