package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Connection;
import net.crunchdroid.model.Flow;
import net.crunchdroid.oozie.OozieClientConfig;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.service.FlowService;
import net.crunchdroid.service.SeparatorFieldService;
import net.crunchdroid.service.TableTypeService;
import net.crunchdroid.shell.hdfs.HdfsBalam;
import net.crunchdroid.shell.local.CreaLoc;
import net.crunchdroid.shell.local.GeneralesLoc;
import net.crunchdroid.shell.remote.ConexionRem;
import net.crunchdroid.shell.remote.CreaRem;
import net.crunchdroid.shell.remote.GeneralesRem;
import net.crunchdroid.util.FlowValidation;
import net.crunchdroid.util.ResultResponse;
import net.crunchdroid.util.Utils;
import org.apache.oozie.client.OozieClientException;
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
import java.util.*;

@Controller
@RequestMapping("/sqoop-flow")
public class SqoopFlowController {

    Logger logger = LoggerFactory.getLogger(SqoopFlowController.class);

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private FlowService flowService;

    @Autowired
    private SeparatorFieldService separatorFieldService;

    @Autowired
    private TableTypeService tableTypeService;

   /* @Autowired
    private Environment env;*/


    private OozieClientConfig oozieClientConfig = new OozieClientConfig();
    private Utils utils = new Utils();
    private FlowValidation validation = new FlowValidation();

    private CreaLoc creaLocal = null;
    private GeneralesLoc generalesLocal = null;

    /*@Value("${ingestas.home}")
    private String INGESTAS_HOME;*/

    @GetMapping("/create")
    public String create(Model model) {

        logger.info("Cargando sqoop flow view");

        model.addAttribute("connectionList", connectionService.findAll());
        model.addAttribute("separatorList", separatorFieldService.findAll());
        model.addAttribute("tableList", tableTypeService.findAll());
        model.addAttribute("flow", new Flow());
        model.addAttribute("connectionSelected", new Connection());
        return "flow/sqoop-flow";
    }

    @PostMapping("/save")
    public String save(Flow flow, Model model,
                       @RequestParam(value = "action", required = true) String action,
                       @RequestParam(value = "query", required = true) String consulta
    ) {

        if (action.equals("save")) {
            String vars = null;

            if (consulta == null || consulta.isEmpty() || consulta.length() <= 0) {
                model.addAllAttributes(setCombos());
                return "flow/spool-flow";

            }
            ArrayList<Integer> list = new ArrayList<Integer>();
            ArrayList<String> param = new ArrayList<String>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }

            if (list.size() != 0) {
                ArrayList<String> variables = new ArrayList<String>();
                for (Integer i : list) {
                    String p_var = "";
                    char ch = '\'';
                    for (int j = i; j < consulta.length(); j++) {
                        if (consulta.charAt(j) == ch)
                            break;
                        p_var = p_var + consulta.charAt(j);
                    }
                    variables.add(p_var);
                }
                Collections.sort(variables);

                int index = 0;
                vars = "";
                boolean flag = false;
                for (int i = 0; i < variables.size(); i++) {
                    flag = false;
                    for (int j = 0; j < i; j++) {
                        if (variables.get(i).equals(variables.get(j)) && i != j) {
                            flag = true;
                            break;
                        }
                    }

                    if (flag == false) {
                        param.add(variables.get(i));
                    }
                }

                for (String i : param) {
                    if (index == param.size() - 1)
                        vars += i;
                    else
                        vars += i + ",";
                    index++;
                }

            }
            model.addAttribute("parametros", vars);
            model.addAttribute("query", consulta);

            System.out.println(vars);
            model.addAllAttributes(setCombos());
        }

        if (action.equals("create")) {

            if (flow.getVariables() != null) {
                System.out.println(flow.getVariables());
            }
            logger.info("Create sqoop flow");
            logger.info(flow.toString());


            flow.setType("sqoop");
            ResultResponse result = validation.validateFields(flow);
            if (!result.getResult()) {
                List<String> validations = (List<String>) result.getObject();
                model.addAllAttributes(setCombos());
                model.addAttribute("messageValidation", "Has errors");
                model.addAttribute("validations", validations);
                return "flow/sqoop-flow";
            }

            if (flowService.existsByName(flow.getName())) {
                logger.info("Ya existe el flujo" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Ya existe el flujo" + flow.getName());
                return "flow/sqoop-flow";
            }

            if (flow.getQuery().contains("&")) {
                if (flow.getVariables() == null || flow.getVariables().equals("")) {
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", "Configure los parametros del query!");
                    return "flow/spool-flow";
                }
            }

            /*
            version anterior al 14 de marzo de 2019
            String vars = "";
            if (flow.getVariables() == null || flow.getVariables().equals("")) {
                vars = "N/A";
            } else {
                vars = flow.getVariables();
            }*/


            //Cambios Realizados el 14 de marzo de 2019, para que setear en variables 1 y 2 =null
            String vars = "";
            if (flow.getVariables().length() <= 0) {
                flow.setVariables(null);
                flow.setVariables2(null);
                vars = "N/A";
            } else {
                vars = flow.getVariables();
            }


            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch(IOException e) {
                System.out.println(e.toString());
            }

            Connection conn = connectionService.findOne(flow.getConnectionId());

            logger.info("validando query ------------------");
            String query = utils.Oracle_query_sqoop(
                    conn.getHost(),
                    conn.getPort(),
                    conn.getSid(),
                    conn.getDbUser(),
                    conn.getPassword(),
                    flow.getQuery(),
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/"),
                    flow.getName(),
                    flow.getSeparator_(),
                    "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()),
                    flow.getFileSize(),
                    flow.getSplit(),
                    flow.getMappers(),
                    vars,
                    flow.getTableSize(),
                    conn.getType()
            );
            if (!"OK".equalsIgnoreCase(query)) {
                model.addAttribute("message", query);
                model.addAllAttributes(setCombos());
                return "flow/sqoop-flow";
            }

            creaLocal = new CreaLoc();
            generalesLocal = new GeneralesLoc();
            HdfsBalam hdfs = new HdfsBalam();


            logger.info("Copiando archivo hdfs----------------");
            try {
                hdfs.copyFromLocal(prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(flow.getName().concat(".txt")), "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/");
            } catch (IOException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al copiar archivo " + prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(flow.getName().concat(".txt") + " hacia /user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/"));
                model.addAllAttributes(setCombos());
                return "flow/sqoop-flow";
            }

            logger.info("mkdir salida ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/salida/",
                    flow.getName(), "-p") != 0) {
                model.addAttribute("message", "Error al crear directorio" + utils.get_pathconf() + "/salida/".concat(flow.getName()));
                model.addAllAttributes(setCombos());
                return "flow/sqoop-flow";
            }

            logger.info("mkdir reg ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/reg/",
                    flow.getName(), "-p") != 0) {
                model.addAttribute("message", "Error al crear directorio" + utils.get_pathconf() + "/reg/".concat(flow.getName()));
                model.addAllAttributes(setCombos());
                return "flow/sqoop-flow";
            }

            if ("tabla".equalsIgnoreCase(flow.getIsDirectory())) {
                logger.info("Creando carpeta hdfs spark test----------------");
                try {
                    hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", "Error al crear carpeta en hdfs: /user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                    model.addAllAttributes(setCombos());
                    return "flow/sqoop-flow";
                }
            } else if ("carpeta".equalsIgnoreCase(flow.getIsDirectory())) {
                logger.info("Creando carpeta hdfs ----------------");
                try {
                    hdfs.mkdir(flow.getDirectory());
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", "Error al crear carpeta en hdfs: /user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                    model.addAllAttributes(setCombos());
                    return "flow/sqoop-flow";
                }
            }

            logger.info("Creando carpeta hdfs data_test----------------");
            try {
                hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()));
            } catch (IOException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al crear carpeta en hdfs: /user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()));
                model.addAllAttributes(setCombos());
                return "flow/sqoop-flow";
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
            if (flow.getParticioned() == null) {
                partitionedResult = "NO";
                partitionedFieldResult = "N/A";
            } else {
                partitionedFieldResult = flow.getParticionedField();
            }


            if (flow.getIsSchedule().equalsIgnoreCase("yes")) {
                logger.info("creando coordinator ------------------");
                try {
                    String jobId = oozieClientConfig.CreateCoordinator(flow.getEmails(),
                            "sqoop",
                            flow.getName(),
                            flow.getIsDirectory(),
                            alertByMailResult,
                            flow.getPercentageToleranceRecords(),
                            flow.getMinute().concat(" ").
                                    concat(utils.parseCronhour(flow.getHour())).
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
                            vars,
                            //"/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
                            utils.get_pathconf() + "/salida/",
                            flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                            "${app_path}/lib",
                            flow.getQueueSqoop().toLowerCase(),
                            flow.getSchemaDatabase(),
                            flow.getTablename(),
                            partitionedResult,
                            partitionedFieldResult,
                            overrideResult,
                            "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()),
                            flow.getEndDate(),
                            flow.getStartDate(),
                            flow.getSeparator_(),
                            utils.get_dateformat("ORACLE"),
                            conn);

                    if (jobId != null) {
                        flow.setJobId(jobId);
                    }

                } catch (OozieClientException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", e.getMessage());
                    model.addAllAttributes(setCombos());
                    return "flow/sqoop-flow";
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", e.getMessage());
                    model.addAllAttributes(setCombos());
                    return "flow/sqoop-flow";
                }
            }//fin de programar ahora

            logger.info("Guardando flujo...");
            Flow persistSqoop = flowService.save(flow);

            if (persistSqoop.getId() == null) {
                model.addAttribute("message", "No se pudo crear el flujo en base de datos, pero si se ha configurado en Oozie.");
                model.addAllAttributes(setCombos());
                return "flow/sqoop-flow";
            }

            //return "redirect:/flows/list";
            return "redirect:/monitor/coordinator";
        }
        return "flow/sqoop-flow";
    }

    @PostMapping("/edit")
    public String edit(Flow flow,
                       Model model,
                       @RequestParam(value = "parameters", required = false) String parameters,
                       @RequestParam(value = "query", required = false) String consulta) {

        if (parameters.equals("edit")) {
            String vars = null;

            if (consulta == null || consulta.isEmpty() || consulta.length() <= 0) {
                model.addAllAttributes(setCombos());
                return "flow/spool-flow";

            }
            ArrayList<Integer> list = new ArrayList<Integer>();
            ArrayList<String> param = new ArrayList<String>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }

            if (list.size() != 0) {
                ArrayList<String> variables = new ArrayList<String>();
                for (Integer i : list) {
                    String p_var = "";
                    char ch = '\'';
                    for (int j = i; j < consulta.length(); j++) {
                        if (consulta.charAt(j) == ch)
                            break;
                        p_var = p_var + consulta.charAt(j);
                    }
                    variables.add(p_var);
                }
                Collections.sort(variables);

                int index = 0;
                vars = "";
                boolean flag = false;
                for (int i = 0; i < variables.size(); i++) {
                    flag = false;
                    for (int j = 0; j < i; j++) {
                        if (variables.get(i).equals(variables.get(j)) && i != j) {
                            flag = true;
                            break;
                        }
                    }

                    if (flag == false) {
                        param.add(variables.get(i));
                    }
                }

                for (String i : param) {
                    if (index == param.size() - 1)
                        vars += i;
                    else
                        vars += i + ",";
                    index++;
                }

            }
            model.addAttribute("parametros", vars);
            model.addAttribute("query", consulta);
            model.addAllAttributes(setCombos());
        }

        if (parameters.equals("create")) {

            if (flow.getVariables() != null) {
                System.out.println(flow.getVariables());
            }

            logger.info("Create sqoop flow");
            logger.info("receive: " + flow.toString());
            Flow foundFlow = flowService.findOne(flow.getId());
            logger.info("find: " + flow.toString());

            flow.setType("sqoop");
            ResultResponse result = validation.validateFields(flow);
            if (!result.getResult()) {
                List<String> validations = (List<String>) result.getObject();
                model.addAllAttributes(setCombos());
                model.addAttribute("messageValidation", "Has errors");
                model.addAttribute("validations", validations);
                return "flow/edit-sqoop-flow";
            }

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch(IOException e) {
                System.out.println(e.toString());
            }

            Connection conn = connectionService.findOne(flow.getConnectionId());

            logger.info("validando query ------------------");
            String query = utils.Oracle_query_sqoop(
                    conn.getHost(),
                    conn.getPort(),
                    conn.getSid(),
                    conn.getDbUser(),
                    conn.getPassword(),
                    flow.getQuery(),
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/"),
                    flow.getName(),
                    flow.getSeparator_(),
                    "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()),
                    flow.getFileSize(),
                    flow.getSplit(),
                    flow.getMappers(),
                    flow.getVariables(),
                    //  "N/A",
                    flow.getTableSize(), conn.getType()
            );
            if (!"OK".equalsIgnoreCase(query)) {
                model.addAttribute("message", query);
                model.addAllAttributes(setCombos());
                return "flow/edit-sqoop-flow";
            }

            creaLocal = new CreaLoc();
            generalesLocal = new GeneralesLoc();
            HdfsBalam hdfs = new HdfsBalam();


            logger.info("Copiando archivo hdfs----------------");
            try {
                hdfs.copyFromLocal(prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(flow.getName().concat(".txt")), "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/");
            } catch (IOException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al copiar archivo " + prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(flow.getName().concat(".txt") + " hacia /user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/"));
                model.addAllAttributes(setCombos());
                return "flow/edit-sqoop-flow";
            }

            logger.info("mkdir salida ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/salida/",
                    flow.getName(), "-p") != 0) {
                model.addAttribute("message", "Error al crear directorio" + utils.get_pathconf() + "/salida/".concat(flow.getName()));
                model.addAllAttributes(setCombos());
                return "flow/edit-sqoop-flow";
            }

            logger.info("mkdir reg ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/reg/",
                    flow.getName(), "-p") != 0) {
                model.addAttribute("message", "Error al crear directorio" + utils.get_pathconf() + "/reg/".concat(flow.getName()));
                model.addAllAttributes(setCombos());
                return "flow/edit-sqoop-flow";
            }

            if ("tabla".equalsIgnoreCase(flow.getIsDirectory())) {
                logger.info("Creando carpeta hdfs spark test----------------");
                try {
                    hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", "Error al crear carpeta en hdfs: /user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                    model.addAllAttributes(setCombos());
                    return "flow/edit-sqoop-flow";
                }
            } else if ("carpeta".equalsIgnoreCase(flow.getIsDirectory())) {
                logger.info("Creando carpeta hdfs ----------------");
                try {
                    hdfs.mkdir(flow.getDirectory());
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", "Error al crear carpeta en hdfs: " + flow.getDirectory());
                    model.addAllAttributes(setCombos());
                    return "flow/edit-sqoop-flow";
                }
            }


            logger.info("Creando carpeta hdfs data_test----------------");
            try {
                hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()));
            } catch (IOException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al crear carpeta en hdfs: /user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()));
                model.addAllAttributes(setCombos());
                return "flow/edit-sqoop-flow";
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
            if (flow.getParticioned() == null) {
                partitionedResult = "NO";
                partitionedFieldResult = "N/A";
            } else {
                partitionedFieldResult = flow.getParticionedField();
            }

            if (flow.getIsSchedule().equalsIgnoreCase("yes")) {
                if (foundFlow.getJobId() != null && foundFlow.getJobId().length() > 0) {
                    logger.info("eliminando coordinator ------------------");
                    try {
                        oozieClientConfig.kill(foundFlow.getJobId());
                    } catch (OozieClientException e) {
                        model.addAllAttributes(setCombos());
                        model.addAttribute("messageValidation", "Has errors");
                        model.addAttribute("validations", e.getMessage());
                        logger.error(e.getMessage());
                        return "flow/edit-sqoop-flow";
                    }
                }

                /*String vars = "";
                if (flow.getVariables() == null || flow.getVariables().equals("")) {
                    vars = "N/A";
                } else {
                    vars = flow.getVariables();
                }*/

                String vars = "";
                if (flow.getVariables().length() <= 0) {
                    flow.setVariables(null);
                    flow.setVariables2(null);
                    vars = "N/A";
                } else {
                    vars = flow.getVariables();
                }

                logger.info("creando coordinator ------------------");
                try {
                    String jobId = oozieClientConfig.CreateCoordinator(flow.getEmails(),
                            "sqoop",
                            flow.getName(),
                            flow.getIsDirectory(),
                            alertByMailResult,
                            flow.getPercentageToleranceRecords(),
                            flow.getMinute().concat(" ").
                                    concat(utils.parseCronhour(flow.getHour())).
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
                            vars,
                            //"A,P9",
                            //"/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
                            utils.get_pathconf() + "/salida/",
                            flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                            "${app_path}/lib",
                            flow.getQueueSqoop().toLowerCase(),
                            flow.getSchemaDatabase(),
                            flow.getTablename(),
                            partitionedResult,
                            partitionedFieldResult,
                            overrideResult,
                            "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(flow.getName()),
                            flow.getEndDate(),
                            flow.getStartDate(),
                            flow.getSeparator_(),
                            utils.get_dateformat("ORACLE"),
                            conn);

                    if (jobId != null) {
                        flow.setJobId(jobId);
                    }

                } catch (OozieClientException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", e.getMessage());
                    model.addAllAttributes(setCombos());
                    return "flow/edit-sqoop-flow";
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    model.addAttribute("message", e.getMessage());
                    model.addAllAttributes(setCombos());
                    return "flow/edit-sqoop-flow";
                }
            }

            logger.info("Guardando flujo...");
            Flow persistSqoop = flowService.save(flow);

            if (persistSqoop.getId() == null) {
                model.addAttribute("message", "No se pudo editar el flujo en base de datos, pero si se ha configurado en Oozie.");
                model.addAllAttributes(setCombos());
                return "flow/edit-sqoop-flow";
            }
            return "redirect:/monitor/coordinator";
        }
        return "flow/edit-sqoop-flow";
    }

    private Map<String, Object> setCombos() {
        Map<String, Object> map = new HashMap<>();
        map.put("connectionList", connectionService.findAll());
        map.put("separatorList", separatorFieldService.findAll());
        map.put("tableList", tableTypeService.findAll());
        return map;
    }

}
