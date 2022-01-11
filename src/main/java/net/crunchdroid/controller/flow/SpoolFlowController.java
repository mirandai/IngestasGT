package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Connection;
import net.crunchdroid.model.Flow;
import net.crunchdroid.oozie.OozieClientConfig;
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
@RequestMapping("/spool-flow")
public class SpoolFlowController {

    Logger logger = LoggerFactory.getLogger(SpoolFlowController.class);

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private FlowService flowService;

    @Autowired
    private SeparatorFieldService separatorFieldService;



    private OozieClientConfig oozieClientConfig = new OozieClientConfig();
    private Utils utils = new Utils();
    private FlowValidation validation = new FlowValidation();

    private ConexionRem conexion = null;
    private CreaRem crearRemoto = null;
    private CreaLoc creaLocal = null;
    private GeneralesRem generalesRem = null;
    private GeneralesLoc generalesLocal = null;


    @GetMapping("/create")
    public String create(Model model) {
        logger.info("Spool flow view");
        List<String> types = new ArrayList<>();
        types.add("yes");
        types.add("no");
        types.add("maybe");
        model.addAttribute("singleSelectAllValues", types);
        model.addAttribute("connectionList", connectionService.findAll());
        model.addAttribute("separatorList", separatorFieldService.findAll());
        Flow flow = new Flow();
        flow.setIsDirectory("tabla");
        model.addAttribute("flow", flow);
        model.addAttribute("connectionSelected", new Connection());
        return "flow/spool-flow";
    }

    @PostMapping("/save")
    public String save(Flow flow,
                       Model model,
                       @RequestParam(value = "action", required = true) String action,
                       @RequestParam(value = "query", required = true) String consulta) {

        //inicia cambios parametros
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
            logger.info("Create spool flow");
            logger.info(flow.toString());
//fin cambios parametros

            flow.setType("spool");
            ResultResponse result = validation.validateFields(flow);
            if (!result.getResult()) {
                List<String> validations = (List<String>) result.getObject();
                model.addAllAttributes(setCombos());
                model.addAttribute("messageValidation", "Has errors");
                model.addAttribute("validations", validations);
                return "flow/spool-flow";
            }

            if (flowService.existsByName(flow.getName())) {
                logger.info("Ya existe el flujo" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Ya existe el flujo" + flow.getName());
                return "flow/spool-flow";
            }

            Connection conn = connectionService.findOne(flow.getConnectionId());

            if (conn.getUnixPath() == null || conn.getUnixPath().length() <= 0) {
                logger.info("Unix path can't be null or empty" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Unix path can't be null or empty on connection selected");
                return "flow/spool-flow";
            }

            if (conn.getUnixUser() == null || conn.getUnixPath().length() <= 0) {
                logger.info("Unix user can't be null or empty" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Unix user can't be null or empty on connection selected");
                return "flow/spool-flow";
            }

            if (conn.getUnixUser() == null || conn.getUnixPath().length() <= 0) {
                logger.info("Unix user can't be null or empty" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Unix user can't be null or empty on connection selected");
                return "flow/spool-flow";
            }

            logger.info(flow.getStartDate() + " " + flow.getEndDate());

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch(IOException e) {
                System.out.println(e.toString());
            }


            logger.info("validando query ------------------");
            String query = (utils.Oracle_query_spool(
                    conn.getHost(),
                    conn.getPort(),
                    conn.getSid(),
                    conn.getDbUser(),
                    conn.getPassword(),
                    flow.getQuery(),
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/sql/"),
                    flow.getName(),
                    conn.getType()));

            logger.info(query);
            if (!"OK".equalsIgnoreCase(query)) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Inconveniente en la consulta, " + query);
                return "flow/spool-flow";
            }

            conexion = new ConexionRem(conn.getUnixUser(), conn.getHost());
            crearRemoto = new CreaRem(conexion);
            generalesRem = new GeneralesRem(conexion);
            creaLocal = new CreaLoc();
            generalesLocal = new GeneralesLoc();

            logger.info("mkdir logs ------------------");
            if (crearRemoto.creaDirRem(conn.getUnixPath(), "/BALAM_INGESTAS/logs/".concat(flow.getName()), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de creación en servidor remoto: " + "/BALAM_INGESTAS/logs/".concat(flow.getName()));
                return "flow/spool-flow";
            }

            logger.info("mkdir sql ------------------");
            if (crearRemoto.creaDirRem(conn.getUnixPath(), "/BALAM_INGESTAS/sql/", "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de creación en servidor remoto: " + "/BALAM_INGESTAS/sql/");
                return "flow/spool-flow";
            }

            logger.info("mkdir salida ------------------");
            if (crearRemoto.creaDirRem(conn.getUnixPath(), "/BALAM_INGESTAS/salida/".concat(flow.getName()), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de creación en servidor remoto: " + "/BALAM_INGESTAS/salida/".concat(flow.getName()));
                return "flow/spool-flow";
            }

            logger.info("copiar extractor --------------------");
            if (crearRemoto.copiaRem(
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/"),
                    "ejecuta_extractor.sh",
                    conn.getUnixPath().concat("/BALAM_INGESTAS/")) != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de copia extractor en servidor remoto.");
                return "flow/spool-flow";
            }

            logger.info("copiar sql--------------------");
            if (crearRemoto.copiaRem(
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/sql/"),
                    flow.getName().concat(".sql"),
                    conn.getUnixPath().concat("/BALAM_INGESTAS/sql")) != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de copia sql en servidor remoto.");
                return "flow/spool-flow";
            }


            logger.info("mkdir salida ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
            if (creaLocal.creaDir(utils.get_pathconf().concat("/salida/"),
                    flow.getName(), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error al crear directorio salida.");
                return "flow/spool-flow";
            }

            logger.info("mkdir reg ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/reg/",
                    flow.getName(), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error al crear el directorio reg.");
                return "flow/spool-flow";
            }

            if ("tabla".equalsIgnoreCase(flow.getIsDirectory())) {
                HdfsBalam hdfs = new HdfsBalam();
                logger.info("Creando carpeta hdfs ----------------");
                try {
                    hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                } catch (IOException e) {
                    logger.error("Creando carpeta hdfs error ----------------");
                    logger.error(e.getMessage());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", "Error al crear la carpeta hdfs." + "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                    return "flow/spool-flow";
                }
            } else if ("carpeta".equalsIgnoreCase(flow.getIsDirectory())) {
                HdfsBalam hdfs = new HdfsBalam();
                logger.info("Creando carpeta hdfs ----------------");
                try {
                    hdfs.mkdir(flow.getDirectory());
                } catch (IOException e) {
                    logger.error("Creando carpeta hdfs error ----------------");
                    logger.error(e.getMessage());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", "Error al crear la carpeta hdfs." + flow.getDirectory());
                    return "flow/spool-flow";
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


        //Cambios Realizados el 14 de marzo de 2019, para que setear en variables 1 y 2 =null
            String vars = "";
            if (flow.getVariables().length() <= 0) {
                flow.setVariables(null);
                flow.setVariables2(null);
                vars = "N/A";
            } else {
                vars = flow.getVariables();
            }

            if (flow.getIsSchedule().equalsIgnoreCase("yes")) {
                logger.info("Creando coordinator ------------------");
                try {
                    String jobId = oozieClientConfig.CreateCoordinator(flow.getEmails(),
                            "spool",
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
                            // "A,P9",
                            //"/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
                            utils.get_pathconf() + "/salida/",
                            flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                            "${app_path}/lib",
                            flow.getQueue().toLowerCase(),
                            flow.getSchemaDatabase(),
                            flow.getTablename(),
                            partitionedResult,
                            partitionedFieldResult,
                            overrideResult,
                            "N/A",
                            flow.getEndDate(),
                            flow.getStartDate(),
                            (flow.getSeparator_().equalsIgnoreCase("|") ? "\\|" : flow.getSeparator_()),
                            utils.get_dateformat("ORACLE").replace(" ", "_"),
                            conn);

                    if (jobId != null) {
                        flow.setJobId(jobId);
                    }


                } catch (OozieClientException e) {
                    logger.error(e.getMessage());
                    //logger.error(e.getCause().toString());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", e.getMessage());
                    return "flow/spool-flow";
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    model.addAllAttributes(setCombos());
                    //logger.error(e.getCause().toString());
                    model.addAttribute("message", e.getMessage());
                    return "flow/spool-flow";
                }
            }//fin programacion ahora.

            Flow persistFlow = flowService.save(flow);

            if (persistFlow.getId() == null) {
                model.addAttribute("message", "No se pudo crear el flujo en base de datos, pero si se ha configurado en Oozie.");
                model.addAllAttributes(setCombos());
                return "flow/spool-flow";
            }

            //return "redirect:/flows/list";
            return "redirect:/monitor/coordinator";
        }
        return "flow/spool-flow";
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

            logger.info("Create spool flow");
            logger.info("receive: " + flow.toString());
            Flow foundFlow = flowService.findOne(flow.getId());
            logger.info("find: " + flow.toString());

            flow.setType("spool");
            ResultResponse result = validation.validateFields(flow);
            if (!result.getResult()) {
                List<String> validations = (List<String>) result.getObject();
                model.addAllAttributes(setCombos());
                model.addAttribute("messageValidation", "Has errors");
                model.addAttribute("validations", validations);
                return "flow/edit-spool-flow";
            }

            Connection conn = connectionService.findOne(flow.getConnectionId());

            if (conn.getUnixPath() == null || conn.getUnixPath().length() <= 0) {
                logger.info("Unix path can't be null or empty" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Unix path can't be null or empty on connection selected");
                return "flow/edit-spool-flow";
            }

            if (conn.getUnixUser() == null || conn.getUnixPath().length() <= 0) {
                logger.info("Unix user can't be null or empty" + flow.getName());
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Unix user can't be null or empty on connection selected");
                return "flow/edit-spool-flow";
            }

            logger.info(flow.getStartDate() + " " + flow.getEndDate());

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch(IOException e) {
                System.out.println(e.toString());
            }


            logger.info("validando query ------------------");
            String query = (utils.Oracle_query_spool(conn.getHost(),
                    conn.getPort(),
                    conn.getSid(),
                    conn.getDbUser(),
                    conn.getPassword(),
                    flow.getQuery(),
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/sql/"),
                    flow.getName(),
                    conn.getType()));

            logger.info(query);
            if (!"OK".equalsIgnoreCase(query)) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Inconveniente en la consulta, " + query);
                return "flow/edit-spool-flow";
            }

            conexion = new ConexionRem(conn.getUnixUser(), conn.getHost());
            crearRemoto = new CreaRem(conexion);
            generalesRem = new GeneralesRem(conexion);
            creaLocal = new CreaLoc();
            generalesLocal = new GeneralesLoc();

            logger.info("mkdir logs ------------------");
            if (crearRemoto.creaDirRem(conn.getUnixPath(), "/BALAM_INGESTAS/logs/".concat(flow.getName()), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de creación en servidor remoto: " + "/BALAM_INGESTAS/logs/".concat(flow.getName()));
                return "flow/edit-spool-flow";
            }

            logger.info("mkdir sql ------------------");
            if (crearRemoto.creaDirRem(conn.getUnixPath(), "/BALAM_INGESTAS/sql/", "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de creación en servidor remoto: " + "/BALAM_INGESTAS/sql/");
                return "flow/edit-spool-flow";
            }

            logger.info("mkdir salida ------------------");
            if (crearRemoto.creaDirRem(conn.getUnixPath(), "/BALAM_INGESTAS/salida/".concat(flow.getName()), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de creación en servidor remoto: " + "/BALAM_INGESTAS/salida/".concat(flow.getName()));
                return "flow/edit-spool-flow";
            }

            logger.info("copiar extractor --------------------");
            if (crearRemoto.copiaRem(
                    prop.getProperty("home") + "/INGESTAS_BALAM/config/lib/",
                    "ejecuta_extractor.sh",
                    conn.getUnixPath().concat("/BALAM_INGESTAS/")) != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de copia extractor en servidor remoto.");
                return "flow/edit-spool-flow";
            }

            logger.info("copiar sql--------------------");
            if (crearRemoto.copiaRem(
                    prop.getProperty("home") + "/INGESTAS_BALAM/config/sql/",
                    flow.getName().concat(".sql"),
                    conn.getUnixPath().concat("/BALAM_INGESTAS/sql")) != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error de copia sql en servidor remoto.");
                return "flow/edit-spool-flow";
            }


            logger.info("mkdir salida ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/salida/",
                    flow.getName(), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error al crear directorio salida.");
                return "flow/edit-spool-flow";
            }

            logger.info("mkdir reg ------------------");
            //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/",
            if (creaLocal.creaDir(utils.get_pathconf() + "/reg/",
                    flow.getName(), "-p") != 0) {
                model.addAllAttributes(setCombos());
                model.addAttribute("message", "Error al crear el directorio reg.");
                return "flow/edit-spool-flow";
            }

            if ("tabla".equalsIgnoreCase(flow.getIsDirectory())) {
                HdfsBalam hdfs = new HdfsBalam();
                logger.info("Creando carpeta hdfs ----------------");
                try {
                    hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                } catch (IOException e) {
                    logger.error("Creando carpeta hdfs error ----------------");
                    logger.error(e.getMessage());
                    //logger.error(e.getCause().toString());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", "Error al crear la carpeta hdfs." + "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                    return "flow/edit-spool-flow";
                }
            } else if ("carpeta".equalsIgnoreCase(flow.getIsDirectory())) {
                HdfsBalam hdfs = new HdfsBalam();
                logger.info("Creando carpeta hdfs ----------------");
                try {
                    hdfs.mkdir(flow.getDirectory());
                } catch (IOException e) {
                    logger.error("Creando carpeta hdfs error ----------------");
                    logger.error(e.getMessage());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", "Error al crear la carpeta hdfs." + flow.getDirectory());
                    return "flow/spool-flow";
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

            if (flow.getIsSchedule().equalsIgnoreCase("yes")) {
                if (foundFlow.getJobId() != null && foundFlow.getJobId().length() > 0) {
                    logger.info("Eliminando coordinator ------------------");
                    try {
                        oozieClientConfig.kill(foundFlow.getJobId());
                    } catch (OozieClientException e) {
                        model.addAllAttributes(setCombos());
                        model.addAttribute("messageValidation", "Has errors");
                        model.addAttribute("validations", e.getMessage());
                        logger.error(e.getMessage());
                        return "flow/edit-spool-flow";
                    }
                }

                String vars = "";
                if (flow.getVariables().length() <= 0) {
                    flow.setVariables(null);
                    flow.setVariables2(null);
                    vars = "N/A";
                } else {
                    vars = flow.getVariables();
                }

                logger.info("Creando coordinator ------------------");
                try {
                    String jobId = oozieClientConfig.CreateCoordinator(flow.getEmails(),
                            "spool",
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
                            utils.get_pathconf() + "/salida/",
                            flow.getIsDirectory().equalsIgnoreCase("tabla") ? "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow}" : flow.getDirectory(),
                            "${app_path}/lib",
                            flow.getQueue().toLowerCase(),
                            flow.getSchemaDatabase(),
                            flow.getTablename(),
                            partitionedResult,
                            partitionedFieldResult,
                            overrideResult,
                            "N/A",
                            flow.getEndDate(),
                            flow.getStartDate(),
                            (flow.getSeparator_().equalsIgnoreCase("|") ? "\\|" : flow.getSeparator_()),
                            utils.get_dateformat("ORACLE").replace(" ", "_")
                            , conn);

                    if (jobId != null) {
                        flow.setJobId(jobId);
                    }


                } catch (OozieClientException e) {
                    logger.error(e.getMessage());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", e.getMessage());
                    return "flow/edit-spool-flow";
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                    model.addAllAttributes(setCombos());
                    model.addAttribute("message", e.getMessage());
                    return "flow/edit-spool-flow";
                }
            }//fin programar ahora

            Flow persistFlow = flowService.save(flow);

            if (persistFlow.getId() == null) {
                model.addAttribute("message", "No se pudo crear el flujo en base de datos, pero si se ha configurado en Oozie.");
                model.addAllAttributes(setCombos());
                return "flow/edit-spool-flow";
            }

            return "redirect:/monitor/coordinator";
        }
        return "flow/edit-spool-flow";
    }

    private Map<String, Object> setCombos() {
        Map<String, Object> map = new HashMap<>();
        List<String> types = new ArrayList<>();
        types.add("yes");
        types.add("no");
        types.add("maybe");
        map.put("singleSelectAllValues", types);
        map.put("connectionList", connectionService.findAll());
        map.put("separatorList", separatorFieldService.findAll());
        return map;
    }

}
