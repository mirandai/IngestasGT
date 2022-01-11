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
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/file-flow")
public class FileFlowController {

    Logger logger = LoggerFactory.getLogger(FileFlowController.class);

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private FlowService flowService;

    @Autowired
    private SeparatorFieldService separatorFieldService;

    private OozieClientConfig oozieClientConfig = new OozieClientConfig();
    private Utils utils = new Utils();
    private HdfsBalam hdfs = new HdfsBalam();
    private FlowValidation validation = new FlowValidation();


    private ConexionRem conexion = null;
    private CreaRem crearRemoto = null;
    private CreaLoc creaLocal = null;
    private GeneralesRem generalesRem = null;
    private GeneralesLoc generalesLocal = null;

    @GetMapping("/create")
    public String create(Model model) {
        logger.info("File flow view");
        model.addAttribute("connectionList", connectionService.findAll());
        model.addAttribute("separatorList", separatorFieldService.findAll());
        model.addAttribute("flow", new Flow());
        return "flow/file-flow";
    }

    @PostMapping("/save")
    public String save(Flow flow, Model model) {

        logger.info("Create archivo flow");
        logger.info(flow.toString());

        flow.setType("archivo");
        ResultResponse result = validation.validateFields(flow);
        if (!result.getResult()) {
            List<String> validations = (List<String>) result.getObject();
            model.addAttribute("messageValidation", "Has errors");
            model.addAttribute("validations", validations);
            return "flow/file-flow";
        }

        if (flowService.existsByName(flow.getName())) {
            logger.info("Ya existe el flujo" + flow.getName());
            model.addAttribute("message", "Ya existe el flujo" + flow.getName());
            return "flow/file-flow";
        }

        logger.info(flow.getStartDate() + " " + flow.getEndDate());

        creaLocal = new CreaLoc();
        generalesLocal = new GeneralesLoc();

        logger.info("mkdir salida ------------------");
        //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
        if (creaLocal.creaDir(utils.get_pathconf() + "/salida/",
                flow.getName(), "-p") != 0) {
            model.addAttribute("message", "Error al crear directorio salida.");
            return "flow/file-flow";
        }

        logger.info("mkdir reg ------------------");
        //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/",
        if (creaLocal.creaDir(utils.get_pathconf() + "/reg/",
                flow.getName(), "-p") != 0) {
            model.addAttribute("message", "Error al crear el directorio reg.");
            return "flow/file-flow";
        }


        String tipoDirectorio = flow.getIsDirectory();

        if ("tabla".equalsIgnoreCase(tipoDirectorio)) {
            HdfsBalam hdfs = new HdfsBalam();
            logger.info("Creando carpeta hdfs ----------------");
            try {
                hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
            } catch (IOException e) {
                logger.error("Creando carpeta hdfs error ----------------");
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al crear la carpeta hdfs." + "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                return "flow/file-flow";
            }

        } else if ("carpeta".equalsIgnoreCase(tipoDirectorio)) {
            logger.info("Creando carpeta hdfs ----------------");
            try {
                hdfs.mkdir(flow.getDirectory());
            } catch (IOException e) {
                logger.error("Creando carpeta hdfs error ----------------");
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al crear la carpeta hdfs: " + flow.getDirectory());
                return "flow/file-flow";
            }
        }

        String alertByMailResult = "si";
        if (flow.getAlertByEmail() == null || flow.getAlertByEmail() == false) {
            alertByMailResult = "no";
            flow.setPercentageToleranceRecords("0");
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
            logger.info("Creando coordinator ------------------");
            try {
                String jobId = oozieClientConfig.CreateCoordinator(flow.getEmails(),
                        "archivo",
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
                        "NA",
                        "NA",
                        "NA",
                        "NA",
                        "NA",
                        flow.getFilename(),
                        "A,P9",
                        flow.getSourceDirectory(),
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
                        "N/A",
                        utils.get_dateformat("ORACLE").replace(" ", "_"),
                        null);

                if (jobId != null) {
                    flow.setJobId(jobId);
                }


            } catch (OozieClientException e) {
                logger.error(e.getMessage());
                //logger.error(e.getCause().toString());
                model.addAttribute("message", e.getMessage());
                return "flow/file-flow";
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                //logger.error(e.getCause().toString());
                model.addAttribute("message", e.getMessage());
                return "flow/file-flow";
            }
        }//fin programar ahora
        Flow persistFlow = flowService.save(flow);

        if (persistFlow.getId() == null) {
            model.addAttribute("message", "No se pudo crear el flujo en base de datos, pero si se ha configurado en Oozie.");
            return "flow/file-flow";
        }

        //return "redirect:/flows/list";
        return "redirect:/monitor/coordinator";
    }

    @PostMapping("/edit")
    public String edit(Flow flow, Model model) {

        logger.info("Create file flow");
        logger.info("receive: " + flow.toString());
        Flow foundFlow = flowService.findOne(flow.getId());
        logger.info("find: " + flow.toString());

        flow.setType("archivo");
        ResultResponse result = validation.validateFields(flow);
        if (!result.getResult()) {
            List<String> validations = (List<String>) result.getObject();
            model.addAttribute("messageValidation", "Has errors");
            model.addAttribute("validations", validations);
            return "flow/edit-file-flow";
        }


        logger.info(flow.getStartDate() + " " + flow.getEndDate());

        creaLocal = new CreaLoc();
        generalesLocal = new GeneralesLoc();

        logger.info("mkdir salida ------------------");
        //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/",
        if (creaLocal.creaDir(utils.get_pathconf() + "/salida/",
                flow.getName(), "-p") != 0) {
            model.addAttribute("message", "Error al crear directorio salida.");
            return "flow/edit-file-flow";
        }

        logger.info("mkdir reg ------------------");
        //if (creaLocal.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/",
        if (creaLocal.creaDir(utils.get_pathconf() + "/reg/",
                flow.getName(), "-p") != 0) {
            model.addAttribute("message", "Error al crear el directorio reg.");
            return "flow/edit-file-flow";
        }


        String tipoDirectorio = flow.getIsDirectory();
        if ("tabla".equalsIgnoreCase(tipoDirectorio)) {
            HdfsBalam hdfs = new HdfsBalam();
            logger.info("Creando carpeta hdfs ----------------");
            try {
                hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
            } catch (IOException e) {
                logger.error("Creando carpeta hdfs error ----------------");
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al crear la carpeta hdfs." + "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/".concat(flow.getName()));
                return "flow/edit-file-flow";
            }
        } else if ("carpeta".equalsIgnoreCase(tipoDirectorio)) {
            HdfsBalam hdfs = new HdfsBalam();
            logger.info("Creando carpeta hdfs ----------------");
            try {
                hdfs.mkdir(flow.getDirectory());
            } catch (IOException e) {
                logger.error("Creando carpeta hdfs error ----------------");
                logger.error(e.getMessage());
                model.addAttribute("message", "Error al crear la carpeta hdfs." + flow.getDirectory());
                return "flow/edit-file-flow";
            }
        }

        String alertByMailResult = "si";
        if (flow.getAlertByEmail() == null || flow.getAlertByEmail() == false) {
            alertByMailResult = "no";
            flow.setPercentageToleranceRecords("0");
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
                    model.addAttribute("messageValidation", "Has errors");
                    model.addAttribute("validations", e.getMessage());
                    logger.error(e.getMessage());
                    return "flow/edit-file-flow";
                }
            }


            logger.info("Creando coordinator ------------------");

            try {
                String jobId = oozieClientConfig.CreateCoordinator(flow.getEmails(),
                        "archivo",
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
                        "NA",
                        "NA",
                        "NA",
                        "NA",
                        "NA",
                        flow.getFilename(),
                        "A,P9",
                        flow.getSourceDirectory(),
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
                       "N/A",
                        utils.get_dateformat("ORACLE").replace(" ", "_"),
                        null);

                if (jobId != null) {
                    flow.setJobId(jobId);
                }
            } catch (OozieClientException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", e.getMessage());
                return "flow/edit-file-flow";
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                model.addAttribute("message", e.getMessage());
                return "flow/edit-file-flow";
            }
        }//fin programar ahora

        Flow persistFlow = flowService.save(flow);

        if (persistFlow.getId() == null) {
            model.addAttribute("message", "No se pudo crear el flujo en base de datos, pero si se ha configurado en Oozie.");
            return "flow/file-flow";
        }

        return "redirect:/monitor/coordinator";
    }

}
