package net.crunchdroid.oozie;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import net.crunchdroid.controller.flow.SqoopFlowController;
import net.crunchdroid.model.Connection;
import net.crunchdroid.pojo.ControlDependencies;
import net.crunchdroid.pojo.DependenciesFilter;
import net.crunchdroid.shell.hdfs.HdfsBalam;
import net.crunchdroid.util.Utils;
import org.apache.oozie.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;

public class OozieClientConfig {
    //CONFIGURACIONES BALAM
    OozieClient wc = new OozieClient("http://10.231.236.5:11000/oozie");
    HdfsBalam hdfs = new HdfsBalam();

    private final Object lock = new Object();
    Logger logger = LoggerFactory.getLogger(SqoopFlowController.class);


  /*  String nameNode = "hdfs://balam";
    String jobTracker = "10.231.236.5:8032";
    String oozie_use_system_libpath = "true";
    String oozie_libpath = "${nameNode}/user/oozie/share/lib";
    String share_lib_ojdbc = "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/ojdbc6.jar";
    String share_lib_sqooop_jar = "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/sqoop-1.4.6.2.6.1.0-129.jar";
    String app_path = "${nameNode}/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM";
    String oozie_wf_application_path = "${app_path}";
    String oozie_coord_application_path = "${app_path}";


    //String shellScriptPath_spool = "${path_spool}/ejecuta_extractor.sh";
    String shellScriptPath_spool = "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/run_remote.sh";
    String argument_spool = "${nombreWorkflow} ${usuario_bd} ${password_bd} ${sid} ${path_spool} ${Narchivo_spool} ${variables} ${separador} ${date_format} ${remoteNode_spool} ${host_ip} ${host_puerto} ${host_tipo_bd}";


    String remoteNode_putHDFS = "appbalam_ingestas@10.231.236.25";
    String shellScriptPath_putHDFS = "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/putHDFS.sh";
    String path_hdfs = "${nameNode}";
    String argument_putHDFS = "${path_salidaFS} ${Narchivo_spool} ${path_hdfs} ${tipo_WF} ${tolerancia} ${nombreWorkflow} ${alerta} ${destino} ${acumulado}";


    String optionFile = "${nombreWorkflow}.txt";
    String option_File_path_in_hdfs = "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/${nombreWorkflow}.txt";
    String path_archivoSpark = "${path_hdfs}/*";


    String fecha_fin = "";
    String separador = "";*/


    public String CreateWorkflow(
            String emails,
            String tipo_WF,
            String nombreWorkflow,
            String destino,
            String alerta,
            String tolerancia,
            String frecuencia,
            String remoteNode_spool,
            String path_spool,
            String usuario_bd,
            String password_bd,
            String sid,
            String Narchivo_spool,
            String variables,
            String path_salidaFS,
            String ruta_path_hdfs,
            String path_jarSpark,
            String queueName,
            String esquema,
            String tabla,
            String particionado,
            String campo_particion,
            String acumulado,
            String sqoop_hdfs,
            String fecha_fin,
            String separador,
            String date_format,
            Connection connection
    ) throws OozieClientException, InterruptedException {


        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) + 6);
        cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 20);
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        format1.setTimeZone(TimeZone.getTimeZone("UTC"));
        String fecha_final = format1.format(cal.getTime());
        String fecha_ini = fecha_final.substring(0, 17);

        Properties conf = wc.createConfiguration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }


        conf.setProperty(OozieClient.APP_PATH, prop.getProperty("app_path"));
        // conf.setProperty(OozieClient.USER_NAME, "appbalam_ingestas");
        conf.setProperty(OozieClient.USER_NAME, prop.getProperty("userServer"));

        conf.setProperty("nameNode", prop.getProperty("nameNode"));
        conf.setProperty("jobTracker", prop.getProperty("jobTracker"));
        conf.setProperty("oozie.use.system.libpath", prop.getProperty("oozie_use_system_libpath"));
        conf.setProperty("oozie.libpath", prop.getProperty("oozie_libpath"));
        conf.setProperty("share_lib_ojdbc", prop.getProperty("share_lib_ojdbc"));
        conf.setProperty("share_lib_sqooop_jar", prop.getProperty("share_lib_sqooop_jar"));


        if (connection == null) {
            conf.setProperty("host_ip", "NA");
            conf.setProperty("host_puerto", "NA");
            conf.setProperty("host_tipo_bd", "NA");
        } else {
            conf.setProperty("host_ip", connection.getHost());
            conf.setProperty("host_puerto", connection.getPort());
            conf.setProperty("host_tipo_bd", connection.getType());
        }

        conf.setProperty("cta_correo", emails);
        conf.setProperty("tipo_WF", tipo_WF);
        conf.setProperty("nombreWorkflow", nombreWorkflow);
        conf.setProperty("destino", destino);
        conf.setProperty("alerta", alerta);
        conf.setProperty("tolerancia", tolerancia);
        conf.setProperty("fecha_ini", fecha_ini);
        conf.setProperty("fecha_fin", fecha_fin);
        conf.setProperty("frecuencia", frecuencia);

        conf.setProperty("remoteNode_spool", remoteNode_spool);
        conf.setProperty("path_spool", path_spool);
        conf.setProperty("usuario_bd", usuario_bd);
        conf.setProperty("password_bd", password_bd);
        conf.setProperty("sid", sid);
        conf.setProperty("Narchivo_spool", Narchivo_spool);
        conf.setProperty("shellScriptPath_spool", prop.getProperty("home").concat(prop.getProperty("shellScriptPath_spool")));
        conf.setProperty("variables", variables);
        conf.setProperty("separador", separador);
        conf.setProperty("argument_spool", prop.getProperty("argument_spool"));

        conf.setProperty("remoteNode_putHDFS", prop.getProperty("remoteNode_putHDFS"));
        conf.setProperty("shellScriptPath_putHDFS", prop.getProperty("home").concat(prop.getProperty("shellScriptPath_putHDFS")));
        conf.setProperty("path_salidaFS", path_salidaFS);
        conf.setProperty("path_hdfs", prop.getProperty("path_hdfs").concat(ruta_path_hdfs));
        conf.setProperty("argument_putHDFS", prop.getProperty("argument_putHDFS"));

        conf.setProperty("path_jarSpark", path_jarSpark);
        conf.setProperty("queueName", queueName);
        conf.setProperty("path_archivoSpark", prop.getProperty("path_archivoSpark"));
        conf.setProperty("esquema", esquema);
        conf.setProperty("tabla", tabla);
        conf.setProperty("particionado", particionado);
        conf.setProperty("campo_particion", campo_particion);
        conf.setProperty("acumulado", acumulado);

        conf.setProperty("optionFile", prop.getProperty("optionFile"));
        conf.setProperty("option_File_path_in_hdfs", prop.getProperty("option_File_path_in_hdfs"));
        conf.setProperty("sqoop_hdfs", sqoop_hdfs);
        conf.setProperty("date_format", date_format);

        String jobId = wc.run(conf);
        System.out.println("Flujo de trabajo enviado...");

        // imprimir el estado final o el trabajo de flujo de trabajo
        System.out.println("Flujo de trabajo completado...");
        return jobId;
    }


    public String CreateWorkflow2(
            String emails,
            String tipo_WF,
            String nombreWorkflow,
            String destino,
            String alerta,
            String tolerancia,
            String frecuencia,
            String remoteNode_spool,
            String path_spool,
            String usuario_bd,
            String password_bd,
            String sid,
            String Narchivo_spool,
            String variables,
            String path_salidaFS,
            String ruta_path_hdfs,
            String path_jarSpark,
            String queueName,
            String esquema,
            String tabla,
            String particionado,
            String campo_particion,
            String acumulado,
            String sqoop_hdfs,
            String fecha_fin,
            String separador,
            String date_format,
            Connection connection
    ) throws OozieClientException, InterruptedException {


        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) + 6);
        cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 20);
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        format1.setTimeZone(TimeZone.getTimeZone("UTC"));
        String fecha_final = format1.format(cal.getTime());
        String fecha_ini = fecha_final.substring(0, 17);

        Properties conf = wc.createConfiguration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        conf.setProperty(OozieClient.APP_PATH, prop.getProperty("app_path"));
        conf.setProperty(OozieClient.USER_NAME, prop.getProperty("userServer"));

        conf.setProperty("nameNode", prop.getProperty("nameNode"));
        conf.setProperty("jobTracker", prop.getProperty("jobTracker"));
        conf.setProperty("oozie.use.system.libpath", prop.getProperty("oozie_use_system_libpath"));
        conf.setProperty("oozie.libpath", prop.getProperty("oozie_libpath"));
        conf.setProperty("share_lib_ojdbc", prop.getProperty("share_lib_ojdbc"));
        conf.setProperty("share_lib_sqooop_jar", prop.getProperty("share_lib_sqooop_jar"));


        if (connection == null) {
            conf.setProperty("host_ip", "NA");
            conf.setProperty("host_puerto", "NA");
            conf.setProperty("host_tipo_bd", "NA");
        } else {
            conf.setProperty("host_ip", connection.getHost());
            conf.setProperty("host_puerto", connection.getPort());
            conf.setProperty("host_tipo_bd", connection.getType());
        }


        conf.setProperty("cta_correo", emails);
        conf.setProperty("tipo_WF", tipo_WF);
        conf.setProperty("nombreWorkflow", nombreWorkflow);
        conf.setProperty("destino", destino);
        conf.setProperty("alerta", alerta);
        conf.setProperty("tolerancia", tolerancia);
        conf.setProperty("fecha_ini", fecha_ini);
        conf.setProperty("fecha_fin", fecha_fin);
        conf.setProperty("frecuencia", frecuencia);

        conf.setProperty("remoteNode_spool", remoteNode_spool);
        conf.setProperty("path_spool", path_spool);
        conf.setProperty("usuario_bd", usuario_bd);
        conf.setProperty("password_bd", password_bd);
        conf.setProperty("sid", sid);
        conf.setProperty("Narchivo_spool", Narchivo_spool);
        conf.setProperty("shellScriptPath_spool", prop.getProperty("home").concat(prop.getProperty("shellScriptPath_spool")));
        conf.setProperty("variables", variables);
        conf.setProperty("separador", separador);
        conf.setProperty("argument_spool", prop.getProperty("argument_spool"));

        conf.setProperty("remoteNode_putHDFS", prop.getProperty("remoteNode_putHDFS"));
        conf.setProperty("shellScriptPath_putHDFS", prop.getProperty("home") + prop.getProperty("shellScriptPath_putHDFS"));
        conf.setProperty("path_salidaFS", path_salidaFS);
        conf.setProperty("path_hdfs", prop.getProperty("path_hdfs").concat(ruta_path_hdfs));
        conf.setProperty("argument_putHDFS", prop.getProperty("argument_putHDFS"));

        conf.setProperty("path_jarSpark", path_jarSpark);
        conf.setProperty("queueName", queueName);
        conf.setProperty("path_archivoSpark", prop.getProperty("path_archivoSpark"));
        conf.setProperty("esquema", esquema);
        conf.setProperty("tabla", tabla);
        conf.setProperty("particionado", particionado);
        conf.setProperty("campo_particion", campo_particion);
        conf.setProperty("acumulado", acumulado);

        conf.setProperty("optionFile", prop.getProperty("optionFile"));
        conf.setProperty("option_File_path_in_hdfs", prop.getProperty("option_File_path_in_hdfs"));
        conf.setProperty("sqoop_hdfs", sqoop_hdfs);
        conf.setProperty("date_format", date_format);

        String jobId = wc.run(conf);
        System.out.println("Flujo de trabajo enviado...");

        // imprimir el estado final o el trabajo de flujo de trabajo
        System.out.println("Flujo de trabajo completado...");
        return jobId;
    }


    public String CreateCoordinator(
            String cta_correo,
            String tipo_WF,
            String nombreWorkflow,
            String destino,
            String alerta,
            String tolerancia,
            String frecuencia,
            String remoteNode_spool,
            String path_spool,
            String usuario_bd,
            String password_bd,
            String sid,
            String Narchivo_spool,
            String variables,
            String path_salidaFS,
            String ruta_path_hdfs,
            String path_jarSpark,
            String queueName,
            String esquema,
            String tabla,
            String particionado,
            String campo_particion,
            String acumulado,
            String sqoop_hdfs,
            String fecha_fin,
            String fecha_ini,
            String separador,
            String date_format,
            Connection connection
    ) throws OozieClientException, InterruptedException {

        Date hora = new Date();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sfhora = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat sfdate_utc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        sfdate_utc.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sfdate_local = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String f_hora = fecha_ini + " " + sfhora.format(hora);
        Date dflocal = null;
        try {
            dflocal = sfdate_local.parse(f_hora);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cal.setTime(dflocal);
        cal.add(Calendar.MINUTE, 1);
        String p_fecha_ini = sfdate_utc.format(cal.getTime());


        String f_fin = fecha_fin + " 23:00:00";
        hora = new Date();
        cal = Calendar.getInstance();
        try {
            dflocal = sfdate_local.parse(f_fin);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cal.setTime(dflocal);
        String p_fecha_fin = sfdate_utc.format(cal.getTime());

        Properties conf = wc.createConfiguration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, prop.getProperty("oozie_coord_application_path"));
        conf.setProperty(OozieClient.USER_NAME, prop.getProperty("userServer"));

        conf.setProperty("nameNode", prop.getProperty("nameNode"));
        conf.setProperty("jobTracker", prop.getProperty("jobTracker"));
        conf.setProperty("oozie.use.system.libpath", prop.getProperty("oozie_use_system_libpath"));
        conf.setProperty("oozie.libpath", prop.getProperty("oozie_libpath"));
        conf.setProperty("share_lib_ojdbc", prop.getProperty("share_lib_ojdbc"));
        conf.setProperty("share_lib_sqooop_jar", prop.getProperty("share_lib_sqooop_jar"));
        conf.setProperty("app_path", prop.getProperty("app_path"));

        if (connection == null) {
            conf.setProperty("host_ip", "NA");
            conf.setProperty("host_puerto", "NA");
            conf.setProperty("host_tipo_bd", "NA");
        } else {
            conf.setProperty("host_ip", connection.getHost());
            conf.setProperty("host_puerto", connection.getPort());
            conf.setProperty("host_tipo_bd", connection.getType());
        }

        conf.setProperty("cta_correo", cta_correo);
        conf.setProperty("tipo_WF", tipo_WF);
        conf.setProperty("nombreWorkflow", nombreWorkflow);
        conf.setProperty("destino", destino);
        conf.setProperty("alerta", alerta);
        conf.setProperty("tolerancia", tolerancia);
        conf.setProperty("fecha_ini", p_fecha_ini);
        logger.info("Fecha inicial: " + p_fecha_ini);
        conf.setProperty("fecha_fin", p_fecha_fin);
        logger.info("Fecha final: " + p_fecha_fin);
        conf.setProperty("frecuencia", frecuencia);

        conf.setProperty("remoteNode_spool", remoteNode_spool);
        conf.setProperty("path_spool", path_spool);
        conf.setProperty("usuario_bd", usuario_bd);
        conf.setProperty("password_bd", password_bd);
        conf.setProperty("sid", sid);
        conf.setProperty("Narchivo_spool", Narchivo_spool);
        conf.setProperty("shellScriptPath_spool", prop.getProperty("home").concat(prop.getProperty("shellScriptPath_spool")));
        conf.setProperty("variables", variables);
        conf.setProperty("separador", separador);
        conf.setProperty("argument_spool", prop.getProperty("argument_spool"));

        conf.setProperty("remoteNode_putHDFS", prop.getProperty("remoteNode_putHDFS"));
        conf.setProperty("shellScriptPath_putHDFS", prop.getProperty("home") + prop.getProperty("shellScriptPath_putHDFS"));
        conf.setProperty("path_salidaFS", path_salidaFS);
        conf.setProperty("path_hdfs", prop.getProperty("path_hdfs").concat(ruta_path_hdfs));
        conf.setProperty("argument_putHDFS", prop.getProperty("argument_putHDFS"));

        conf.setProperty("path_jarSpark", path_jarSpark);
        conf.setProperty("queueName", queueName);
        conf.setProperty("path_archivoSpark", prop.getProperty("path_archivoSpark"));
        conf.setProperty("esquema", esquema);
        conf.setProperty("tabla", tabla);
        conf.setProperty("particionado", particionado);
        conf.setProperty("campo_particion", campo_particion);
        conf.setProperty("acumulado", acumulado);

        conf.setProperty("optionFile", prop.getProperty("optionFile"));
        conf.setProperty("option_File_path_in_hdfs", prop.getProperty("option_File_path_in_hdfs"));
        conf.setProperty("sqoop_hdfs", sqoop_hdfs);
        conf.setProperty("date_format", date_format);

        String jobId = wc.run(conf);
        System.out.println("Flujo de trabajo enviado...");

        // imprimir el estado final o el trabajo de flujo de trabajo
        System.out.println("Flujo de trabajo completado...");
        System.out.println(wc.getJobInfo(jobId));

        return jobId;

    }


    public Boolean flowValidation(String jobId) throws OozieClientException {
        boolean status = true;

        if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("Actualmente hay un Flujo de Trabajo en ejecucución..");
            status = true;
            return status;
        } else if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.KILLED) {
            System.out.println("El fujo de Trabajo ha sido finalizado");
            status = false;
            return status;
        } else if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUSPENDED) {
            System.out.println("Flujo de Trabajo Suspendido...");
            status = false;
            return status;
        }
        return status;

    }

    public boolean getCoordJobInfo(String jobId) throws OozieClientException {

        boolean status = false;
        int count = 0;
        CoordinatorJob coordJobInfo = wc.getCoordJobInfo(jobId);

        for (CoordinatorAction action : coordJobInfo.getActions()) {
            count++;

            if (count > coordJobInfo.getActions().size() - 1) {
                System.out.println(action.getId() + (action.getStatus()));

                if (coordJobInfo.getStatus() == Job.Status.RUNNING) {
                    System.out.println("Flujo de Trabajo corriendo...");
                    status = true;
                    return status;
                } else if (coordJobInfo.getStatus() == Job.Status.KILLED) {
                    System.out.println("Flujo de Trabajo finalizado...");
                    status = false;
                    return status;
                } else if (coordJobInfo.getStatus() == Job.Status.SUSPENDED) {
                    System.out.println("flujo de Trabajo suspendido...");
                    status = false;
                    return status;
                }
            }
        }
        return status;
    }

    public boolean getListCoordJobInfo(String jobId) throws OozieClientException {

        boolean status = false;
        int count = 0;

        CoordinatorJob coordJobInfo = wc.getCoordJobInfo(jobId);

        for (CoordinatorAction action : coordJobInfo.getActions()) {
            count++;

            if (count > coordJobInfo.getActions().size() - 1) {
                System.out.println(action.getId() + (action.getStatus()));

                if (coordJobInfo.getStatus() == Job.Status.RUNNING) {
                    System.out.println("Flujo de Trabajo corriendo...");
                    status = true;
                    return status;
                } else if (coordJobInfo.getStatus() == Job.Status.KILLED) {
                    System.out.println("Flujo de Trabajo finalizado...");
                    status = false;
                    return status;
                } else if (coordJobInfo.getStatus() == Job.Status.SUSPENDED) {
                    System.out.println("flujo de Trabajo suspendido...");
                    status = false;
                    return status;
                }
            }
        }
        return status;
    }

    //Devuelve una Lista de workflows donde el status=(RUNNING) (Opcional)
    public List<WorkflowJob> getJobsInfo(String filter, int start, int len)
            throws OozieClientException {
        synchronized (lock) {

            return wc.getJobsInfo(filter, start, len);
        }
    }

    //Devuelve una Lista de workflows donde el status=(RUNNING)
    public List<WorkflowJob> getJobsInfo(String filter) throws OozieClientException {
        synchronized (lock) {
            int count = 0;

            List<WorkflowJob> coordJobInfo = wc.getJobsInfo(filter);

            for (WorkflowJob action : coordJobInfo) {
                count++;

                if (count < coordJobInfo.size()) {
                    System.out.println(action.getId() + (action.getStatus()));
                    System.out.println(action.getId() + " "
                            + action.getStatus()
                            + " " + action.getAcl()
                            + " " + action.getAppName()
                            + " " + action.getAppPath()
                            + " " + action.getUser()
                            + " " + action.getConsoleUrl()
                            + " " + action.getExternalId()
                            + " " + action.getParentId()
                    );

                }
            }
            return coordJobInfo;
        }
    }

    public List<ControlDependencies> getCoordJobsInfo(String filter, int start, int len)
            throws OozieClientException {

        boolean flag = false;

        List<CoordinatorJob> coordJobInfo;
        ArrayList<ControlDependencies> dependencias = new ArrayList<>();

        synchronized (lock) {
            int count = 0;

            coordJobInfo = wc.getCoordJobsInfo(filter, start, len);

            for (CoordinatorJob action : coordJobInfo) {


                java.sql.Connection conn = null;
                try {

                    Properties prop = new Properties();
                    InputStream is = null;

                    try {
                        is = new FileInputStream("path.properties");
                        prop.load(is);
                    } catch (IOException e) {
                        System.out.println(e.toString());
                    }


                    conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));

                    // Do something with the Connection
                    java.sql.PreparedStatement stmt = null;
                    ResultSet rs = null;
                    String query = "select *from wf_flujos_dependencias a " +
                            "where a.coordinator=?  limit 1;";
                    stmt = conn.prepareStatement(query);
                    stmt.setString(1, action.getAppName());
                    rs = stmt.executeQuery();

                    while (rs.next()) {
                        count++;
                    }
                    if (count > 0) {
                        flag = true;
                    } else {
                        flag = false;
                    }
                } catch (SQLException ex) {
                    // handle any errors
                    ex.printStackTrace();

                } finally {
                    try {

                        conn.close();

                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                }
                dependencias.add(new ControlDependencies(action, flag));
                count = 0;
            }

        }
        return dependencias;
    }

    public void reRun(String jobId, Properties conf) throws OozieClientException {
        synchronized (lock) {
            wc.reRun(jobId, conf);
        }
    }

    public void suspend(String jobId) throws OozieClientException {
        synchronized (lock) {
            wc.suspend(jobId);
        }
    }

    public void kill(String jobId) throws OozieClientException {
        synchronized (lock) {
            wc.kill(jobId);
        }
    }

    public void resume(String jobId) throws OozieClientException {
        synchronized (lock) {
            wc.resume(jobId);
        }
    }

    public boolean getStatusWF(String WFname) throws OozieClientException {
        boolean status = false;
        List<WorkflowJob> workflowList = null;
        workflowList = wc.getJobsInfo("Status=RUNNING;Status=PREP;");

        for (WorkflowJob workflowJob : workflowList) {

            if (workflowJob.getAppName().equalsIgnoreCase(WFname)) {
                status = true;
                break;
            }
        }
        return status;
    }

    public boolean getCoordInfo(String jobId) throws OozieClientException {
        boolean status = false;

        CoordinatorJob coordJobInfo = wc.getCoordJobInfo(jobId);

        if (coordJobInfo.getStatus() == Job.Status.RUNNING) {
            return true;
        }
        if (coordJobInfo.getStatus() == Job.Status.SUSPENDED) {
            return false;
        }
        return status;
    }

    public boolean isCoordinatorRunning(String jobId) throws OozieClientException {

        boolean isRunning = false;
        int count = 0;
        CoordinatorJob coordJobInfo = wc.getCoordJobInfo(jobId);

        for (CoordinatorAction action : coordJobInfo.getActions()) {
            count++;

            if (count > coordJobInfo.getActions().size() - 1) {
                System.out.println(action.getId() + (action.getStatus()));

                if (action.getStatus().toString() == Job.Status.RUNNING.toString()) {
                    System.out.println("Flujo de Trabajo corriendo...");
                    isRunning = true;
                } else if (action.getStatus().toString() == Job.Status.KILLED.toString()) {
                    System.out.println("Flujo de Trabajo finalizado...");
                    isRunning = false;
                } else if (action.getStatus().toString() == Job.Status.SUSPENDED.toString()) {
                    System.out.println("flujo de Trabajo suspendido...");
                    isRunning = false;
                }
            }
        }
        return isRunning;
    }

    public String CreateBundle(String nombreWFDep, String fecha_ini, String fecha_fin, String p_minutos, String p_hora, String p_dsemana, String p_mes, String p_dmes) throws OozieClientException, InterruptedException {
        String jobId = "";


        Properties conf = wc.createConfiguration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        ////configuraciones generales
        conf.setProperty(OozieClient.USER_NAME, prop.getProperty("userServer"));
        conf.setProperty("nombreWFDep", nombreWFDep);
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("oozie.libpath", "hdfs://balam/user/oozie/share/lib");


        ////////////////////////////////////////////////////////////////////
        Date hora = new Date();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sfhora = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat sfdate_utc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        sfdate_utc.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sfdate_local = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String f_hora = fecha_ini + " " + sfhora.format(hora);
        Date dflocal = null;
        try {
            dflocal = sfdate_local.parse(f_hora);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cal.setTime(dflocal);
        cal.add(Calendar.MINUTE, 1);
        String p_fecha_ini = sfdate_utc.format(cal.getTime());

        String f_fin = fecha_fin + " 23:00:00";
        //hora = new Date();
        cal = Calendar.getInstance();
        try {
            dflocal = sfdate_local.parse(f_fin);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cal.setTime(dflocal);
        String p_fecha_fin = sfdate_utc.format(cal.getTime());
        /////////////////////////////////////////////////////////////
        Utils util = new Utils();
        conf.setProperty("fecha_ini", p_fecha_ini);
        conf.setProperty("fecha_fin", p_fecha_fin);
        conf.setProperty("frecuencia", p_minutos + " " + util.parseCronhour(p_hora) + " " + p_dsemana + " " + p_mes + " " + p_dmes);
        conf.setProperty("fecha_ini2", p_fecha_ini);
        conf.setProperty("fecha_fin2", p_fecha_fin);
        /*int i_min = Integer.parseInt(p_minutos);
        if (i_min >= 0 && i_min <= 58)
            i_min += 1;
        else
            i_min = 0;*/
        //conf.setProperty("frecuencia2", i_min + " " + util.parseCronhour(p_hora) + " " + p_dsemana + " " + p_mes + " " + p_dmes);
        conf.setProperty("frecuencia2", parseMinutes(p_minutos) + " " + util.parseCronhour(p_hora) + " " + p_dsemana + " " + p_mes + " " + p_dmes);


        conf.setProperty(OozieClient.BUNDLE_APP_PATH, "hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}");

        java.sql.Connection conn = null;
        try {


            conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));

            // Do something with the Connection
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select a.*,b.*,c.db_user,c.host,c.password,c.port,c.unix_path,c.unix_user,c.sid,c.type tipo_bd " +
                    "from wf_flujos_dependencias a " +
                    "join flow b on a.coordinator=b.name " +
                    "left outer join connection c on (c.id=b.connection_id) " +
                    "where a.nombre=?  order by posicion;";
            stmt = (PreparedStatement) conn.prepareStatement(query);
            stmt.setString(1, nombreWFDep);
            rs = stmt.executeQuery();


            //int indice = 1;
            while (rs.next()) {
                int indice = Integer.parseInt(rs.getString("posicion"));
                int col = rs.getInt("col");
                //////
                conf.setProperty("nombreWorkflow" + indice + col, rs.getString("name"));
                conf.setProperty("nameNode" + indice + col, "hdfs://balam");
                //conf.setProperty("jobTracker" + indice + col, "10.231.236.5:8032");
                conf.setProperty("jobTracker" + indice + col, prop.getProperty("jobTracker"));
                conf.setProperty("app_path" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}");
                conf.setProperty("cta_correo" + indice + col, rs.getString("emails"));
                conf.setProperty("tipo_WF" + indice + col, rs.getString("type"));
                conf.setProperty("destino" + indice + col, rs.getString("is_directory"));
                if (rs.getString("alert_by_email").equals("1"))
                    conf.setProperty("alerta" + indice + col, "si");
                else
                    conf.setProperty("alerta" + indice + col, "no");
                conf.setProperty("tolerancia" + indice + col, rs.getString("percentage_tolerance_records"));
                if (rs.getString("separator_") == null || rs.getString("separator_").equals(""))
                    conf.setProperty("separador" + indice + col, "N/A");
                else {
                    if (rs.getString("separator_").equals(","))
                        conf.setProperty("separador" + indice + col, rs.getString("separator_"));
                    else
                        conf.setProperty("separador" + indice + col, "\\" + rs.getString("separator_"));
                }
                conf.setProperty("date_format" + indice + col, util.get_dateformat("oracle").replace(" ", "_"));

                if (rs.getString("unix_user") == null || rs.getString("unix_user").equals(""))
                    conf.setProperty("remoteNode_spool" + indice + col, "N/A");
                else
                    conf.setProperty("remoteNode_spool" + indice + col, rs.getString("unix_user") + "@" + rs.getString("host"));
                conf.setProperty("path_spool" + indice + col, rs.getString("unix_path") + "/BALAM_INGESTAS");
                if (rs.getString("db_user") == null || rs.getString("db_user").equals(""))
                    conf.setProperty("usuario_bd" + indice + col, "N/A");
                else
                    conf.setProperty("usuario_bd" + indice + col, rs.getString("db_user"));
                if (rs.getString("password") == null || rs.getString("password").equals(""))
                    conf.setProperty("password_bd" + indice + col, "NA");
                else
                    conf.setProperty("password_bd" + indice + col, rs.getString("password"));
                if (rs.getString("sid") == null || rs.getString("sid").equals(""))
                    conf.setProperty("sid" + indice + col, "NA");
                else
                    conf.setProperty("sid" + indice + col, rs.getString("sid"));
                conf.setProperty("Narchivo_spool" + indice + col, rs.getString("filename"));

                /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                //  conf.setProperty("shellScriptPath_spool" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh");
                conf.setProperty("shellScriptPath_spool" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh"));

                if (rs.getString("variables") == null || rs.getString("variables").equals(""))
                    conf.setProperty("variables" + indice + col, "NA");
                else
                    conf.setProperty("variables" + indice + col, rs.getString("variables"));
                if (rs.getString("host") == null || rs.getString("host").equals(""))
                    conf.setProperty("host_ip" + indice + col, "N/A");
                else
                    conf.setProperty("host_ip" + indice + col, rs.getString("host"));
                if (rs.getString("port") == null || rs.getString("port").equals(""))
                    conf.setProperty("host_puerto" + indice + col, "N/A");
                else
                    conf.setProperty("host_puerto" + indice + col, rs.getString("port"));
                if (rs.getString("tipo_bd") == null || rs.getString("tipo_bd").equals(""))
                    conf.setProperty("host_tipo_bd" + indice + col, "NA");
                else
                    conf.setProperty("host_tipo_bd" + indice + col, rs.getString("tipo_bd"));
                if (rs.getString("posicion").equals("1"))
                    conf.setProperty("flag_first" + indice + col, "SI");
                else
                    conf.setProperty("flag_first" + indice + col, "NO");
                conf.setProperty("clean_sqoop" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("clean_file" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("argument_spool" + indice + col, "${nombreWorkflow" + indice + col + "} ${usuario_bd" + indice + col + "} ${password_bd" + indice + col + "} ${sid" + indice + col + "} ${path_spool" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${variables" + indice + col + "} ${separador" + indice + col + "} ${date_format" + indice + col + "} ${remoteNode_spool" + indice + col + "} ${host_ip" + indice + col + "} ${host_puerto" + indice + col + "} ${host_tipo_bd" + indice + col + "} ${flag_first" + indice + col + "} ${nombreWFDep}");

                //////////////////////////////////////////////////////////
                // conf.setProperty("remoteNode_putHDFS" + indice + col, "appbalam_ingestas@10.231.236.25");
                conf.setProperty("remoteNode_putHDFS" + indice + col, prop.getProperty("remoteNode_putHDFS"));
                // conf.setProperty("shellScriptPath_putHDFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/putHDFS.sh");
                conf.setProperty("shellScriptPath_putHDFS" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/putHDFS.sh"));

                if (rs.getString("type").equals("archivo"))
                    conf.setProperty("path_salidaFS" + indice + col, rs.getString("source_directory"));
                else
                    conf.setProperty("path_salidaFS" + indice + col, util.get_pathconf() + "/salida/");
                //conf.setProperty("path_salidaFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/"); ya estaba Comentado antes de hacer cambio agegando el properties

                if (rs.getString("is_directory").equals("tabla"))
                    conf.setProperty("path_hdfs" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow" + indice + col + "}");
                else if (rs.getString("is_directory").equals("carpeta"))
                    conf.setProperty("path_hdfs" + indice + col, rs.getString("directory"));
                conf.setProperty("argument_putHDFS" + indice + col, "${path_salidaFS" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${path_hdfs" + indice + col + "} ${tipo_WF" + indice + col + "} ${tolerancia" + indice + col + "} ${nombreWorkflow" + indice + col + "} ${alerta" + indice + col + "} ${destino" + indice + col + "} ${acumulado" + indice + col + "}");
                conf.setProperty("path_jarSpark" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}/lib");
                if ((rs.getString("queue") == null || rs.getString("queue").equals("")) && (rs.getString("queue_sqoop") == null || rs.getString("queue_sqoop").equals("")))
                    conf.setProperty("queueName" + indice + col, "NA");
                else if (rs.getString("queue") != null && !rs.getString("queue").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue"));
                else if (rs.getString("queue_sqoop") != null && !rs.getString("queue_sqoop").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue_sqoop"));
                conf.setProperty("path_archivoSpark" + indice + col, "${path_hdfs" + indice + col + "}/*");
                if (rs.getString("schema_database") == null || rs.getString("schema_database").equals(""))
                    conf.setProperty("esquema" + indice + col, "N/A");
                else
                    conf.setProperty("esquema" + indice + col, rs.getString("schema_database"));
                if (rs.getString("tablename") == null || rs.getString("tablename").equals(""))
                    conf.setProperty("tabla" + indice + col, "N/A");
                else
                    conf.setProperty("tabla" + indice + col, rs.getString("tablename"));
                if (rs.getString("particioned").equals("1"))
                    conf.setProperty("particionado" + indice + col, "SI");
                else
                    conf.setProperty("particionado" + indice + col, "NO");
                if (rs.getString("particioned_field") == null || rs.getString("particioned_field").equals(""))
                    conf.setProperty("campo_particion" + indice + col, "N/A");
                else
                    conf.setProperty("campo_particion" + indice + col, rs.getString("particioned_field"));
                if (rs.getString("override").equals("1"))
                    conf.setProperty("acumulado" + indice + col, "NO");
                else
                    conf.setProperty("acumulado" + indice + col, "SI");
                conf.setProperty("optionFile" + indice + col, "${nombreWorkflow" + indice + col + "}.txt");
                conf.setProperty("option_File_path_in_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/${nombreWorkflow" + indice + col + "}.txt");
                conf.setProperty("sqoop_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/${nombreWorkflow" + indice + col + "}");
                if (indice == rs.getInt("max"))
                    conf.setProperty("flag_last" + indice + col, "SI");
                else
                    conf.setProperty("flag_last" + indice + col, "NO");
                conf.setProperty("argument_trigger" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_last" + indice + col + "}");
                if (rs.getString("dependencia") != null) {
                    String[] tmp = rs.getString("dependencia").split("\\|");
                    for (int i = 0; i <= tmp.length - 1; i++) {
                        conf.setProperty("triggerFileDir" + indice + col + i, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/triggers/${nombreWFDep}/" + tmp[i]);
                    }
                }
                conf.setProperty("share_lib_ojdbc" + indice + col, "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/ojdbc6.jar");
                conf.setProperty("share_lib_sqooop_jar" + indice + col, "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/sqoop-1.4.6.2.6.1.0-129.jar");


                if (rs.getString("is_directory").equals("tabla")) {
                    try {
                        hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombreWFDep + "/" + rs.getString("name") + "/lib");
                        // hdfs.copyFromLocal("/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/Spark_oozie-assembly-1.0.jar", "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombreWFDep + "/" + rs.getString("name") + "/lib");
                        hdfs.copyFromLocal(prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/Spark_oozie-assembly-1.0.jar"), "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombreWFDep + "/" + rs.getString("name") + "/lib");


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                //indice++;
            }
            stmt.close();
            conn.close();

        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();

        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        jobId = wc.run(conf);

        conn = null;

        try {
            conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));

            //System.out.println(p_nombre+"-"+p_coordinator+"-"+p_posicion+"-"+p_dependencia+"-"+p_max+"-"+p_col);
            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "insert into wf_bundles values(?,?,?,?,?,?,?,?,?,?,?,?)";


            stmt = conn.prepareStatement(query);
            stmt.setString(1, nombreWFDep);
            stmt.setString(2, fecha_ini);
            stmt.setString(3, (fecha_fin));
            stmt.setString(4, "");
            stmt.setString(5, jobId);
            stmt.setString(6, p_dmes);
            stmt.setString(7, p_dsemana);
            stmt.setString(8, p_hora);
            stmt.setString(9, p_minutos);
            stmt.setString(10, p_mes);
            stmt.setString(11, "");
            stmt.setString(12, "");
            stmt.execute();

            stmt.close();
            conn.close();
        }
        // Do something with the Connection

        catch (
                SQLException ex)

        {
            // handle any errors
            ex.printStackTrace();

        }

        System.out.println("Flujo de trabajo enviado...");
        System.out.println(wc.getJobInfo(jobId));

        return jobId;
    }

    public List<DependenciesFilter> filter_Dependences(String p_finicio, String p_ffin, String p_nombre, String p_extractor, String p_pais) {
        ArrayList<DependenciesFilter> monitor_filterDep = new ArrayList<>();


        boolean flag = false;
        java.sql.Connection conn = null;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        Date ffin = null, fini = null;
        String sfini = null, sffin = null;
        try {

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));

            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;

            String query = "select distinct a.nombre,a.fecha_inicio,a.fecha_fin, a.minuto,a.hora, a.dia_semana,a.mes,a.dia_mes,a.bundle_id " +
                    "from wf_bundles a " +
                    "join wf_flujos_dependencias b on (a.nombre=b.nombre) " +
                    "join flow c on (b.coordinator=c.name) " +
                    "where a.fecha_inicio BETWEEN ? and ? " +
                    "and a.nombre like ? " +
                    "and b.coordinator like ? " +
                    "and c.country like ?; ";

            stmt = conn.prepareStatement(query);
            stmt.setString(1, p_finicio);
            stmt.setString(2, p_ffin);
            stmt.setString(3, "%" + p_nombre.replace("*", "") + "%");
            stmt.setString(4, "%" + p_extractor.replace("*", "") + "%");
            stmt.setString(5, "%" + p_pais.replace("*", "") + "%");
            rs = stmt.executeQuery();

            while (rs.next()) {

                String status = "PREP";
                int running = 0, waiting = 0, succeeded = 0, failed = 0, coord = 0;
                try {

                    String bundle_id = rs.getString("bundle_id");

                    BundleJob bundleJob = wc.getBundleJobInfo(bundle_id);
                    for (CoordinatorJob job : bundleJob.getCoordinators()) {
                        coord++;
                        CoordinatorJob jobInfo = wc.getCoordJobInfo(job.getId());
                        // for (CoordinatorAction action : jobInfo.getActions()) {

                        List<CoordinatorAction> action = jobInfo.getActions();
                        if (action.size() > 0)
                            if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.RUNNING)
                                running++;
                            else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.KILLED)
                                failed++;
                            else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.FAILED)
                                failed++;
                            else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.WAITING)
                                waiting++;
                            else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.SUCCEEDED)
                                succeeded++;
                            else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.SUSPENDED)
                                failed++;
                            else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.TIMEDOUT)
                                failed++;

                        //}


                    }
                    if (running > 0)
                        status = "RUNNING";
                    else if (failed > 0)
                        status = "FAILED";
                    else if (waiting > 0 && waiting == coord)
                        status = "WAITING";
                    else if (succeeded > 0 && succeeded == coord)
                        status = "SUCCEEDED";
                    else
                        status = "WAITING";


                } catch (OozieClientException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                //////////////////////////////****************************
                String query2 = "select coordinator from wf_flujos_dependencias where nombre=? and posicion=1 ";
                PreparedStatement stmt2 = conn.prepareStatement(query2);
                stmt2.setString(1, rs.getString("nombre"));
                ResultSet rs2 = stmt2.executeQuery();
                rs2.next();
                List<BundleJob> BJ = wc.getBundleJobsInfo("Name=" + rs.getString("nombre") + ";Status=RUNNING", 1, 1);

                //if (BJ != null) {
                if (BJ.size()>0) {
                    BundleJob BJa = wc.getBundleJobInfo(BJ.get(0).getId());
                    for (CoordinatorJob ca : BJa.getCoordinators()) {
                        if (ca.getAppName().trim().toUpperCase().equals(rs2.getString("coordinator").toUpperCase())) {
                            CoordinatorJob coord2 = wc.getCoordJobInfo(ca.getId());
                            if (coord2.getActions().size() > 0) {
                                if (coord2.getActions().get(coord2.getActions().size() - 1).getExternalId() != null) {
                                    WorkflowJob work = wc.getJobInfo(coord2.getActions().get(coord2.getActions().size() - 1).getExternalId());
                                    fini = work.getStartTime();
                                    sfini = df.format(dateOozie.parse(fini.toString()));
                                }
                            } else {
                                sfini = null;
                            }
                            break;
                        }
                    }


                    List<WorkflowJob> WF = wc.getJobsInfo("Name=" + rs2.getString("coordinator"), 1, 1);
                    /////////////////////
                    String query3 = "select coordinator from wf_flujos_dependencias where nombre=? and posicion=max";
                    PreparedStatement stmt3 = conn.prepareStatement(query3);
                    stmt3.setString(1, rs.getString("nombre"));
                    ResultSet rs3 = stmt3.executeQuery();
                    while (rs3.next()) {
                        for (CoordinatorJob ca : BJa.getCoordinators()) {
                            if (ca.getAppName().trim().toUpperCase().equals(rs3.getString("coordinator").toUpperCase())) {
                                CoordinatorJob coord2 = wc.getCoordJobInfo(ca.getId());
                                if (coord2.getActions().size() > 0) {
                                    if (coord2.getActions().get(coord2.getActions().size() - 1).getExternalId() != null) {
                                        WorkflowJob work = wc.getJobInfo(coord2.getActions().get(coord2.getActions().size() - 1).getExternalId());
                                        if (work.getEndTime() != null) {
                                            if (ffin == null) {
                                                ffin = work.getEndTime();
                                                sffin = df.format(dateOozie.parse(ffin.toString()));
                                            } else {
                                                if (ffin.before(work.getEndTime())) {
                                                    ffin = work.getEndTime();
                                                    sffin = df.format(dateOozie.parse(ffin.toString()));
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    sffin = null;
                                }
                                break;
                            }
                        }

                    }

                    /////////////////////////////////////////////
                    monitor_filterDep.add(new DependenciesFilter(sfini, sffin, rs.getString("nombre"), "", "", rs.getString("minuto"), rs.getString("hora"), rs.getString("dia_semana"), rs.getString("mes"), rs.getString("dia_mes"), rs.getString("bundle_id"), status));
                    //monitor_filterDep.add(sfini+"|"+ sffin+"|"+ rs.getString("nombre")+"|"+ ""+"|"+ ""+"|"+ rs.getString("minuto")+"|"+ rs.getString("hora")+"|"+ rs.getString("dia_semana")+"|"+ rs.getString("mes")+"|"+ rs.getString("dia_mes")+"|"+ rs.getString("bundle_id")+"|"+ status);
                    ffin = null;
                    fini = null;
                    sfini = null;
                    sffin = null;
                }
            }
            stmt.close();
            conn.close();

        } catch (SQLException ex) {
            ex.printStackTrace();

        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return monitor_filterDep;

    }

    public String parseMinutes(String p_minutos) {
        String UTChour = "", parseN = "", parseC = "", ant = "";
        boolean flag;


        for (int i = 0; i <= p_minutos.length() - 1; i++) {

            try {
                flag = false;
                Integer.parseInt(Character.toString(p_minutos.charAt(i)));
                parseN += p_minutos.charAt(i);

            } catch (NumberFormatException e) {
                flag = true;
                parseC = Character.toString(p_minutos.charAt(i));

            }
            if (flag || i == p_minutos.length() - 1) {
                if ((i - 1) > 0) {
                    if (!ant.equals("/")) {

                        int uh = Integer.parseInt(parseN);
                        if (uh >= 0 && uh <= 58)
                            uh += 1;
                        else
                            uh = 0;

                        UTChour += Integer.toString(uh);
                    } else {
                        UTChour += parseN;
                    }
                } else {
                    int uh = Integer.parseInt(parseN);
                    if (uh >= 0 && uh <= 58)
                        uh += 1;
                    else
                        uh = 0;
                    UTChour += Integer.toString(uh);
                }
                UTChour += parseC;
                ant = parseC;

                parseN = "";
                parseC = "";
            }
        }


        return UTChour;


    }

    public String RunBundle(String nombreWFDep) {

        String mensaje = null;
        int running = 0, waiting = 0, failed = 0;
        try {
            List<BundleJob> bundleJob = wc.getBundleJobsInfo("Name=" + nombreWFDep + ";Status=RUNNING", 1, 1);
            BundleJob bj = wc.getBundleJobInfo(bundleJob.get(0).getId());
            for (CoordinatorJob coord : bj.getCoordinators()) {
                CoordinatorJob coordJob = wc.getCoordJobInfo(coord.getId());
                List<CoordinatorAction> action = coordJob.getActions();

                if (action.size() > 0)
                    if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.RUNNING)
                        running++;
                    else if (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.WAITING)
                        waiting++;
                    else if ((action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.FAILED) || (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.KILLED) || (action.get(action.size() - 1).getStatus() == CoordinatorAction.Status.SUSPENDED)) {
                        failed++;
                    }
            }


            if (running > 0) {
                mensaje = "No fue posible ejecutar maya de dependencia, ya que se encuentra en ejecucion.";
            } else if (waiting > 0) {
                mensaje = "No fue posible ejecutar maya de dependencia, ya que se encuentra en ejecucion.";
            } else if (failed > 0 && waiting > 0) {
                mensaje = "No fue posible ejecutar maya de dependencia, intente recuperar el proceso fallido.";
            } else {

                Calendar cal = Calendar.getInstance();

                cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1);
                SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat df2 = new SimpleDateFormat("HH");
                SimpleDateFormat df3 = new SimpleDateFormat("mm");

                CreateBundle(nombreWFDep, df1.format(cal.getTime()), df1.format(cal.getTime()), df3.format(cal.getTime()), df2.format(cal.getTime()), "*", "*", "*");

            }
        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return mensaje;
    }

    public String createWorkflowDependencias(String NombreCoord) {
        String jobId = null;

        List<WorkflowJob> workflowList = null;
        try {
            workflowList = wc.getJobsInfo("Name=" + NombreCoord + ";Status=RUNNING");

            if (workflowList.size() > 0) {
                jobId = "No es posible ejecutar proceso ya que esta en ejecucion: " + NombreCoord;
                return jobId;
            }


        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        ///////////////////

        java.sql.Connection conn = null;
        try {

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                   /* DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select a.*,b.*,c.db_user,c.host,c.password,c.port,c.unix_path,c.unix_user,c.sid,c.type tipo_bd " +
                    "from wf_flujos_dependencias a " +
                    "join flow b on a.coordinator=b.name " +
                    "left outer join connection c on (c.id=b.connection_id) " +
                    "where a.coordinator=?  limit 1;";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, NombreCoord);
            rs = stmt.executeQuery();

            Properties conf = wc.createConfiguration();
            Utils util = new Utils();
            ////configuraciones generales
            conf.setProperty(OozieClient.USER_NAME, "appbalam_ingestas");
            conf.setProperty("oozie.use.system.libpath", "true");
            conf.setProperty("oozie.libpath", "hdfs://balam/user/oozie/share/lib");


            while (rs.next()) {
                int indice = Integer.parseInt(rs.getString("posicion"));
                int col = rs.getInt("col");

                conf.setProperty("nombreWFDep", rs.getString("nombre"));
                conf.setProperty(OozieClient.APP_PATH, "hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + rs.getString("nombre") + "/" + NombreCoord);

                ////////////////////////////////////////////////////////////////////////
                conf.setProperty("nombreWorkflow" + indice + col, rs.getString("name"));
                conf.setProperty("nameNode" + indice + col, "hdfs://balam");
                //conf.setProperty("jobTracker" + indice + col, "10.231.236.5:8032");
                conf.setProperty("jobTracker" + indice + col, prop.getProperty("jobTracker"));
                conf.setProperty("app_path" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}");
                conf.setProperty("cta_correo" + indice + col, rs.getString("emails"));
                conf.setProperty("tipo_WF" + indice + col, rs.getString("type"));
                conf.setProperty("destino" + indice + col, rs.getString("is_directory"));
                if (rs.getString("alert_by_email").equals("1"))
                    conf.setProperty("alerta" + indice + col, "si");
                else
                    conf.setProperty("alerta" + indice + col, "no");
                conf.setProperty("tolerancia" + indice + col, rs.getString("percentage_tolerance_records"));
                if (rs.getString("separator_") == null || rs.getString("separator_").equals(""))
                    conf.setProperty("separador" + indice + col, "N/A");
                else {
                    if (rs.getString("separator_").equals(","))
                        conf.setProperty("separador" + indice + col, rs.getString("separator_"));
                    else
                        conf.setProperty("separador" + indice + col, "\\" + rs.getString("separator_"));
                }
                conf.setProperty("date_format" + indice + col, util.get_dateformat("oracle").replace(" ", "_"));

                if (rs.getString("unix_user") == null || rs.getString("unix_user").equals(""))
                    conf.setProperty("remoteNode_spool" + indice + col, "N/A");
                else
                    conf.setProperty("remoteNode_spool" + indice + col, rs.getString("unix_user") + "@" + rs.getString("host"));
                conf.setProperty("path_spool" + indice + col, rs.getString("unix_path") + "/BALAM_INGESTAS");
                if (rs.getString("db_user") == null || rs.getString("db_user").equals(""))
                    conf.setProperty("usuario_bd" + indice + col, "N/A");
                else
                    conf.setProperty("usuario_bd" + indice + col, rs.getString("db_user"));
                if (rs.getString("password") == null || rs.getString("password").equals(""))
                    conf.setProperty("password_bd" + indice + col, "NA");
                else
                    conf.setProperty("password_bd" + indice + col, rs.getString("password"));
                if (rs.getString("sid") == null || rs.getString("sid").equals(""))
                    conf.setProperty("sid" + indice + col, "NA");
                else
                    conf.setProperty("sid" + indice + col, rs.getString("sid"));
                conf.setProperty("Narchivo_spool" + indice + col, rs.getString("filename"));


                ///////////////////////////////////////////////////////////////////////////////////////////
                //  conf.setProperty("shellScriptPath_spool" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh");
                conf.setProperty("shellScriptPath_spool" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh"));


                if (rs.getString("variables") == null || rs.getString("variables").equals(""))
                    conf.setProperty("variables" + indice + col, "NA");
                else
                    conf.setProperty("variables" + indice + col, rs.getString("variables"));
                if (rs.getString("host") == null || rs.getString("host").equals(""))
                    conf.setProperty("host_ip" + indice + col, "N/A");
                else
                    conf.setProperty("host_ip" + indice + col, rs.getString("host"));
                if (rs.getString("port") == null || rs.getString("port").equals(""))
                    conf.setProperty("host_puerto" + indice + col, "N/A");
                else
                    conf.setProperty("host_puerto" + indice + col, rs.getString("port"));
                if (rs.getString("tipo_bd") == null || rs.getString("tipo_bd").equals(""))
                    conf.setProperty("host_tipo_bd" + indice + col, "NA");
                else
                    conf.setProperty("host_tipo_bd" + indice + col, rs.getString("tipo_bd"));
                if (rs.getString("posicion").equals("1"))
                    conf.setProperty("flag_first" + indice + col, "SI");
                else
                    conf.setProperty("flag_first" + indice + col, "NO");
                conf.setProperty("clean_sqoop" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("clean_file" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("argument_spool" + indice + col, "${nombreWorkflow" + indice + col + "} ${usuario_bd" + indice + col + "} ${password_bd" + indice + col + "} ${sid" + indice + col + "} ${path_spool" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${variables" + indice + col + "} ${separador" + indice + col + "} ${date_format" + indice + col + "} ${remoteNode_spool" + indice + col + "} ${host_ip" + indice + col + "} ${host_puerto" + indice + col + "} ${host_tipo_bd" + indice + col + "} ${flag_first" + indice + col + "} ${nombreWFDep}");


                //  conf.setProperty("remoteNode_putHDFS" + indice + col, "appbalam_ingestas@10.231.236.25");
                conf.setProperty("remoteNode_putHDFS" + indice + col, prop.getProperty("remoteNode_putHDFS"));

                // conf.setProperty("shellScriptPath_putHDFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/putHDFS.sh");
                conf.setProperty("shellScriptPath_putHDFS" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/putHDFS.sh"));

                if (rs.getString("type").equals("archivo"))
                    conf.setProperty("path_salidaFS" + indice + col, rs.getString("source_directory"));
                else
                    conf.setProperty("path_salidaFS" + indice + col, util.get_pathconf() + "/salida/");
                // conf.setProperty("path_salidaFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/") ya estaba comentado antes de hacer cambios en el properti;

                if (rs.getString("is_directory").equals("tabla"))
                    conf.setProperty("path_hdfs" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow" + indice + col + "}");
                else if (rs.getString("is_directory").equals("carpeta"))
                    conf.setProperty("path_hdfs" + indice + col, rs.getString("directory"));
                conf.setProperty("argument_putHDFS" + indice + col, "${path_salidaFS" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${path_hdfs" + indice + col + "} ${tipo_WF" + indice + col + "} ${tolerancia" + indice + col + "} ${nombreWorkflow" + indice + col + "} ${alerta" + indice + col + "} ${destino" + indice + col + "} ${acumulado" + indice + col + "}");
                conf.setProperty("path_jarSpark" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}/lib");
                if ((rs.getString("queue") == null || rs.getString("queue").equals("")) && (rs.getString("queue_sqoop") == null || rs.getString("queue_sqoop").equals("")))
                    conf.setProperty("queueName" + indice + col, "NA");
                else if (rs.getString("queue") != null && !rs.getString("queue").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue"));
                else if (rs.getString("queue_sqoop") != null && !rs.getString("queue_sqoop").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue_sqoop"));
                conf.setProperty("path_archivoSpark" + indice + col, "${path_hdfs" + indice + col + "}/*");
                if (rs.getString("schema_database") == null || rs.getString("schema_database").equals(""))
                    conf.setProperty("esquema" + indice + col, "N/A");
                else
                    conf.setProperty("esquema" + indice + col, rs.getString("schema_database"));
                if (rs.getString("tablename") == null || rs.getString("tablename").equals(""))
                    conf.setProperty("tabla" + indice + col, "N/A");
                else
                    conf.setProperty("tabla" + indice + col, rs.getString("tablename"));
                if (rs.getString("particioned").equals("1"))
                    conf.setProperty("particionado" + indice + col, "SI");
                else
                    conf.setProperty("particionado" + indice + col, "NO");
                if (rs.getString("particioned_field") == null || rs.getString("particioned_field").equals(""))
                    conf.setProperty("campo_particion" + indice + col, "N/A");
                else
                    conf.setProperty("campo_particion" + indice + col, rs.getString("particioned_field"));
                if (rs.getString("override").equals("1"))
                    conf.setProperty("acumulado" + indice + col, "NO");
                else
                    conf.setProperty("acumulado" + indice + col, "SI");
                conf.setProperty("optionFile" + indice + col, "${nombreWorkflow" + indice + col + "}.txt");
                conf.setProperty("option_File_path_in_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/${nombreWorkflow" + indice + col + "}.txt");
                conf.setProperty("sqoop_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/${nombreWorkflow" + indice + col + "}");
                if (indice == rs.getInt("max"))
                    conf.setProperty("flag_last" + indice + col, "SI");
                else
                    conf.setProperty("flag_last" + indice + col, "NO");
                conf.setProperty("argument_trigger" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_last" + indice + col + "}");
                if (rs.getString("dependencia") != null) {
                    String[] tmp = rs.getString("dependencia").split("\\|");
                    for (int i = 0; i <= tmp.length - 1; i++) {
                        conf.setProperty("triggerFileDir" + indice + col + i, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/triggers/${nombreWFDep}/" + tmp[i]);
                    }
                }
                conf.setProperty("share_lib_ojdbc" + indice + col, "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/ojdbc6.jar");
                conf.setProperty("share_lib_sqooop_jar" + indice + col, "hdfs://balam/user/oozie/share/lib_20170713001650/sqoop/sqoop-1.4.6.2.6.1.0-129.jar");
                ////////////////////////////////////////////////////////////////////////

                //System.out.println("hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/"+ rs.getString("nombre")+"/"+NombreCoord);
            }

            stmt.close();
            conn.close();

            try {
                jobId = wc.run(conf);
            } catch (OozieClientException e) {
                e.printStackTrace();
            }
            System.out.println("Flujo de trabajo enviado...");
            //System.out.println(wc.getJobInfo(jobId));

        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();

        } finally {
            try {

                conn.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        return null;

    }

    public String createWorkflowDependencias2(String NombreCoord) {
        String jobId = null;

        List<WorkflowJob> workflowList = null;
        try {
            workflowList = wc.getJobsInfo("Name=" + NombreCoord + ";Status=RUNNING");

            if (workflowList.size() > 0) {
                jobId = "No es posible ejecutar proceso ya que esta en ejecucion: " + NombreCoord;
                return jobId;
            }


        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        ///////////////////

        java.sql.Connection conn = null;
        try {

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select a.*,b.*,c.db_user,c.host,c.password,c.port,c.unix_path,c.unix_user,c.sid,c.type tipo_bd " +
                    "from wf_flujos_dependencias a " +
                    "join flow b on a.coordinator=b.name " +
                    "left outer join connection c on (c.id=b.connection_id) " +
                    "where a.coordinator=?  limit 1;";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, NombreCoord);
            rs = stmt.executeQuery();

            Properties conf = wc.createConfiguration();

            Utils util = new Utils();

            util.Oracle_query_sqoop(rs.getString("host"),
                    rs.getString("port"),
                    rs.getString("sid"),
                    rs.getString("db_user"),
                    rs.getString("password"),
                    rs.getString("query"),
                    //  "/home/appbalam_ingestas/INGESTAS_BALAM/config/sqoop_config/",
                    prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/"),
                    rs.getString("name") + "_reproceso",
                    rs.getString("separator_"), "/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/".concat(rs.getString("name")), rs.getString("file_size"),
                    rs.getString("split"),
                    rs.getString("mappers"),
                    rs.getString("variables2"),
                    rs.getString("table_size"),
                    rs.getString("type"));

            HdfsBalam hdfs = new HdfsBalam();
            try {
                //  hdfs.copyFromLocal("/home/appbalam_ingestas/INGESTAS_BALAM/config/sqoop_config/".concat(rs.getString("name").concat("_reproceso.txt")), "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/");
                hdfs.copyFromLocal(prop.getProperty("home").concat("/INGESTAS_BALAM/config/sqoop_config/").concat(rs.getString("name").concat("_reproceso.txt")), "/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/");

            } catch (IOException e) {
                logger.error(e.getMessage());
            }

            ////configuraciones generales
            conf.setProperty(OozieClient.USER_NAME, "appbalam_ingestas");
            conf.setProperty("oozie.use.system.libpath", "true");
            conf.setProperty("oozie.libpath", "hdfs://balam/user/oozie/share/lib");


            while (rs.next()) {
                int indice = Integer.parseInt(rs.getString("posicion"));
                int col = rs.getInt("col");

                conf.setProperty("nombreWFDep", rs.getString("nombre"));
                conf.setProperty(OozieClient.APP_PATH, "hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + rs.getString("nombre") + "/" + NombreCoord);

                ////////////////////////////////////////////////////////////////////////
                conf.setProperty("nombreWorkflow" + indice + col, rs.getString("name"));
                conf.setProperty("nameNode" + indice + col, "hdfs://balam");
                //conf.setProperty("jobTracker" + indice + col, "10.231.236.5:8032");
                conf.setProperty("jobTracker" + indice + col, prop.getProperty("jobTracker"));
                conf.setProperty("app_path" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}");
                conf.setProperty("cta_correo" + indice + col, rs.getString("emails"));
                conf.setProperty("tipo_WF" + indice + col, rs.getString("type"));
                conf.setProperty("destino" + indice + col, rs.getString("is_directory"));
                if (rs.getString("alert_by_email").equals("1"))
                    conf.setProperty("alerta" + indice + col, "si");
                else
                    conf.setProperty("alerta" + indice + col, "no");
                conf.setProperty("tolerancia" + indice + col, rs.getString("percentage_tolerance_records"));
                if (rs.getString("separator_") == null || rs.getString("separator_").equals(""))
                    conf.setProperty("separador" + indice + col, "N/A");
                else {
                    if (rs.getString("separator_").equals(","))
                        conf.setProperty("separador" + indice + col, rs.getString("separator_"));
                    else
                        conf.setProperty("separador" + indice + col, "\\" + rs.getString("separator_"));
                }
                conf.setProperty("date_format" + indice + col, util.get_dateformat("oracle").replace(" ", "_"));

                if (rs.getString("unix_user") == null || rs.getString("unix_user").equals(""))
                    conf.setProperty("remoteNode_spool" + indice + col, "N/A");
                else
                    conf.setProperty("remoteNode_spool" + indice + col, rs.getString("unix_user") + "@" + rs.getString("host"));
                conf.setProperty("path_spool" + indice + col, rs.getString("unix_path") + "/BALAM_INGESTAS");
                if (rs.getString("db_user") == null || rs.getString("db_user").equals(""))
                    conf.setProperty("usuario_bd" + indice + col, "N/A");
                else
                    conf.setProperty("usuario_bd" + indice + col, rs.getString("db_user"));
                if (rs.getString("password") == null || rs.getString("password").equals(""))
                    conf.setProperty("password_bd" + indice + col, "NA");
                else
                    conf.setProperty("password_bd" + indice + col, rs.getString("password"));
                if (rs.getString("sid") == null || rs.getString("sid").equals(""))
                    conf.setProperty("sid" + indice + col, "NA");
                else
                    conf.setProperty("sid" + indice + col, rs.getString("sid"));
                conf.setProperty("Narchivo_spool" + indice + col, rs.getString("filename"));
                // conf.setProperty("shellScriptPath_spool" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh");
                conf.setProperty("shellScriptPath_spool" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh"));


                if (rs.getString("variables") == null || rs.getString("variables").equals(""))
                    conf.setProperty("variables" + indice + col, "NA");
                else
                    conf.setProperty("variables" + indice + col, rs.getString("variables"));
                if (rs.getString("host") == null || rs.getString("host").equals(""))
                    conf.setProperty("host_ip" + indice + col, "N/A");
                else
                    conf.setProperty("host_ip" + indice + col, rs.getString("host"));
                if (rs.getString("port") == null || rs.getString("port").equals(""))
                    conf.setProperty("host_puerto" + indice + col, "N/A");
                else
                    conf.setProperty("host_puerto" + indice + col, rs.getString("port"));
                if (rs.getString("tipo_bd") == null || rs.getString("tipo_bd").equals(""))
                    conf.setProperty("host_tipo_bd" + indice + col, "NA");
                else
                    conf.setProperty("host_tipo_bd" + indice + col, rs.getString("tipo_bd"));
                if (rs.getString("posicion").equals("1"))
                    conf.setProperty("flag_first" + indice + col, "SI");
                else
                    conf.setProperty("flag_first" + indice + col, "NO");
                conf.setProperty("clean_sqoop" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("clean_file" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("argument_spool" + indice + col, "${nombreWorkflow" + indice + col + "} ${usuario_bd" + indice + col + "} ${password_bd" + indice + col + "} ${sid" + indice + col + "} ${path_spool" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${variables" + indice + col + "} ${separador" + indice + col + "} ${date_format" + indice + col + "} ${remoteNode_spool" + indice + col + "} ${host_ip" + indice + col + "} ${host_puerto" + indice + col + "} ${host_tipo_bd" + indice + col + "} ${flag_first" + indice + col + "} ${nombreWFDep}");


                // conf.setProperty("remoteNode_putHDFS" + indice + col, "appbalam_ingestas@10.231.236.25");
                conf.setProperty("remoteNode_putHDFS" + indice + col, prop.getProperty("remoteNode_putHDFS"));


                //conf.setProperty("shellScriptPath_putHDFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/putHDFS.sh");
                conf.setProperty("shellScriptPath_putHDFS" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/putHDFS.sh"));

                if (rs.getString("type").equals("archivo"))
                    conf.setProperty("path_salidaFS" + indice + col, rs.getString("source_directory"));
                else
                    conf.setProperty("path_salidaFS" + indice + col, util.get_pathconf() + "/salida/");
                // conf.setProperty("path_salidaFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/"); ya estaba comentado antes de hacer los Cambios el 08/03/19

                if (rs.getString("is_directory").equals("tabla"))
                    conf.setProperty("path_hdfs" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow" + indice + col + "}");
                else if (rs.getString("is_directory").equals("carpeta"))
                    conf.setProperty("path_hdfs" + indice + col, rs.getString("directory"));
                conf.setProperty("argument_putHDFS" + indice + col, "${path_salidaFS" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${path_hdfs" + indice + col + "} ${tipo_WF" + indice + col + "} ${tolerancia" + indice + col + "} ${nombreWorkflow" + indice + col + "} ${alerta" + indice + col + "} ${destino" + indice + col + "} ${acumulado" + indice + col + "}");
                conf.setProperty("path_jarSpark" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}/lib");
                if ((rs.getString("queue") == null || rs.getString("queue").equals("")) && (rs.getString("queue_sqoop") == null || rs.getString("queue_sqoop").equals("")))
                    conf.setProperty("queueName" + indice + col, "NA");
                else if (rs.getString("queue") != null && !rs.getString("queue").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue"));
                else if (rs.getString("queue_sqoop") != null && !rs.getString("queue_sqoop").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue_sqoop"));
                conf.setProperty("path_archivoSpark" + indice + col, "${path_hdfs" + indice + col + "}/*");
                if (rs.getString("schema_database") == null || rs.getString("schema_database").equals(""))
                    conf.setProperty("esquema" + indice + col, "N/A");
                else
                    conf.setProperty("esquema" + indice + col, rs.getString("schema_database"));
                if (rs.getString("tablename") == null || rs.getString("tablename").equals(""))
                    conf.setProperty("tabla" + indice + col, "N/A");
                else
                    conf.setProperty("tabla" + indice + col, rs.getString("tablename"));
                if (rs.getString("particioned").equals("1"))
                    conf.setProperty("particionado" + indice + col, "SI");
                else
                    conf.setProperty("particionado" + indice + col, "NO");
                if (rs.getString("particioned_field") == null || rs.getString("particioned_field").equals(""))
                    conf.setProperty("campo_particion" + indice + col, "N/A");
                else
                    conf.setProperty("campo_particion" + indice + col, rs.getString("particioned_field"));
                if (rs.getString("override").equals("1"))
                    conf.setProperty("acumulado" + indice + col, "NO");
                else
                    conf.setProperty("acumulado" + indice + col, "SI");
                conf.setProperty("optionFile" + indice + col, "${nombreWorkflow" + indice + col + "}_reproceso.txt");
                conf.setProperty("option_File_path_in_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/${nombreWorkflow" + indice + col + "}_reproceso.txt");
                conf.setProperty("sqoop_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/${nombreWorkflow" + indice + col + "}");
                if (indice == rs.getInt("max"))
                    conf.setProperty("flag_last" + indice + col, "SI");
                else
                    conf.setProperty("flag_last" + indice + col, "NO");
                conf.setProperty("argument_trigger" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_last" + indice + col + "}");
                if (rs.getString("dependencia") != null) {
                    String[] tmp = rs.getString("dependencia").split("\\|");
                    for (int i = 0; i <= tmp.length - 1; i++) {
                        conf.setProperty("triggerFileDir" + indice + col + i, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/triggers/${nombreWFDep}/" + tmp[i]);
                    }
                }
                conf.setProperty("share_lib_ojdbc" + indice + col, "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/ojdbc6.jar");
                conf.setProperty("share_lib_sqooop_jar" + indice + col, "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/sqoop-1.4.6.2.6.1.0-129.jar");
                ////////////////////////////////////////////////////////////////////////

                //System.out.println("hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/"+ rs.getString("nombre")+"/"+NombreCoord);
            }

            stmt.close();
            conn.close();

            try {
                jobId = wc.run(conf);
            } catch (OozieClientException e) {
                e.printStackTrace();
            }
            System.out.println("Flujo de trabajo enviado...");
            //System.out.println(wc.getJobInfo(jobId));

        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();

        } finally {
            try {

                conn.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        return null;

    }

    public String createWorkflowDependencias3(String NombreCoord) {
        String jobId = null;

        List<WorkflowJob> workflowList = null;
        try {
            workflowList = wc.getJobsInfo("Name=" + NombreCoord + ";Status=RUNNING");

            if (workflowList.size() > 0) {
                jobId = "No es posible ejecutar proceso ya que esta en ejecucion: " + NombreCoord;
                return jobId;
            }


        } catch (OozieClientException e) {
            e.printStackTrace();
        }
        ///////////////////

        java.sql.Connection conn = null;
        try {
            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select a.*,b.*,c.db_user,c.host,c.password,c.port,c.unix_path,c.unix_user,c.sid,c.type tipo_bd " +
                    "from wf_flujos_dependencias a " +
                    "join flow b on a.coordinator=b.name " +
                    "left outer join connection c on (c.id=b.connection_id) " +
                    "where a.coordinator=?  limit 1;";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, NombreCoord);
            rs = stmt.executeQuery();

            Properties conf = wc.createConfiguration();

            Utils util = new Utils();


            ////configuraciones generales
            conf.setProperty(OozieClient.USER_NAME, "appbalam_ingestas");
            conf.setProperty("oozie.use.system.libpath", "true");
            conf.setProperty("oozie.libpath", "hdfs://balam/user/oozie/share/lib");


            while (rs.next()) {
                int indice = Integer.parseInt(rs.getString("posicion"));
                int col = rs.getInt("col");

                conf.setProperty("nombreWFDep", rs.getString("nombre"));
                conf.setProperty(OozieClient.APP_PATH, "hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + rs.getString("nombre") + "/" + NombreCoord);

                ////////////////////////////////////////////////////////////////////////
                conf.setProperty("nombreWorkflow" + indice + col, rs.getString("name"));
                conf.setProperty("nameNode" + indice + col, "hdfs://balam");
                conf.setProperty("jobTracker" + indice + col, prop.getProperty("jobTracker"));
                conf.setProperty("app_path" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}");
                conf.setProperty("cta_correo" + indice + col, rs.getString("emails"));
                conf.setProperty("tipo_WF" + indice + col, rs.getString("type"));
                conf.setProperty("destino" + indice + col, rs.getString("is_directory"));
                if (rs.getString("alert_by_email").equals("1"))
                    conf.setProperty("alerta" + indice + col, "si");
                else
                    conf.setProperty("alerta" + indice + col, "no");
                conf.setProperty("tolerancia" + indice + col, rs.getString("percentage_tolerance_records"));
                if (rs.getString("separator_") == null || rs.getString("separator_").equals(""))
                    conf.setProperty("separador" + indice + col, "N/A");
                else {
                    if (rs.getString("separator_").equals(","))
                        conf.setProperty("separador" + indice + col, rs.getString("separator_"));
                    else
                        conf.setProperty("separador" + indice + col, "\\" + rs.getString("separator_"));
                }
                conf.setProperty("date_format" + indice + col, util.get_dateformat("oracle").replace(" ", "_"));

                if (rs.getString("unix_user") == null || rs.getString("unix_user").equals(""))
                    conf.setProperty("remoteNode_spool" + indice + col, "N/A");
                else
                    conf.setProperty("remoteNode_spool" + indice + col, rs.getString("unix_user") + "@" + rs.getString("host"));
                conf.setProperty("path_spool" + indice + col, rs.getString("unix_path") + "/BALAM_INGESTAS");
                if (rs.getString("db_user") == null || rs.getString("db_user").equals(""))
                    conf.setProperty("usuario_bd" + indice + col, "N/A");
                else
                    conf.setProperty("usuario_bd" + indice + col, rs.getString("db_user"));
                if (rs.getString("password") == null || rs.getString("password").equals(""))
                    conf.setProperty("password_bd" + indice + col, "NA");
                else
                    conf.setProperty("password_bd" + indice + col, rs.getString("password"));
                if (rs.getString("sid") == null || rs.getString("sid").equals(""))
                    conf.setProperty("sid" + indice + col, "NA");
                else
                    conf.setProperty("sid" + indice + col, rs.getString("sid"));
                conf.setProperty("Narchivo_spool" + indice + col, rs.getString("filename"));
                conf.setProperty("shellScriptPath_spool" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/dependencias/run_remote_dep.sh"));
                if (rs.getString("variables2") == null || rs.getString("variables2").equals(""))
                    conf.setProperty("variables" + indice + col, "NA");
                else
                    conf.setProperty("variables" + indice + col, rs.getString("variables2"));
                if (rs.getString("host") == null || rs.getString("host").equals(""))
                    conf.setProperty("host_ip" + indice + col, "N/A");
                else
                    conf.setProperty("host_ip" + indice + col, rs.getString("host"));
                if (rs.getString("port") == null || rs.getString("port").equals(""))
                    conf.setProperty("host_puerto" + indice + col, "N/A");
                else
                    conf.setProperty("host_puerto" + indice + col, rs.getString("port"));
                if (rs.getString("tipo_bd") == null || rs.getString("tipo_bd").equals(""))
                    conf.setProperty("host_tipo_bd" + indice + col, "NA");
                else
                    conf.setProperty("host_tipo_bd" + indice + col, rs.getString("tipo_bd"));
                if (rs.getString("posicion").equals("1"))
                    conf.setProperty("flag_first" + indice + col, "SI");
                else
                    conf.setProperty("flag_first" + indice + col, "NO");
                conf.setProperty("clean_sqoop" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("clean_file" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_first" + indice + col + "}");
                conf.setProperty("argument_spool" + indice + col, "${nombreWorkflow" + indice + col + "} ${usuario_bd" + indice + col + "} ${password_bd" + indice + col + "} ${sid" + indice + col + "} ${path_spool" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${variables" + indice + col + "} ${separador" + indice + col + "} ${date_format" + indice + col + "} ${remoteNode_spool" + indice + col + "} ${host_ip" + indice + col + "} ${host_puerto" + indice + col + "} ${host_tipo_bd" + indice + col + "} ${flag_first" + indice + col + "} ${nombreWFDep}");
                conf.setProperty("remoteNode_putHDFS" + indice + col, prop.getProperty("remoteNode_putHDFS"));
                conf.setProperty("shellScriptPath_putHDFS" + indice + col, prop.getProperty("home").concat("/INGESTAS_BALAM/config/lib/putHDFS.sh"));
                if (rs.getString("type").equals("archivo"))
                    conf.setProperty("path_salidaFS" + indice + col, rs.getString("source_directory"));
                else
                    conf.setProperty("path_salidaFS" + indice + col, util.get_pathconf() + "/salida/");
                // conf.setProperty("path_salidaFS" + indice + col, "/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/");

                if (rs.getString("is_directory").equals("tabla"))
                    conf.setProperty("path_hdfs" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/data/spark_test/${nombreWorkflow" + indice + col + "}");
                else if (rs.getString("is_directory").equals("carpeta"))
                    conf.setProperty("path_hdfs" + indice + col, rs.getString("directory"));
                conf.setProperty("argument_putHDFS" + indice + col, "${path_salidaFS" + indice + col + "} ${Narchivo_spool" + indice + col + "} ${path_hdfs" + indice + col + "} ${tipo_WF" + indice + col + "} ${tolerancia" + indice + col + "} ${nombreWorkflow" + indice + col + "} ${alerta" + indice + col + "} ${destino" + indice + col + "} ${acumulado" + indice + col + "}");
                conf.setProperty("path_jarSpark" + indice + col, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/${nombreWFDep}/${nombreWorkflow" + indice + col + "}/lib");
                if ((rs.getString("queue") == null || rs.getString("queue").equals("")) && (rs.getString("queue_sqoop") == null || rs.getString("queue_sqoop").equals("")))
                    conf.setProperty("queueName" + indice + col, "NA");
                else if (rs.getString("queue") != null && !rs.getString("queue").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue"));
                else if (rs.getString("queue_sqoop") != null && !rs.getString("queue_sqoop").equals(""))
                    conf.setProperty("queueName" + indice + col, rs.getString("queue_sqoop"));
                conf.setProperty("path_archivoSpark" + indice + col, "${path_hdfs" + indice + col + "}/*");
                if (rs.getString("schema_database") == null || rs.getString("schema_database").equals(""))
                    conf.setProperty("esquema" + indice + col, "N/A");
                else
                    conf.setProperty("esquema" + indice + col, rs.getString("schema_database"));
                if (rs.getString("tablename") == null || rs.getString("tablename").equals(""))
                    conf.setProperty("tabla" + indice + col, "N/A");
                else
                    conf.setProperty("tabla" + indice + col, rs.getString("tablename"));
                if (rs.getString("particioned").equals("1"))
                    conf.setProperty("particionado" + indice + col, "SI");
                else
                    conf.setProperty("particionado" + indice + col, "NO");
                if (rs.getString("particioned_field") == null || rs.getString("particioned_field").equals(""))
                    conf.setProperty("campo_particion" + indice + col, "N/A");
                else
                    conf.setProperty("campo_particion" + indice + col, rs.getString("particioned_field"));
                if (rs.getString("override").equals("1"))
                    conf.setProperty("acumulado" + indice + col, "NO");
                else
                    conf.setProperty("acumulado" + indice + col, "SI");
                conf.setProperty("optionFile" + indice + col, "${nombreWorkflow" + indice + col + "}_reproceso.txt");
                conf.setProperty("option_File_path_in_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Workflows/WF_BALAM/lib/${nombreWorkflow" + indice + col + "}_reproceso.txt");
                conf.setProperty("sqoop_hdfs" + indice + col, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/sqoop/data_test/${nombreWorkflow" + indice + col + "}");
                if (indice == rs.getInt("max"))
                    conf.setProperty("flag_last" + indice + col, "SI");
                else
                    conf.setProperty("flag_last" + indice + col, "NO");
                conf.setProperty("argument_trigger" + indice + col, "${nombreWFDep} ${nombreWorkflow" + indice + col + "} ${flag_last" + indice + col + "}");
                if (rs.getString("dependencia") != null) {
                    String[] tmp = rs.getString("dependencia").split("\\|");
                    for (int i = 0; i <= tmp.length - 1; i++) {
                        conf.setProperty("triggerFileDir" + indice + col + i, "${nameNode" + indice + col + "}/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/triggers/${nombreWFDep}/" + tmp[i]);
                    }
                }
                conf.setProperty("share_lib_ojdbc" + indice + col, "hdfs://balam/user/oozie/share/lib/lib_20170713001650/sqoop/ojdbc6.jar");
                conf.setProperty("share_lib_sqooop_jar" + indice + col, "hdfs://balam/user/oozie/share/lib_20170713001650/sqoop/sqoop-1.4.6.2.6.1.0-129.jar");
                ////////////////////////////////////////////////////////////////////////

                //System.out.println("hdfs://balam/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/"+ rs.getString("nombre")+"/"+NombreCoord);
            }

            stmt.close();
            conn.close();

            try {
                jobId = wc.run(conf);
            } catch (OozieClientException e) {
                e.printStackTrace();
            }
            System.out.println("Flujo de trabajo enviado...");
            //System.out.println(wc.getJobInfo(jobId));

        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();

        } finally {
            try {

                conn.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        return null;

    }


}
