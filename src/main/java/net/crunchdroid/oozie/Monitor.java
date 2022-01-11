package net.crunchdroid.oozie;

import com.mysql.jdbc.PreparedStatement;
import net.crunchdroid.model.WfBalamBitacoraEjecucion;
import net.crunchdroid.model.Wf_bundles;
import net.crunchdroid.pojo.WorkflowFilter;
import net.crunchdroid.util.Utils;
import org.apache.oozie.client.*;
//import org.apache.oozie.client.OozieClient;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//import java.io.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Monitor {
    Logger logger = LoggerFactory.getLogger(WorkflowFilter.class);

    private static OozieClient wc = null;
    //private static OozieClient wc=new OozieClient("http://10.231.236.5:11000/oozie");
    Utils utils = new Utils();

    public Monitor() {

        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }
        // wc = new OozieClient("http://10.231.236.5:11000/oozie");
       // wc = new OozieClient(prop.getProperty("oozieServerURL"));
    wc=new OozieClient("http://10.231.236.5:11000/oozie");
    }


    public List<WorkflowJobShow> getWorkflows(String p_fini, String p_ffin, String status, String WFname, String
            Pais, String SID, String p_wf) {
        int inicio = 1;
        boolean flag = false;

        ArrayList<WorkflowJobShow> WFJob = new ArrayList<>();
        Date fecstart = null;
        String fini = null;
        Date fecfin = null;
        String ffin = null;
        String WFError;
        String WFPavance;
        String WFAlerta;
        String p_status = null;

        int succeeded = 10;
        int succeed_alert = 2;
        int suspended = 0;
        int killed = 0;
        int running = 0;
        int prep = 0;
        int failed = 0;


        List<WorkflowJob> workflowList = new ArrayList<>();
        while (flag == false) {
            try {
                workflowList = wc.getJobsInfo("", inicio, 30);

            } catch (OozieClientException e) {
                e.printStackTrace();
            }
            try {
                for (WorkflowJob workflowJob : workflowList) {

                    logger.info(workflowJob.toString());

                    WFError = "";
                    WFAlerta = "";
                    DateFormat formatFilter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    Date dp_fini;
                    Date dp_ffin;
                    Date dpo_fstart = null;
                    if (p_fini == null || p_ffin == null) {
                        dp_fini = formatFilter.parse("2018-11-01");
                        dp_ffin = formatFilter.parse("2018-11-12");
                    } else {
                        dp_fini = formatFilter.parse(p_fini);
                        dp_ffin = formatFilter.parse(p_ffin);
                    }

                    DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    if (workflowJob.getStartTime() != null) {
                        fecstart = dateOozie.parse(workflowJob.getStartTime().toString());
                        dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                        fini = dateFormat.format(fecstart);
                    }
                    if (workflowJob.getEndTime() != null) {
                        fecfin = dateOozie.parse(workflowJob.getEndTime().toString());
                        ffin = dateFormat.format(fecfin);
                    }
                    Properties prop = new Properties();
                    InputStream is = null;

                    try {
                        is = new FileInputStream("path.properties");
                        prop.load(is);
                    } catch (IOException e) {
                        System.out.println(e.toString());
                    }
                    if (workflowJob.getUser().toString().equals(prop.getProperty("userServer"))) {
                        if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                            if (status == null) {
                                logger.info("status vacio");
                            }
                            if (workflowJob.getStatus() == null) {
                                logger.info("status vacio 2");
                            }

                            if ((status != null && status.equals("*")) || (status != null && status.equals(workflowJob.getStatus().toString().trim()))) {
                                if (WFname.equals("*") || workflowJob.getAppName().toUpperCase().contains(WFname.toUpperCase())) {
                                    if (Pais.equals("*") || workflowJob.getAppName().toUpperCase().contains(Pais.toUpperCase())) {
                                        List<WorkflowAction> workflowAction = null;

                                        WorkflowJob w2 = wc.getJobInfo(workflowJob.getId());
                                        //String confSID = get_confSID(w2.getConf());
                                        //String tipo_WF = get_confTipoWF(w2.getConf());
                                        String depcol = get_dependenciaWF(w2.getConf(), w2.getAppName());

                                        String confSID = get_confSID(w2.getConf(), depcol);
                                        String tipo_WF = get_confTipoWF(w2.getConf(), depcol);

                                        if (p_wf.equals("*") || p_wf.equals(tipo_WF)) {
                                            if (SID.equals("*") || SID.trim().equals(confSID)) {
                                                if (workflowJob.getStatus().toString().trim().equals("SUCCEEDED")) {
                                                    WFError = "";
                                                    WFPavance = "100";

                                                    WorkflowJob w = wc.getJobInfo(workflowJob.getId());
                                                    for (WorkflowAction wfAction : w.getActions()) {
                                                        if (wfAction.getName().trim().equals("Mail_ALERTA")) {
                                                            WFAlerta = get_error(wfAction.getConf());
                                                            break;
                                                        } else {
                                                            WFAlerta = "";
                                                        }
                                                    }
                                                    ///////////////___ SE AGREGO ESTE BLOQUE DE CODIGO LA FECHA 19/07/2019___/////////////////////
                                                    if (WFAlerta.equals("")) {
                                                        if (tipo_WF.equals("spool")) {
                                                            String v_fechafin;
                                                            if (workflowJob.getEndTime() == null)
                                                                v_fechafin = null;
                                                            else
                                                                v_fechafin = workflowJob.getEndTime().toString();
                                                            String Alert_cmp = get_compareRows(
                                                                    workflowJob.getAppName(),
                                                                    workflowJob.getStartTime().toString(),
                                                                    v_fechafin);
                                                            if (!Alert_cmp.equals("-1")) {
                                                                WFAlerta = "Alerta: Diferencia de registros detectada, Registros en BD: " + Alert_cmp;
                                                            }
                                                        }
                                                    }
                                                    //////////////////____FINALIZA____////////////////////

                                                    String regs = get_registros(workflowJob.getAppName().toString().trim(), fini.replace("-", ""), ffin.replace("-", ""));

                                                    WFError = "Registros insertados: " + regs;

                                                    if (WFAlerta.length() > 0) {
                                                        p_status = "SUCCEDEED_ALERT";
                                                    } else {

                                                        p_status = workflowJob.getStatus().toString();
                                                    }
                                                } else if (workflowJob.getStatus().toString().trim().equals("RUNNING")) {
                                                    WFError = "";
                                                    WFPavance = "0";
                                                    WFAlerta = "";

                                                    WorkflowJob w = wc.getJobInfo(workflowJob.getId());
                                                    for (WorkflowAction wfAction : w.getActions()) {
                                                        if (wfAction.getStatus().toString().trim().equals("RUNNING")) {
                                                            if (wfAction.getName().toString().equals("sshAction_Spool")) {
                                                                WFPavance = "15";
                                                                //revisar en la base de datos mysql
                                                                if (tipo_WF.equals("spool")) {
                                                                    String v_fechafin;
                                                                    if (workflowJob.getEndTime() == null)
                                                                        v_fechafin = null;
                                                                    else
                                                                        v_fechafin = wfAction.getEndTime().toString();
                                                                    String v_estado = get_bitacora(workflowJob.getAppName(), workflowJob.getStartTime().toString(), v_fechafin);
                                                                    if (v_estado.equals("Inicio 1")) {
                                                                        WFPavance = "20";
                                                                    } else if (v_estado.equals("Inicio 2")) {
                                                                        WFPavance = "30";
                                                                    } else if (v_estado.equals("Inicio 3")) {
                                                                        WFPavance = "35";
                                                                    } else if (v_estado.equals("Inicio 4")) {
                                                                        WFPavance = "40";
                                                                    } else if (v_estado.equals("Fin")) {
                                                                        WFPavance = "45";
                                                                    }
                                                                }
                                                                //////////////////////////////////
                                                            } else if (wfAction.getName().equals("spoolAction_qry")) {
                                                                WFPavance = "25";
                                                            } else if (wfAction.getName().toString().equals("sshAction_putHDFS")) {
                                                                WFPavance = "50";
                                                                String v_fechafin;
                                                                if (workflowJob.getEndTime() == null)
                                                                    v_fechafin = null;
                                                                else
                                                                    v_fechafin = wfAction.getEndTime().toString();
                                                                String v_estado = get_bitacora(workflowJob.getAppName(), workflowJob.getStartTime().toString(), v_fechafin);
                                                                if (v_estado.equals("Hdfs inicio 2")) {
                                                                    WFPavance = "60";
                                                                } else if (v_estado.equals("Hdfs inicio 3")) {
                                                                    WFPavance = "65";
                                                                } else if (v_estado.equals("Hdfs Fin")) {
                                                                    WFPavance = "70";
                                                                }
                                                            } else if (wfAction.getName().toString().equals("sparkAction_Insert")) {
                                                                WFPavance = "75";
                                                            }
                                                            break;
                                                        }
                                                    }
                                                    p_status = workflowJob.getStatus().toString();
                                                    logger.info("Printing status ..." + p_status);

                                                } else if (workflowJob.getStatus().toString().trim().equals("PREP")) {
                                                    WFError = "";
                                                    WFPavance = "0";
                                                    WFAlerta = "";

                                                    p_status = workflowJob.getStatus().toString();
                                                } else {
                                                    WFAlerta = "";
                                                    WFPavance = "0";
                                                    p_status = workflowJob.getStatus().toString();
                                                    WorkflowJob w = wc.getJobInfo(workflowJob.getId());
                                                    for (WorkflowAction wfAction : w.getActions()) {
                                                        if (!wfAction.getStatus().toString().trim().equals("OK")) {
                                                            if (wfAction.getErrorMessage() != null) {
                                                                WFError = wfAction.getErrorMessage();
                                                            } else {
                                                                WFError = "Flujo Cancelado";
                                                            }
                                                            break;

                                                        } else if (wfAction.getName().toString().equals("Mail_spool") || wfAction.getName().toString().equals("Mail_putHDFS")) {
                                                            WFError = get_error(wfAction.getConf());
                                                            break;
                                                        }
                                                    }
                                                    for (WorkflowAction wfAction : w.getActions()) {
                                                        if (wfAction.getName().toString().equals("sshAction_Spool") || wfAction.getName().equals("spoolAction_qry")) {
                                                            WFPavance = "25";
                                                            break;
                                                        } else if (wfAction.getName().toString().equals("sshAction_putHDFS")) {
                                                            WFPavance = "50";
                                                            break;
                                                        } else if (wfAction.getName().toString().equals("sparkAction_Insert")) {
                                                            WFPavance = "75";
                                                            break;
                                                        }
                                                    }


                                                }

                                                String p_fechafin = "";
                                                if (workflowJob.getEndTime() == null) {
                                                    p_fechafin = null;
                                                } else {

                                                    p_fechafin = workflowJob.getEndTime().toString();
                                                }

                                                //WFJob.add(workflowJob.getId() + "|" + workflowJob.getAppName() + "|" + workflowJob.getStatus() + "|" + fini + "|" + ffin + "|" + WFAlerta + "|" + WFError.replace("\n", " ") + "|" + WFPavance);
                                                WFJob.add(new WorkflowJobShow(workflowJob.getId(), workflowJob.getAppName(), p_status, fini, ffin, WFAlerta, WFError.replace("\n", ""), WFPavance, wc.getJobInfo(workflowJob.getId()).getActions(), succeeded, succeed_alert, suspended, killed, running, prep, failed,
                                                        get_bitacora2(workflowJob.getAppName(), workflowJob.getStartTime().toString(), p_fechafin)));


                                            }//if sid
                                        }//if p_wf
                                    }
                                }
                            }
                        }
                    }//valida usuario

                    if (dpo_fstart.before(dp_fini)) {
                        flag = true;
                        break;
                    }
                }
                inicio += 30;

                if (workflowList.size() <= 0) {
                    flag = true;
                }
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (OozieClientException e) {
                e.printStackTrace();
            }
        }
        return WFJob;
    }

    public List<CoordinatorJobShow> getWorkflowsCoordinators(String p_fini, String p_ffin, String CoordName, String
            Pais, String SID, String p_wf) {
        Date fecstart = null;
        String fini = null;
        String fNextMat = null;
        Date fecfin = null;
        String ffin = null;
        ArrayList<CoordinatorJobShow> WFJobCoord = new ArrayList<>();
        DateFormat dateOozie = new SimpleDateFormat(
                "E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        List<CoordinatorJob> coordList = null;
        try {
            coordList = wc.getCoordJobsInfo("Status=RUNNING;Status=PREP;Status=SUSPENDED", 0, 2000);
            logger.info(coordList + "datos coord");
        } catch (OozieClientException e) {
            e.printStackTrace();
        }

        DateFormat formatFilter = new SimpleDateFormat("yyyy-MM-dd");
        for (CoordinatorJob Coord : coordList) {

            try {
                Date dp_fini = formatFilter.parse(p_fini);

                Date dp_ffin = formatFilter.parse(p_ffin);
                Date dpo_fstart = null;

                fecstart = dateOozie.parse(Coord.getStartTime().toString());
                dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                fini = dateFormat.format(fecstart);
                if (Coord.getNextMaterializedTime() != null)
                    fNextMat = dateFormat.format(Coord.getNextMaterializedTime());
                if (Coord.getEndTime() != null) {
                    fecfin = dateOozie.parse(Coord.getEndTime().toString());
                    ffin = dateFormat.format(fecfin);
                }
                Properties prop = new Properties();
                InputStream is = null;

                try {
                    is = new FileInputStream("path.properties");
                    prop.load(is);
                } catch (IOException e) {
                    System.out.println(e.toString());
                }
                if (Coord.getUser().toString().equals(prop.getProperty("userServer"))) {
                    if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                        if (CoordName.equals("*") || Coord.getAppName().toUpperCase().contains(CoordName.toUpperCase())) {
                            if (Pais.equals("*") || Coord.getAppName().toUpperCase().contains(Pais.toUpperCase())) {

                                try {
                                    CoordinatorJob cj = wc.getCoordJobInfo(Coord.getId());
                                    //String confSID = get_confSID(cj.getConf());

                                    String depcol = get_dependenciaWF(cj.getConf(), cj.getAppName());
                                    String confSID = get_confSID(cj.getConf(), depcol);
                                    String tipo_WF = get_confTipoWF(cj.getConf(), depcol);

                                    if (p_wf.equals("*") || p_wf.equals(tipo_WF)) {
                                        if (SID.equals("*") || SID.trim().equals(confSID)) {
                                            WFJobCoord.add(new CoordinatorJobShow(Coord.getId(), Coord.getAppName(),
                                                    Coord.getStatus().toString(), fini, ffin,
                                                    utils.parseCronhourLocal(Coord.getFrequency()),
                                                    fNextMat));
                                        }
                                    }
                                } catch (OozieClientException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

        return WFJobCoord;
    }

    public List<getActionsWorkflow> getJobActions(String Jobid) {
        ArrayList<getActionsWorkflow> WFJobActions = new ArrayList<>();
        DateFormat dateOozie = new SimpleDateFormat(
                "EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        WorkflowJob w = null;
        try {
            w = wc.getJobInfo(Jobid);


        } catch (OozieClientException e) {
            e.printStackTrace();
        }

        Date fecstart, fecfin;
        String ffin = "", fini = "", Mensaje = "";


        for (WorkflowAction wfAction : w.getActions()) {

            try {
                Mensaje = "";
                fecstart = dateOozie.parse(wfAction.getStartTime().toString());
                fini = dateFormat.format(fecstart);
                if (wfAction.getEndTime() != null) {
                    fecfin = dateOozie.parse(wfAction.getEndTime().toString());
                    ffin = dateFormat.format(fecfin);
                }

                if (!wfAction.getStatus().toString().trim().equals("OK")) {
                    Mensaje = wfAction.getErrorMessage();
                } else if (wfAction.getName().toString().equals("Mail_spool") || wfAction.getName().toString().equals("Mail_putHDFS") || wfAction.getName().toString().equals("Mail_ALERTA")) {
                    Mensaje = get_error(wfAction.getConf());
                }
                //Mensaje=get_error(wfAction.getConf());

            } catch (ParseException e) {
                e.printStackTrace();
            }


            // WFJobActions.add(wfAction.getName() + "|" + wfAction.getType() + "|" + wfAction.getStatus() + "|" + fini + "|" + ffin + "|" + Mensaje);
            WFJobActions.add(new getActionsWorkflow(wfAction.getId() + "|" + wfAction.getName() + "|" + wfAction.getType() + "|" + wfAction.getStatus() + "|" + fini + "|" + ffin + "|" + Mensaje));


        }
        return WFJobActions;
    }

    private String get_error(String xml) {
        String vError = "";
        SAXBuilder saxBuilder = new SAXBuilder();
        try {
            Document doc = saxBuilder.build(new StringReader(xml));
            Element email = doc.getRootElement();
            List<Element> channelChildren = email.getChildren();
            Element channelChild = channelChildren.get(2);
            vError = channelChild.getText();

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return vError;

    }

    public String get_registros(String N_wf, String fdate2, String fdate3) {
        String cant = "";
        final String fdate = fdate2.substring(0, 8);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.matches(".*" + fdate + ".*");
            }
        };

        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        // String Path_f = "/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/reg/" + N_wf;
        String Path_f = prop.getProperty("home").concat("/INGESTAS_BALAM/datos_fs/reg/") + N_wf;
        //String Path_f = "C:\\Users\\escc_\\Documents\\DESARROLLOS\\2018\\EXTRACTORES BIG DATA\\datos_fs\\reg\\" + N_wf;

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        SimpleDateFormat formatFile = new SimpleDateFormat("yyyyMMddHHmmss");

        Date pini = null, pfin = null, dfile;
        try {
            pini = format.parse(fdate2);
            pfin = format.parse(fdate3);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        File folder = new File(Path_f);
        File[] listOfFiles = folder.listFiles(filter);

        String Narch = "";
        if (listOfFiles != null)
            for (int i = 0; i <= listOfFiles.length - 1; i++) {
                Narch = listOfFiles[i].getName();
                try {
                    dfile = formatFile.parse(Narch.substring(4, 18));
                    if ((dfile.after(pini) || dfile.equals(pini)) && (dfile.before(pfin) || dfile.equals(pfin))) {
                        break;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

        StringBuilder contentBuilder = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(Path_f + "/" + Narch));
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                contentBuilder.append(sCurrentLine).append("\n");
            }
            cant = contentBuilder.toString();
        } catch (IOException e) {
            return "";
        }

        return cant;
    }

    public class WorkflowJobShow {


        public WorkflowJobShow(String id, String name, String status, String startDate, String endDate, String alert, String error, String progress, List<WorkflowAction> actions, int succeeded, int succeed_alert, int suspended, int killed, int running, int prep, int failed, List<WfBalamBitacoraEjecucion> bitacora) {
            this.id = id;
            this.name = name;
            this.status = status;
            this.startDate = startDate;
            this.endDate = endDate;
            this.alert = alert;
            this.error = error;
            this.progress = progress;
            this.actions = actions;
            this.succeeded = succeeded;
            this.succeed_alert = succeed_alert;
            this.suspended = suspended;
            this.killed = killed;
            this.running = running;
            this.prep = prep;
            this.failed = failed;
            this.bitacora = bitacora;
        }

        private String id;
        private String name;
        private String status;
        private String startDate;
        private String endDate;
        private String alert;
        private String error;
        private String progress;
        private List<WorkflowAction> actions;
        private List<WfBalamBitacoraEjecucion> bitacora;
        private int succeeded = 0;
        private int succeed_alert = 0;
        private int suspended = 0;
        private int killed = 0;
        private int running = 0;
        private int prep = 0;
        private int failed = 0;


        public int getSucceeded() {
            return succeeded;
        }

        public void setSucceeded(int succeeded) {
            this.succeeded = succeeded;
        }

        public int getSucceed_alert() {
            return succeed_alert;
        }

        public void setSucceed_alert(int succeed_alert) {
            this.succeed_alert = succeed_alert;
        }

        public int getSuspended() {
            return suspended;
        }

        public void setSuspended(int suspended) {
            this.suspended = suspended;
        }

        public int getKilled() {
            return killed;
        }

        public void setKilled(int killed) {
            this.killed = killed;
        }

        public int getRunning() {
            return running;
        }

        public void setRunning(int running) {
            this.running = running;
        }

        public int getPrep() {
            return prep;
        }

        public void setPrep(int prep) {
            this.prep = prep;
        }

        public int getFailed() {
            return failed;
        }

        public void setFailed(int failed) {
            this.failed = failed;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }

        public String getAlert() {
            return alert;
        }

        public void setAlert(String alert) {
            this.alert = alert;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public String getProgress() {
            return progress;
        }

        public void setProgress(String progress) {
            this.progress = progress;
        }

        public List<WorkflowAction> getActions() {
            return actions;
        }

        public void setActions(List<WorkflowAction> actions) {
            this.actions = actions;
        }

        public List<WfBalamBitacoraEjecucion> getBitacora() {
            return bitacora;
        }

        public void setBitacora(List<WfBalamBitacoraEjecucion> bitacora) {
            this.bitacora = bitacora;
        }
    }


    public class CoordinatorJobShow {
        private String id;
        private String name;
        private String status;
        private String frequency;
        private String startDate;
        private String endDate;
        private String fNextmat;

        public CoordinatorJobShow(String id, String name, String status, String startDate, String endDate, String frequency, String fNextmat) {
            this.id = id;
            this.name = name;
            this.status = status;
            this.frequency = frequency;
            this.startDate = startDate;
            this.endDate = endDate;
            this.fNextmat = fNextmat;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getFrequency() {
            return frequency;
        }

        public void setFrequency(String frequency) {
            this.frequency = frequency;
        }

        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }

        public String getfNextmat() {
            return fNextmat;
        }

        public void setfNextmat(String fNextmat) {
            this.fNextmat = fNextmat;
        }
    }

    public class getActionsWorkflow {
        public getActionsWorkflow(String id) {

            this.id = id;
            this.name = name;
            this.type = type;
            this.status = status;
            this.ffini = ffini;
            this.ffin = ffin;
            this.message = message;
        }

        public String id;
        public String name;
        public String type;
        public String status;
        public String ffini;
        public String ffin;
        public String message;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getFfini() {
            return ffini;
        }

        public void setFfini(String ffini) {
            this.ffini = ffini;
        }

        public String getFfin() {
            return ffin;
        }

        public void setFfin(String ffin) {
            this.ffin = ffin;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    public Status countByStatus(String p_fini, String p_ffin, String status, String WFname, String Pais, String sid, String p_wf) {
        int inicio = 1;
        boolean flag2 = false;

        List<WorkflowJob> workflowList = null;
        Date fecstart = null;
        String fini = null;
        Date fecfin = null;
        String ffin = null;

        int succeeded = 0;
        int failed = 0;
        int running = 0;
        int killed = 0;
        int suspended = 0;
        int prep = 0;
        int succeededAlert = 0;
        DateFormat formatFilter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Date dp_fini = null;
        Date dp_ffin = null;
        Date dpo_fstart = null;


        Status p_status = new Status();
        try {


            if (p_fini == null || p_ffin == null) {
                dp_fini = formatFilter.parse("2018-11-01");
                dp_ffin = formatFilter.parse("2018-11-12");
            } else {
                dp_fini = formatFilter.parse(p_fini);
                dp_ffin = formatFilter.parse(p_ffin);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        //DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy");
        DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        while (flag2 == false) {
            try {
                workflowList = wc.getJobsInfo("", inicio, 30);

                for (WorkflowJob wf : workflowList) {

                    CoordinatorJob cj = wc.getCoordJobInfo(wf.getId());

                    String depcol = get_dependenciaWF(cj.getConf(), wf.getAppName());

                    String confSID = get_confSID(cj.getConf(), depcol);
                    String tipo_WF = get_confTipoWF(cj.getConf(), depcol);

                    if (wf.getStartTime() != null) {
                        fecstart = dateOozie.parse(wf.getStartTime().toString());
                        dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                        fini = dateFormat.format(fecstart);
                    }

                    if (wf.getEndTime() != null) {
                        fecfin = dateOozie.parse(wf.getEndTime().toString());
                        ffin = dateFormat.format(fecfin);
                    }
                    Properties prop = new Properties();
                    InputStream is = null;

                    try {
                        is = new FileInputStream("path.properties");
                        prop.load(is);
                    } catch (IOException e) {
                        System.out.println(e.toString());
                    }

                    if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                        if ((status != null && status.equals("*")) || (status != null && status.equals(wf.getStatus().toString().trim()))) {
                            if (WFname.equals("*") || wf.getAppName().toUpperCase().contains(WFname.toUpperCase())) {
                                if (Pais.equals("*") || wf.getAppName().toUpperCase().contains(Pais.toUpperCase())) {

                                    if (wf.getUser().toString().equals(prop.getProperty("userServer"))) {

                                        if (p_wf.equals("*") || p_wf.equals(tipo_WF)) {

                                            if (sid.equals("*") || sid.trim().equals(confSID)) {


                                                boolean flag = false;

                                                switch (wf.getStatus()) {
                                                    case SUCCEEDED:
                                                        WorkflowJob w = wc.getJobInfo(wf.getId());
                                                        for (WorkflowAction wfAction : w.getActions()) {
                                                            if (wfAction.getName().trim().equals("Mail_ALERTA")) {
                                                                flag = true;
                                                                break;
                                                            }
                                                        }
                                                        ///////////////___ SE AGREGO ESTE BLOQUE DE CODIGO LA FECHA 19/07/2019___/////////////////////
                                                        if (flag == false) {
                                                            if (tipo_WF.equals("spool")) {
                                                                String v_fechafin;
                                                                if (wf.getEndTime() == null)
                                                                    v_fechafin = null;
                                                                else
                                                                    v_fechafin = wf.getEndTime().toString();
                                                                String Alert_cmp = get_compareRows(wf.getAppName(), wf.getStartTime().toString(), v_fechafin);
                                                                if (!Alert_cmp.equals("-1")) {
                                                                    flag = true;
                                                                }
                                                            }
                                                        }
                                                        /////////////___FINALIZA BLOQUE DE CODIGO____//////////
                                                        if (flag) {
                                                            succeededAlert++;
                                                        } else {
                                                            succeeded++;
                                                        }
                                                        break;
                                                    case FAILED:
                                                        failed++;
                                                        break;
                                                    case RUNNING:
                                                        running++;
                                                        break;
                                                    case KILLED:
                                                        killed++;
                                                        break;
                                                    case SUSPENDED:
                                                        suspended++;
                                                        break;
                                                    case PREP:
                                                        prep++;
                                                        break;
                                                }//if sid
                                            }//if tipo
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (dpo_fstart.before(dp_fini) || dpo_fstart.equals(dp_fini)) {
                        flag2 = true;
                        break;
                    }
                }
                inicio += 30;

                if (workflowList.size() <= 0) {
                    flag2 = true;
                }
                p_status.setSucceeded(succeeded);
                p_status.setFailed(failed);
                p_status.setRunning(running);
                p_status.setKilled(killed);
                p_status.setSuspended(suspended);
                p_status.setPrep(prep);
                p_status.setSucceededAlert(succeededAlert);

            } catch (OozieClientException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
        return p_status;
    }

    public class Status {
        private int succeeded = 0;
        private int failed = 0;
        private int running = 0;
        private int killed = 0;
        private int suspended = 0;
        private int prep = 0;
        private int succeededAlert = 0;

        public int getSucceeded() {
            return succeeded;
        }

        public void setSucceeded(int succeeded) {
            this.succeeded = succeeded;
        }

        public int getFailed() {
            return failed;
        }

        public void setFailed(int failed) {
            this.failed = failed;
        }

        public int getRunning() {
            return running;
        }

        public void setRunning(int running) {
            this.running = running;
        }

        public int getKilled() {
            return killed;
        }

        public void setKilled(int killed) {
            this.killed = killed;
        }

        public int getSuspended() {
            return suspended;
        }

        public void setSuspended(int suspended) {
            this.suspended = suspended;
        }

        public int getPrep() {
            return prep;
        }

        public void setPrep(int prep) {
            this.prep = prep;
        }

        public int getSucceededAlert() {
            return succeededAlert;
        }

        public void setSucceededAlert(int succeededAlert) {
            this.succeededAlert = succeededAlert;
        }
    }


    /*public String get_confSID(String xml) {
        String vSID = "";
        SAXBuilder saxBuilder = new SAXBuilder();
        try {
            Document doc = saxBuilder.build(new StringReader(xml));
            Element property = doc.getRootElement();
            List<Element> channelChildren = property.getChildren();

            for (int i = 0; i <= channelChildren.size() - 1; i++) {
                Element channelChild = channelChildren.get(i);
                String p_property = channelChild.getChild("name").getText();
                if (p_property.equals("sid")) {
                    vSID = channelChild.getChild("value").getText();
                }
            }

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return vSID;
    }*/

    public String get_confSID(String xml, String poscol) {
        String vSID = "";
        SAXBuilder saxBuilder = new SAXBuilder();
        try {
            Document doc = saxBuilder.build(new StringReader(xml));
            Element property = doc.getRootElement();
            List<Element> channelChildren = property.getChildren();

            for (int i = 0; i <= channelChildren.size() - 1; i++) {
                Element channelChild = channelChildren.get(i);
                String p_property = channelChild.getChild("name").getText();
                if (p_property.equals("sid" + poscol)) {
                    vSID = channelChild.getChild("value").getText();
                }
            }

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return vSID;
    }


    /*public String get_confTipoWF(String xml) {
        String vSID = "";
        SAXBuilder saxBuilder = new SAXBuilder();
        try {
            Document doc = saxBuilder.build(new StringReader(xml));
            Element property = doc.getRootElement();
            List<Element> channelChildren = property.getChildren();

            for (int i = 0; i <= channelChildren.size() - 1; i++) {
                Element channelChild = channelChildren.get(i);
                String p_property = channelChild.getChild("name").getText();
                if (p_property.equals("tipo_WF")) {
                    vSID = channelChild.getChild("value").getText();
                }
            }

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return vSID;
    }*/

    public String get_confTipoWF(String xml, String poscol) {
        String vSID = "";
        SAXBuilder saxBuilder = new SAXBuilder();
        try {
            Document doc = saxBuilder.build(new StringReader(xml));
            Element property = doc.getRootElement();
            List<Element> channelChildren = property.getChildren();

            for (int i = 0; i <= channelChildren.size() - 1; i++) {
                Element channelChild = channelChildren.get(i);
                String p_property = channelChild.getChild("name").getText();
                if (p_property.equals("tipo_WF" + poscol)) {
                    vSID = channelChild.getChild("value").getText();
                }
            }

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return vSID;
    }

    public String get_bitacora(String NombreWF, String Fechaini, String Fechafin) {
        String p_estado = "";
        Connection conn = null;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

        String ffin = "";
        String fini = "";
        try {
            fini = df.format(dateOozie.parse(Fechaini));
            if (Fechafin == null) {
                ffin = df.format(new Date());
            } else {
                ffin = df.format(dateOozie.parse(Fechafin));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //System.out.println(Fechafin);
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
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select fecha,nombre_wf,hora,estado,descripcion from wf_balam_bitacora_ejecucion where nombre_wf=? and hora between ? and ?";
            stmt = (PreparedStatement) conn.prepareStatement(query);
            stmt.setString(1, NombreWF);
            stmt.setString(2, fini);
            stmt.setString(3, ffin);
            rs = stmt.executeQuery();
            String estado = "", fecha = "", nombre_wf = "", hora = "", descripcion = "";
            while (rs.next()) {
                fecha = rs.getString("fecha");
                nombre_wf = rs.getString("nombre_wf");
                hora = rs.getString("hora");
                estado = rs.getString("estado");
                descripcion = rs.getString("descripcion");
            }
            p_estado = estado;
            stmt.close();


        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();
            return "Error de conexion en Mysql";
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return p_estado;

    }

    public List<WfBalamBitacoraEjecucion> get_bitacora2(String NombreWF, String Fechaini, String Fechafin) {

        ArrayList<WfBalamBitacoraEjecucion> bitacoraList = new ArrayList<>();


        String p_estado = "";
        Connection conn = null;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

        String ffin = "";
        String fini = "";
        logger.info(Fechaini + " " + Fechafin);
        try {
            fini = df.format(dateOozie.parse(Fechaini));
            if (Fechafin == null) {
                ffin = df.format(new Date());
            } else {
                ffin = df.format(dateOozie.parse(Fechafin));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //System.out.println(Fechafin);
        try

        {
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
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select fecha,nombre_wf,hora,estado,descripcion from wf_balam_bitacora_ejecucion where nombre_wf=? and hora between ? and ?";
            stmt = (PreparedStatement) conn.prepareStatement(query);
            stmt.setString(1, NombreWF);
            stmt.setString(2, fini);
            stmt.setString(3, ffin);
            rs = stmt.executeQuery();
            String estado = "", fecha = "", nombre_wf = "", hora = "", descripcion = "";

            logger.info(NombreWF + fini + ffin);
            while (rs.next()) {

                WfBalamBitacoraEjecucion wfbitacoraEjecucion = new WfBalamBitacoraEjecucion();

                //if para la fecha cuando es null
                wfbitacoraEjecucion.setFecha((rs.getString("fecha") != null ? rs.getString("fecha") : ""));
                wfbitacoraEjecucion.setNombre_wf(rs.getString("nombre_wf"));
                wfbitacoraEjecucion.setHora(rs.getString("hora"));
                wfbitacoraEjecucion.setEstado(rs.getString("estado"));
                wfbitacoraEjecucion.setDescripcion(rs.getString("descripcion"));
                bitacoraList.add(wfbitacoraEjecucion);

            }
            stmt.close();
        } catch (
                SQLException ex) {
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
        return bitacoraList;
    }


    public String get_dependenciaWF(String xml, String wfName) {
        String indcol = "";
        String dep = null;
        SAXBuilder saxBuilder = new SAXBuilder();
        try {
            Document doc = saxBuilder.build(new StringReader(xml));
            Element property = doc.getRootElement();
            List<Element> channelChildren = property.getChildren();

            for (int i = 0; i <= channelChildren.size() - 1; i++) {
                Element channelChild = channelChildren.get(i);
                String p_property = channelChild.getChild("name").getText();
                if (p_property.equals("nombreWFDep")) {
                    dep = channelChild.getChild("value").getText();
                }
            }

            if (dep != null) {
                Connection conn = null;
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
                    String query = "select posicion,col from wf_flujos_dependencias where nombre=? and coordinator=?";
                    stmt = conn.prepareStatement(query);
                    stmt.setString(1, dep);
                    stmt.setString(2, wfName);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        indcol = rs.getString("posicion") + rs.getString("col");
                    }

                    stmt.close();


                } catch (SQLException ex) {
                    // handle any errors
                    ex.printStackTrace();
                    return "Error de conexion en Mysql";
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return indcol;
    }

    public List<String> getWFbyCountry(String v_pais) {
        ArrayList<String> wf_flujos = new ArrayList<String>();


        String[] arr_pais = v_pais.split(",");
        String p_fini, p_ffin;

        int pend = 0, term = 0, fall = 0;
        Connection conn = null;
        Date fecstart = null;

        DateFormat formatFilter = new SimpleDateFormat("yyyy-MM-dd");
        Date dp_fini = null;
        Date dp_ffin = null;
        Date dpo_fstart = null;

        p_fini = formatFilter.format(new Date());
        p_ffin = formatFilter.format(new Date());

        for (int k = 0; k <= arr_pais.length - 1; k++) {

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

                java.sql.PreparedStatement stmt = null;
                ResultSet rs = null;
                String query = "select name from  flow  where  country= ? ";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, arr_pais[k]);
                rs = stmt.executeQuery();

                dp_fini = formatFilter.parse(p_fini);
                dp_ffin = formatFilter.parse(p_ffin);
                DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


                while (rs.next()) {
                    String coordName = rs.getString("name");

                    List<CoordinatorJob> CoordJob = wc.getCoordJobsInfo("Name=" + coordName + ";Status=RUNNING", 1, 1);
                    if (CoordJob.size() > 0) {

                        List<WorkflowJob> WF = wc.getJobsInfo("Name=" + coordName, 1, 1);

                        if (WF.size() > 0) {

                            String[] freq = CoordJob.get(0).getFrequency().split(" ");


                            if (freq[2].equals("*") && freq[3].equals("*") && freq[4].equals("*")) {
                                if (WF.get(0).getStartTime() != null) {
                                    fecstart = dateOozie.parse(WF.get(0).getStartTime().toString());
                                    dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                                }

                                if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                                    if (WF.get(0).getStatus() == WorkflowJob.Status.SUCCEEDED)
                                        term++;
                                    else if (WF.get(0).getStatus() == WorkflowJob.Status.FAILED)
                                        fall++;
                                    else if (WF.get(0).getStatus() == WorkflowJob.Status.KILLED)
                                        fall++;
                                    else if (WF.get(0).getStatus() == WorkflowJob.Status.SUSPENDED)
                                        fall++;
                                    else
                                        pend++;
                                } else {
                                    pend++;
                                }
                            } else {
                                if (WF.get(0).getStartTime() != null) {
                                    fecstart = dateOozie.parse(WF.get(0).getStartTime().toString());
                                    dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                                }
                                Date FecNextMat = dateOozie.parse(CoordJob.get(0).getNextMaterializedTime().toString());
                                Date FecNextMatLoc = formatFilter.parse(formatFilter.format(FecNextMat));

                                if (FecNextMatLoc.equals(dp_fini)) {
                                    if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                                        if (WF.get(0).getStatus() == WorkflowJob.Status.SUCCEEDED)
                                            term++;
                                        else if (WF.get(0).getStatus() == WorkflowJob.Status.FAILED)
                                            fall++;
                                        else if (WF.get(0).getStatus() == WorkflowJob.Status.KILLED)
                                            fall++;
                                        else if (WF.get(0).getStatus() == WorkflowJob.Status.SUSPENDED)
                                            fall++;
                                        else
                                            pend++;
                                    } else {
                                        pend++;
                                    }
                                }
                            }

                        } else {
                            pend++;
                        }
                        //System.out.println(WF.get(0).getAppName() + "|" + WF.get(0).getStatus() + "|" + WF.get(0).getStartTime());
                    }


                }

                wf_flujos.add(arr_pais[k] + "|" + term + "|" + pend + "|" + fall);
                pend = 0;
                term = 0;
                fall = 0;
                //System.out.println("Finalizado: " + term);
                //System.out.println("Pendiente: " + pend);
                //System.out.println("Fallidos: " + fall);


            } catch (SQLException ex) {
                ex.printStackTrace();

            } /*catch (OozieClientException e) {
            e.printStackTrace();
        }*/ catch (OozieClientException e) {
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

        }
        return wf_flujos;
    }

    public List<String> getWFbyConn() {
        ArrayList<String> wf_flujos = new ArrayList<String>();
        java.sql.Connection conn = null;
        String conexiones = "";


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
  /*                  DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");
*/

            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select id,CONNECTION_NAME from connection a";
            stmt = conn.prepareStatement(query);

            rs = stmt.executeQuery();

            while (rs.next()) {
                conexiones += rs.getString("connection_name") + "|" + rs.getString("id") + ",";
            }

        } catch (SQLException ex) {
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

        conexiones = conexiones.substring(0, conexiones.length() - 1);
        String[] arr_conn = conexiones.split(",");

        try {

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }

            for (int j = 0; j <= arr_conn.length - 1; j++) {
                String[] arr_connid = arr_conn[j].split("\\|");

                conn = DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                       /* DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                                "user=ingestas_balam&password=Ingestas123$");*/


                java.sql.PreparedStatement stmt = null;
                ResultSet rs = null;
                String query = "select name from  flow  where  connection_id= ? ";
                stmt = conn.prepareStatement(query);
                stmt.setInt(1, Integer.parseInt(arr_connid[1]));
                rs = stmt.executeQuery();

                String p_fini, p_ffin;

                int pend = 0, term = 0, fall = 0;
                Date fecstart = null;

                DateFormat formatFilter = new SimpleDateFormat("yyyy-MM-dd");
                Date dp_fini = null;
                Date dp_ffin = null;
                Date dpo_fstart = null;


                p_fini = formatFilter.format(new Date());
                p_ffin = formatFilter.format(new Date());

                dp_fini = formatFilter.parse(p_fini);
                dp_ffin = formatFilter.parse(p_ffin);
                DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

                while (rs.next()) {
                    ///////
                    String coordName = rs.getString("name");

                    List<CoordinatorJob> CoordJob = wc.getCoordJobsInfo("Name=" + coordName + ";Status=RUNNING", 1, 1);
                    if (CoordJob.size() > 0) {

                        List<WorkflowJob> WF = wc.getJobsInfo("Name=" + coordName, 1, 1);

                        if (WF.size() > 0) {

                            String[] freq = CoordJob.get(0).getFrequency().split(" ");


                            if (freq[2].equals("*") && freq[3].equals("*") && freq[4].equals("*")) {
                                if (WF.get(0).getStartTime() != null) {
                                    fecstart = dateOozie.parse(WF.get(0).getStartTime().toString());
                                    dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                                }

                                if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                                    if (WF.get(0).getStatus() == WorkflowJob.Status.SUCCEEDED)
                                        term++;
                                    else if (WF.get(0).getStatus() == WorkflowJob.Status.FAILED)
                                        fall++;
                                    else if (WF.get(0).getStatus() == WorkflowJob.Status.KILLED)
                                        fall++;
                                    else if (WF.get(0).getStatus() == WorkflowJob.Status.SUSPENDED)
                                        fall++;
                                    else
                                        pend++;
                                } else {
                                    pend++;
                                }
                            } else {
                                if (WF.get(0).getStartTime() != null) {
                                    fecstart = dateOozie.parse(WF.get(0).getStartTime().toString());
                                    dpo_fstart = formatFilter.parse(formatFilter.format(fecstart));
                                }
                                Date FecNextMat = dateOozie.parse(CoordJob.get(0).getNextMaterializedTime().toString());
                                Date FecNextMatLoc = formatFilter.parse(formatFilter.format(FecNextMat));

                                if (FecNextMatLoc.equals(dp_fini)) {
                                    if ((dpo_fstart.after(dp_fini) || dpo_fstart.equals(dp_fini)) && (dpo_fstart.before(dp_ffin) || dpo_fstart.equals(dp_ffin))) {
                                        if (WF.get(0).getStatus() == WorkflowJob.Status.SUCCEEDED)
                                            term++;
                                        else if (WF.get(0).getStatus() == WorkflowJob.Status.FAILED)
                                            fall++;
                                        else if (WF.get(0).getStatus() == WorkflowJob.Status.KILLED)
                                            fall++;
                                        else if (WF.get(0).getStatus() == WorkflowJob.Status.SUSPENDED)
                                            fall++;
                                        else
                                            pend++;
                                    } else {
                                        pend++;
                                    }
                                }
                            }

                        } else {
                            pend++;
                        }
                        //System.out.println(WF.get(0).getAppName() + "|" + WF.get(0).getStatus() + "|" + WF.get(0).getStartTime());
                    }
                    ////////////
                }

                wf_flujos.add(arr_connid[0] + "|" + term + "|" + pend + "|" + fall);
                pend = 0;
                term = 0;
                fall = 0;

            }

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

        return wf_flujos;
    }

    public List<Wf_bundles> list_dependences() {
        ArrayList<Wf_bundles> Wf_bundlesList = new ArrayList<>();

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        Date ffin = null, fini = null;

        Connection conn = null;
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
  /*                  DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");
*/

            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select *from  wf_bundles ";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();


            while (rs.next()) {
                String query2 = "select coordinator from wf_flujos_dependencias where nombre=? and posicion=1 ";
                java.sql.PreparedStatement stmt2 = conn.prepareStatement(query2);
                stmt2.setString(1, rs.getString("nombre"));
                ResultSet rs2 = stmt2.executeQuery();
                rs2.next();
                List<BundleJob> BJ = wc.getBundleJobsInfo("Name=" + rs.getString("nombre") + ";Status=RUNNING", 1, 1);
                BundleJob BJa = wc.getBundleJobInfo(BJ.get(0).getId());
                for (CoordinatorJob ca : BJa.getCoordinators()) {
                    if (ca.getAppName().trim().toUpperCase().equals(rs2.getString("coordinator").toUpperCase())) {
                        CoordinatorJob coord = wc.getCoordJobInfo(ca.getId());
                        WorkflowJob work = wc.getJobInfo(coord.getActions().get(coord.getActions().size() - 1).getExternalId());
                        fini = work.getStartTime();
                        break;
                    }
                }
                List<WorkflowJob> WF = wc.getJobsInfo("Name=" + rs2.getString("coordinator"), 1, 1);
                /////////////////////
                String query3 = "select coordinator from wf_flujos_dependencias where nombre=? and posicion=max";
                java.sql.PreparedStatement stmt3 = conn.prepareStatement(query3);
                stmt3.setString(1, rs.getString("nombre"));
                ResultSet rs3 = stmt3.executeQuery();
                while (rs3.next()) {
                    for (CoordinatorJob ca : BJa.getCoordinators()) {
                        if (ca.getAppName().trim().toUpperCase().equals(rs3.getString("coordinator").toUpperCase())) {
                            CoordinatorJob coord = wc.getCoordJobInfo(ca.getId());
                            WorkflowJob work = wc.getJobInfo(coord.getActions().get(coord.getActions().size() - 1).getExternalId());
                            if (work.getEndTime() != null) {
                                if (ffin == null) {
                                    ffin = work.getEndTime();
                                } else {
                                    if (ffin.before(work.getEndTime())) {
                                        ffin = work.getEndTime();
                                    }
                                }
                            }
                            break;
                        }
                    }

                }
                ////////////////////
                Wf_bundlesList.add(new Wf_bundles(rs.getString("nombre"), df.format(dateOozie.parse(fini.toString())), df.format(dateOozie.parse(ffin.toString())), rs.getString("frecuencia"), rs.getString("bundle_id"), rs.getString("dia_mes"), rs.getString("dia_semana"), rs.getString("hora"), rs.getString("minuto"), rs.getString("mes"), rs.getString("cadena")));
                // Wf_bundlesList.add(rs.getString("nombre") + "|" + df.format(dateOozie.parse(fini.toString())) + "|" + df.format(dateOozie.parse(ffin.toString())) + "|" + rs.getString("frecuencia") + "|" + rs.getString("bundle_id") + "|" + rs.getString("dia_mes") + "|" + rs.getString("dia_semana") + "|" + rs.getString("hora") + "|" + rs.getString("minuto") + "|" + rs.getString("mes") + "|" + rs.getString("cadena") + "|" + rs.getString("status"));
                ffin = null;
                fini = null;
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
        return Wf_bundlesList;

    }

//

   /* public List<String> getflows_dependences(String p_nombre) {
        ArrayList<String> Wf_flujos_dep = new ArrayList<String>();

        java.sql.Connection conn = null;
        try {
            conn =
                    DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");


            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select *from  wf_flujos_dependencias  where  nombre= ? ";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, p_nombre);
            rs = stmt.executeQuery();


            List<BundleJob> bundleJob = wc.getBundleJobsInfo("Name=" + p_nombre + ";Status=RUNNING;Status=SUCCEEDED", 1, 1);
            BundleJob bj = wc.getBundleJobInfo(bundleJob.get(0).getId());

            int count = 0;
            DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaStart = null;
            Date fini = null, ffin = null;
            String sfini=null,sffin=null;
            String coordId = "", status = "";

            while (rs.next()) {
                status = "WAITING";
                List<WorkflowJob> workflowList = null;

                try {
                    for (CoordinatorJob job : bj.getCoordinators()) {
                        if (job.getAppName().trim().equals(rs.getString("coordinator"))) {
                            coordId = job.getId();
                            break;
                        }
                    }
                    CoordinatorJob coordJob = wc.getCoordJobInfo(coordId);

                    List<CoordinatorAction> action = coordJob.getActions();

                    if(action.size()>0) {
                        WorkflowJob wfj = wc.getJobInfo(action.get(action.size() - 1).getExternalId());
                        fini = wfj.getStartTime();
                        ffin = wfj.getEndTime();
                        sfini=df.format(dateOozie.parse(fini.toString()));
                        sffin=df.format(dateOozie.parse(ffin.toString()));
                    }else{
                        sfini=null;
                        sffin=null;
                    }

                    if (count == 0) {
                        if (action.size() > 0)
                            fechaStart = dateOozie.parse(action.get(action.size() - 1).getNominalTime().toString());
                        count++;
                    }

                    Date feccomp = null;
                    if (action.size() > 0)
                        feccomp = dateOozie.parse(action.get(action.size() - 1).getNominalTime().toString());

                    if (action.size() > 0)
                        if (feccomp.equals(fechaStart) || feccomp.after(fechaStart))
                            status = action.get(action.size() - 1).getStatus().toString();


                } catch (OozieClientException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                Wf_flujos_dep.add(rs.getString("coordinator") + ";" + rs.getInt("posicion") + ";" + rs.getInt("col") + ";" + status + ";" + sfini + ";" + sffin);
                fini = null;
                ffin = null;
                sfini=null;
                sffin=null;
            }
            stmt.close();
            conn.close();

        } catch (SQLException ex) {
            ex.printStackTrace();

        } catch (OozieClientException e) {
            e.printStackTrace();
        }  finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return Wf_flujos_dep;

    }*/

    public List<String> getflows_dependences(String p_nombre) {
        ArrayList<String> Wf_flujos_dep = new ArrayList<String>();

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


            java.sql.PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select *from  wf_flujos_dependencias  where  nombre= ? ";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, p_nombre);
            rs = stmt.executeQuery();


            List<BundleJob> bundleJob = wc.getBundleJobsInfo("Name=" + p_nombre + ";Status=RUNNING;Status=SUCCEEDED", 1, 1);
            BundleJob bj = wc.getBundleJobInfo(bundleJob.get(0).getId());

            int count = 0;
            DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaStart = null;
            Date fini = null, ffin = null;
            String sfini = "", sffin = "";
            String coordId = "", status = "";

            while (rs.next()) {
                status = "WAITING";
                List<WorkflowJob> workflowList = null;

                try {
                    for (CoordinatorJob job : bj.getCoordinators()) {
                        if (job.getAppName().trim().equals(rs.getString("coordinator"))) {
                            coordId = job.getId();
                            break;
                        }
                    }
                    CoordinatorJob coordJob = wc.getCoordJobInfo(coordId);

                    List<CoordinatorAction> action = coordJob.getActions();

                    if (action.size() > 0) {
                        if (action.get(action.size() - 1).getExternalId() != null) {
                            WorkflowJob wfj = wc.getJobInfo(action.get(action.size() - 1).getExternalId());
                            fini = wfj.getStartTime();
                            ffin = wfj.getEndTime();
                            sfini = df.format(dateOozie.parse(fini.toString()));
                            if (ffin != null)
                                sffin = df.format(dateOozie.parse(ffin.toString()));
                        }
                    } else {
                        sfini = null;
                        sffin = null;
                    }

                    if (count == 0) {
                        if (action.size() > 0)
                            fechaStart = dateOozie.parse(action.get(action.size() - 1).getNominalTime().toString());
                        count++;
                    }

                    Date feccomp = null;
                    if (action.size() > 0)
                        feccomp = dateOozie.parse(action.get(action.size() - 1).getNominalTime().toString());

                    if (action.size() > 0)
                        if (feccomp.equals(fechaStart) || feccomp.after(fechaStart))
                            status = action.get(action.size() - 1).getStatus().toString();


                } catch (OozieClientException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                Wf_flujos_dep.add(rs.getString("coordinator") + ";" + rs.getInt("posicion") + ";" + rs.getInt("col") + ";" + status + ";" + sfini + ";" + sffin);
                fini = null;
                ffin = null;
                sfini = "";
                sffin = "";
            }
            stmt.close();
            conn.close();

        } catch (SQLException ex) {
            ex.printStackTrace();

        } catch (OozieClientException e) {
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
        return Wf_flujos_dep;

    }


    public String get_compareRows(String NombreWF, String Fechaini, String Fechafin) {
        String p_estado = "";
        Connection conn = null;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        DateFormat dateOozie = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

        String ffin = "";
        String fini = "";
        try {
            fini = df.format(dateOozie.parse(Fechaini));
            if (Fechafin == null) {
                ffin = df.format(new Date());
            } else {
                ffin = df.format(dateOozie.parse(Fechafin));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
//System.out.println(Fechafin);
        try {
            conn =
                    DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");

            // Do something with the Connection
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select registros from wf_balam_bitacora_ejecucion where nombre_wf=? and hora between ? and ? and estado=?";
            stmt = (PreparedStatement) conn.prepareStatement(query);
            stmt.setString(1, NombreWF);
            stmt.setString(2, fini);
            stmt.setString(3, ffin);
            stmt.setString(4,"Inicio 1.1");
            rs = stmt.executeQuery();
            String registros="";
            int reg_bd=-1;
            while (rs.next()) {
                registros = rs.getString("registros");

            }
            if(registros!=null)
                if(registros.equals(""))
                    reg_bd=-1;
                else
                    reg_bd=Integer.parseInt(registros.trim());
            else
                reg_bd=-1;

            ///////////////////////////////
            stmt = null;
            rs = null;
            query = "select registros from wf_balam_bitacora_ejecucion where nombre_wf=? and hora between ? and ? and estado=?";
            stmt = (PreparedStatement) conn.prepareStatement(query);
            stmt.setString(1, NombreWF);
            stmt.setString(2, fini);
            stmt.setString(3, ffin);
            stmt.setString(4,"Hdfs inicio 1.2");
            rs = stmt.executeQuery();
            registros="";
            int reg_ext=-1;
            while (rs.next()) {
                registros = rs.getString("registros");

            }
            if(registros!=null)
                if(registros.equals(""))
                    reg_ext=-1;
                else
                    reg_ext=Integer.parseInt(registros.trim());
            else
                reg_ext=-1;
            ///////////////////////////////
            stmt.close();
            // System.out.println(reg_bd+" - "+reg_ext);

            if(reg_bd!=-1 && reg_ext!=-1) {
                if (reg_bd == reg_ext)
                    p_estado = "-1";
                else
                    p_estado = String.valueOf(reg_bd);
            }else{
                p_estado = "-1";
            }

        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();
            return "-1";
        }finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return p_estado;

    }

}