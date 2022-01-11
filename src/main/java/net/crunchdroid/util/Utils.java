package net.crunchdroid.util;

import net.crunchdroid.model.Wf_bundles;
import net.crunchdroid.pojo.DependenciesFilter;
import net.crunchdroid.shell.hdfs.HdfsBalam;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    /*@Autowired
    private Environment env;*/


    Logger logger = LoggerFactory.getLogger(Utils.class);
    HdfsBalam hdfs = new HdfsBalam();
    private final static String DB_URL = "jdbc:oracle:thin:@//192.168.1.105:1521/MYORA";
    private final static String USER = "myuser";
    private final static String PASS = "mypwd";

    //--VARIABLES DE ENTORNO - PROPERTIES
  /*  @Value("${ingestas.home}")
    private String INGESTAS_HOME;

    @Value("${ingestas.remoteNode_putHDFS")
    private String REMOTE_NODE_PUTHDFS;
*/

    public static void main(String[] args) {
        Utils util = new Utils();
        util.setTest("ORACLE", "10.231.220.55", "1521", "mth", "ADM_SV", "y2k120318");

    }

    public String setTest(String type, String host, String port, String database, String user, String password) {
        String url = null;
        String forName = null;
        if ("ORACLE".equalsIgnoreCase(type)) {
            url = "jdbc:oracle:thin:@".concat(host).concat(":").concat(port).concat(":").concat(database);
            forName = "oracle.jdbc.driver.OracleDriver";
        } else if ("SQLSERVER".equalsIgnoreCase(type)) {
            url = "jdbc:microsoft:sqlserver://".concat(host).concat(":").concat(port).concat(";DatabaseName=").concat(database);
            forName = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
        } else if ("MYSQL".equalsIgnoreCase(type)) {
            url = "jdbc:mysql://".concat(host).concat(":").concat(port).concat("/").concat(database);
            forName = "com.mysql.jdbc.Driver";
        }

        return runTest(url, forName, user, password);
    }

    public String setTest(String type, net.crunchdroid.model.Connection connectionModel) {
        String url = null;
        String forName = null;
        if ("ORACLE".equalsIgnoreCase(type)) {
            if (connectionModel.getType().equalsIgnoreCase("SID")) {
                url = "jdbc:oracle:thin:@".concat(connectionModel.getHost()).concat(":").concat(connectionModel.getPort()).concat(":").concat(connectionModel.getSid());
                forName = "oracle.jdbc.driver.OracleDriver";
            } else {
                url = "jdbc:oracle:thin:@//".concat(connectionModel.getHost()).concat(":").concat(connectionModel.getPort()).concat("/").concat(connectionModel.getSid());
                forName = "oracle.jdbc.driver.OracleDriver";
            }
        } else if ("SQLSERVER".equalsIgnoreCase(type)) {
            url = "jdbc:microsoft:sqlserver://".concat(connectionModel.getHost()).concat(":").concat(connectionModel.getPort()).concat(";DatabaseName=").concat(connectionModel.getSid());
            forName = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
        } else if ("MYSQL".equalsIgnoreCase(type)) {
            url = "jdbc:mysql://".concat(connectionModel.getHost()).concat(":").concat(connectionModel.getPort()).concat("/").concat(connectionModel.getSid());
            forName = "com.mysql.jdbc.Driver";
        }
        return runTest(url, forName, connectionModel.getDbUser(), connectionModel.getPassword());
    }

    public String runTest(String url, String forname, String user, String password) {
        String result = "";
        Connection conn = null;
        try {
            Class.forName(forname);
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(url, user, password);
            result = "OK";
        } catch (Exception e) {
            result = e.getMessage();
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    result = e.getMessage();
                    e.printStackTrace();
                }
            }
        }
        return result;
    }


    /*STEVE*/


    public String Oracle_query_spool(String IP, String puerto, String sid, String Usuario, String password, String consulta, String path_spool, String nombreExtractor, String typeConexion) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return "Driver no encontrado";
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

        Connection connection = null;

        try {
            if (typeConexion.equalsIgnoreCase("SID")) {
                connection = DriverManager.getConnection(
                        "jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid, Usuario, password);
            } else {
                connection = DriverManager.getConnection(
                        "jdbc:oracle:thin:@//" + IP + ":" + puerto + "/" + sid, Usuario, password);
            }


            //validando consulta
            consulta = consulta.replace(";", "");
            Statement stmt = connection.createStatement();
            stmt.execute("explain plan for " + consulta);
            ResultSet rs = stmt.executeQuery("select plan_table_output from table(dbms_xplan.display())");

            System.out.println("quitando punto y coma");
            if (consulta.indexOf(';') == -1) {
                consulta = consulta + ";";
            }

            System.out.println("cierra conexiones");
            stmt.close();
            connection.close();

            ArrayList<Integer> list = new ArrayList<Integer>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }

            if (list.size() > 0) {
                ArrayList<String> variables = new ArrayList<String>();
                for (Integer i : list) {
                    String p_var = "";
                    char ch4 = '&';
                    char ch5 = '0';
                    char ch6 = '9';
                    for (int j = i; j < consulta.length(); j++) {
                        if ((consulta.charAt(j) >= ch5 && consulta.charAt(j) <= ch6) || consulta.charAt(j) == ch4)
                            p_var = p_var + consulta.charAt(j);
                        else
                            break;
                    }
                    variables.add(p_var);
                }
                Collections.sort(variables);

                int indice = 4;
                for (String i : variables) {
                    consulta = consulta.replace(i, "&" + indice);
                    indice++;
                }
            }
            /*ArrayList<Integer> list = new ArrayList<Integer>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }*/

            /*ArrayList<String> variables = new ArrayList<String>();
            for (Integer i : list) {
                String p_var = "";
                char ch = '\'';
                for (int j = i; j < consulta.length(); j++) {
                    if (consulta.charAt(j) == ch)
                        break;
                    p_var = p_var + consulta.charAt(j);
                }
                variables.add(p_var);
            }*/
            //Collections.sort(variables);

            /*int indice = 4;
            for (String i : variables) {
                consulta = consulta.replace(i, "&" + indice);
                indice++;
            }*/

            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }

            logger.info(prop.getProperty("home").concat("/INGESTAS_BALAM/config/sql/") + nombreExtractor + ".sql");
            logger.info("Generando Archivo");
            // String FILENAME = "/home/appbalam_ingestas/INGESTAS_BALAM/config/sql/" + nombreExtractor + ".sql";
            String FILENAME = prop.getProperty("home").concat("/INGESTAS_BALAM/config/sql/") + nombreExtractor + ".sql";
            System.out.println("filename: " + FILENAME);


            BufferedWriter bw = null;
            FileWriter fw = null;

            try {
                String content = "WHENEVER OSERROR EXIT 2\nWHENEVER SQLERROR EXIT 1\n";
                content = content + "set linesize 30000;\n";
                content = content + "set termout off;\n";
                content = content + "set heading off;\n";
                content = content + "set pagesize 0;\n";
                content = content + "set feedback off;\n";
                content = content + "set trims on;\n";
                content = content + "set echo off;\n";
                content = content + "set verify off;\n";
                content = content + "set trimspool on;\n";
                content = content + "set term off;\n";
                content = content + "set colsep '&2';\n";
                content = content + "alter session set nls_date_format = '&3';\n";
                content = content + "spool &1;\n\n";
                content = content + consulta + "\n\n";
                content = content + "spool off;\n\n";
                content = content + "exit;\n\n";
                content = content + "set termout on;\n";
                content = content + "set heading on;\n";

                fw = new FileWriter(FILENAME);
                bw = new BufferedWriter(fw);
                bw.write(content);

            } catch (IOException e) {
                logger.error(e.getMessage());
                //logger.error(e.getCause().toString());
                return e.getMessage();
            } finally {
                try {
                    if (bw != null)
                        bw.close();
                    if (fw != null)
                        fw.close();
                } catch (IOException ex) {
                    logger.error(ex.getMessage());
                    //logger.error(ex.getCause().toString());
                    return ex.getMessage();
                }
            }


        } catch (SQLException e) {
            logger.error(e.getMessage());
            //logger.error(e.getCause().toString());
            return e.getMessage();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return "OK";

    }

    public String Oracle_query_sqoop(String IP, String puerto, String sid, String Usuario, String password, String consulta, String path_spool, String nombreExtractor, String separador, String phdfs_dest, String p_fsize, String p_split, String p_mapers, String variables, String Tipo_tabla, String typeConexion) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return "Driver no encontrado";
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (InstantiationException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        Connection connection = null;

        try {
            if (typeConexion.equalsIgnoreCase("SID")) {
                connection = DriverManager.getConnection(
                        "jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid, Usuario, password);
            } else {
                connection = DriverManager.getConnection(
                        "jdbc:oracle:thin:@//" + IP + ":" + puerto + "/" + sid, Usuario, password);
            }

            logger.info("Validando consulta");
            consulta = consulta.replace(";", "");
            Statement stmt = connection.createStatement();
            stmt.execute("explain plan for " + consulta);
            ResultSet rs = stmt.executeQuery("select plan_table_output from table(dbms_xplan.display())");

            logger.info("Cerrando conexión");
            stmt.close();
            connection.close();

            String FILENAME = path_spool + nombreExtractor + ".txt";
            BufferedWriter bw = null;
            FileWriter fw = null;

            logger.info("Condiciones");
            if (!consulta.toUpperCase().contains("WHERE")) {
                consulta += " WHERE $CONDITIONS";
            } else {
                consulta = consulta + " AND $CONDITIONS";
            }

            ArrayList<Integer> list = new ArrayList<Integer>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }


            if (list.size() > 0) {
                ArrayList<String> p_variables = new ArrayList<String>();
                for (Integer i : list) {
                    String p_var = "";
                    char ch4 = '&';
                    char ch5 = '0';
                    char ch6 = '9';


                    for (int j = i; j < consulta.length(); j++) {
                        if ((consulta.charAt(j) >= ch5 && consulta.charAt(j) <= ch6) || consulta.charAt(j) == ch4)
                            p_var = p_var + consulta.charAt(j);
                        else
                            break;
                    }
                    p_variables.add(p_var);
                }
                Collections.sort(p_variables);

                String[] par_var = variables.split(",");
                int indice = 0;
                for (String i : p_variables) {
                    consulta = consulta.replace(i, par_var[indice]);
                    indice++;
                }
            }

            /*ArrayList<Integer> list = new ArrayList<Integer>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }

            ArrayList<String> p_variables = new ArrayList<String>();
            for (Integer i : list) {
                String p_var = "";
                char ch = '\'';
                for (int j = i; j < consulta.length(); j++) {
                    if (consulta.charAt(j) == ch)
                        break;
                    p_var = p_var + consulta.charAt(j);
                }
                p_variables.add(p_var);
            }
            Collections.sort(p_variables);

            String[] par_var = variables.split(",");
            int indice = 0;
            for (String i : p_variables) {
                consulta = consulta.replace(i, par_var[indice]);
                indice++;
            }*/


            try {
                logger.info("Crea archivo");
                String stringConnection;
                if (typeConexion.equalsIgnoreCase("SID")) {
                    stringConnection = "jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid;
                } else {
                    stringConnection = "jdbc:oracle:thin:@//" + IP + ":" + puerto + "/" + sid;
                }

                String content = "--connect\n";
                content = content + stringConnection + "\n";
                content = content + "--username\n";
                content = content + Usuario + "\n";
                content = content + "--password\n";
                content = content + password + "\n";
                content = content + "--query\n";
                content = content + "\"" + consulta + "\"\n";
                content = content + "--fields-terminated-by\n";
                content = content + "'" + separador + "'\n";
                content = content + "--target-dir\n";
                content = content + phdfs_dest + "\n";
                if (Tipo_tabla.equals("GRANDE") || Tipo_tabla.equals("MEDIANA")) {
                    logger.info("Grande/Mediana p_fsize");
                    content = content + "--fetch-size\n";
                    content = content + p_fsize + "\n";
                }
                content = content + "--direct\n";
                if (Tipo_tabla.equals("GRANDE") || Tipo_tabla.equals("MEDIANA")) {
                    logger.info("Grande/Mediana p_split");
                    content = content + "--split-by\n";
                    content = content + p_split + "\n";
                }
                content = content + "-m\n";
                if (Tipo_tabla.equals("GRANDE") || Tipo_tabla.equals("MEDIANA")) {
                    logger.info("Grande/Mediana p_mapers");
                    content = content + p_mapers;
                } else {
                    content = content + "1";
                }

                fw = new FileWriter(FILENAME);
                bw = new BufferedWriter(fw);
                bw.write(content);


            } catch (IOException e) {
                logger.error(e.getMessage());
                e.printStackTrace();

            } finally {

                try {

                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {
                    logger.error(ex.getMessage());
                    ex.printStackTrace();

                }

            }

            return "OK";

        } catch (SQLException e) {
            logger.error(e.getMessage());
            return e.getMessage();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    public String mssql_query_sqoop(String ip, String puerto, String baseDatos, String usuario, String pass, String consulta, String path_spool, String nombreExtractor, String separador, String phdfs_dest, String p_fsize, String p_split, String p_mapers, String variables, String Tipo_tabla) {

        String url = "jdbc:sqlserver://" + ip + ":" + puerto + ";database=" + baseDatos;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
        } catch (ClassNotFoundException e) {
            return "Driver no encontrado";
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, usuario, pass);
            String query = " SET SHOWPLAN_XML on ";
            Statement st = conn.createStatement();
            boolean execute = st.execute(query);
            consulta = consulta.replace(";", "");

            ResultSet rs = st.executeQuery(consulta);

            st.close();
            conn.close();

            String FILENAME = path_spool + "\\" + nombreExtractor + ".txt";
            BufferedWriter bw = null;
            FileWriter fw = null;

            if (!consulta.toUpperCase().contains("WHERE")) {
                consulta += " WHERE $CONDITIONS";
            } else {
                consulta = consulta + " AND $CONDITIONS";
            }

            /////////////
            ArrayList<Integer> list = new ArrayList<Integer>();
            char character = '&';
            for (int i = 0; i < consulta.length(); i++) {
                if (consulta.charAt(i) == character) {
                    list.add(i);
                }
            }

            ArrayList<String> p_variables = new ArrayList<String>();
            for (Integer i : list) {
                String p_var = "";
                char ch = '\'';
                for (int j = i; j < consulta.length(); j++) {
                    if (consulta.charAt(j) == ch)
                        break;
                    p_var = p_var + consulta.charAt(j);
                }
                p_variables.add(p_var);
            }
            Collections.sort(p_variables);

            String[] par_var = variables.split(",");
            int indice = 0;
            for (String i : p_variables) {
                consulta = consulta.replace(i, par_var[indice]);
                indice++;
            }
            ////////////

            try {


                String content = "--connect\n";
                content = content + "jdbc:sqlserver://" + ip + ":" + puerto + ";databaseName=" + baseDatos + "\n";
                content = content + "--driver\n";
                content = content + "com.microsoft.sqlserver.jdbc.SQLServerDriver\n";
                content = content + "--username\n";
                content = content + usuario + "\n";
                content = content + "--password\n";
                content = content + pass + "\n";
                content = content + "--query\n";
                content = content + "\"" + consulta + "\"\n";
                content = content + "--fields-terminated-by\n";
                content = content + "'" + separador + "'\n";
                content = content + "--target-dir\n";
                content = content + phdfs_dest + "\n";
                if (Tipo_tabla.equals("GRANDE")) {
                    content = content + "--fetch-size\n";
                    content = content + p_fsize + "\n";
                }
                //content=content+"--direct\n";
                if (Tipo_tabla.equals("GRANDE")) {
                    content = content + "--split-by\n";
                    content = content + p_split + "\n";
                }
                content = content + "-m\n";
                if (Tipo_tabla.equals("GRANDE"))
                    content = content + p_mapers;
                else
                    content = content + "1";

                fw = new FileWriter(FILENAME);
                bw = new BufferedWriter(fw);
                bw.write(content);


            } catch (IOException e) {

                e.printStackTrace();

            } finally {

                try {

                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }

            }

        } catch (SQLException e) {
            //e.printStackTrace();
            return e.getMessage();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return "Archivo sqoop generado OK";
    }

    public String getColumns(String consulta) {
        String campos = "";
        try {

            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(consulta));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            List<SelectItem> selectitems = plain.getSelectItems();

            for (int i = 0; i < selectitems.size(); i++) {
                Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();

                if (i == selectitems.size() - 1) {
                    campos = campos + expression;
                } else {
                    campos = campos + expression + "|";
                }
                //System.out.println(expression);
            }
        } catch (JSQLParserException e) {
            return e.getMessage();
        }
        return campos.trim();
    }

    public String FormatQuery_Oracle_sqoop(String columnas, String query) {
        String Consulta = "";

        String[] qcol = getColumns(query).split("\\|");
        String[] col = columnas.split("\\|");
        if (col.length == 1 && col[0].equals("*")) {
            return query;
        }

        String format = get_dateformat("oracle");
        int indexFrom = query.toUpperCase().indexOf("FROM");
        Consulta = "SELECT ";
        for (int i = 0; i < qcol.length; i++) {
            if (columnas.contains(qcol[i])) {
                if (i == qcol.length - 1)
                    Consulta += "to_char(" + qcol[i] + ",'" + format + "') ";
                else
                    Consulta += "to_char(" + qcol[i] + ",'" + format + "'), ";
            } else {
                if (i == qcol.length - 1)
                    Consulta += qcol[i] + " ";
                else
                    Consulta += qcol[i] + ", ";
            }
        }
        Consulta += "" + query.substring(indexFrom, query.length());


        return Consulta;

    }

    public String FormatQuery_sqlServer_sqoop(String columnas, String query) {
        String Consulta = "";

        String[] qcol = getColumns(query).split("\\|");
        String[] col = columnas.split("\\|");
        if (col.length == 1 && col[0].equals("*")) {
            return query;
        }

        String format = get_dateformat("sqlserver");
        int indexFrom = query.toUpperCase().indexOf("FROM");
        Consulta = "SELECT ";

        for (int i = 0; i < qcol.length; i++) {
            if (columnas.contains(qcol[i])) {
                if (i == qcol.length - 1)
                    Consulta += "format(" + qcol[i] + ",'" + format + "') ";
                else
                    Consulta += "format(" + qcol[i] + ",'" + format + "'), ";
            } else {
                if (i == qcol.length - 1)
                    Consulta += qcol[i] + " ";
                else
                    Consulta += qcol[i] + ", ";
            }
            //Consulta=query.replaceAll("\\w?(?<!_)"+col[i]+"(?!_)[\b]?","format("+col[i]+",'"+format+"')");
            //query=Consulta;
        }
        Consulta += "" + query.substring(indexFrom, query.length());


        return Consulta;

    }

    public String get_dateformat(String bd) {

        Connection conn = null;
        String DateFormat = "";
        try {
            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            Statement stmt = null;
            ResultSet rs = null;
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from wf_balam_date_format where bd='" + bd + "'");

            while (rs.next()) {

                DateFormat = rs.getString("formato");
            }
            stmt.close();


        } catch (SQLException ex) {
            // handle any errors
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

        return DateFormat;
    }

    public String get_separador(int id) {

        Connection conn = null;
        String DateFormat = "";
        try {
            Properties prop = new Properties();
            InputStream is = null;

            try {
                is = new FileInputStream("path.properties");
                prop.load(is);
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                 /*   DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            Statement stmt = null;
            ResultSet rs = null;
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select caracter from wf_balam_separadores where id=" + id);

            while (rs.next()) {

                DateFormat = rs.getString("caracter");
            }
            stmt.close();


        } catch (SQLException ex) {
            // handle any errors
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

        return DateFormat;
    }

    public String parseCronhour(String hourCron) {
        String UTChour = "", parseN = "", parseC = "", ant = "";
        int singleHour, UTCh;
        boolean flag;
        SimpleDateFormat sfdate_local = new SimpleDateFormat("HH");
        SimpleDateFormat sfdate_utc = new SimpleDateFormat("HH");
        sfdate_utc.setTimeZone(TimeZone.getTimeZone("UTC"));

        flag = false;

        for (int i = 0; i <= hourCron.length() - 1; i++) {
            try {
                flag = false;
                Integer.parseInt(Character.toString(hourCron.charAt(i)));
                parseN += hourCron.charAt(i);

            } catch (NumberFormatException e) {
                flag = true;
                parseC = Character.toString(hourCron.charAt(i));

            }
            if (flag || i == hourCron.length() - 1) {

                Calendar cal = Calendar.getInstance();
                java.util.Date date;
                try {
                    if ((i - 1) > 0) {
                        if (!ant.equals("/")) {
                            date = sfdate_local.parse(String.valueOf(parseN));
                            cal.setTime(date);
                            String utc = sfdate_utc.format(cal.getTime());
                            UTChour += utc;
                        } else {
                            UTChour += parseN;
                        }
                    } else {
                        date = sfdate_local.parse(String.valueOf(parseN));
                        cal.setTime(date);
                        String utc = sfdate_utc.format(cal.getTime());
                        UTChour += utc;
                    }
                } catch (ParseException e) {
                    // e.printStackTrace();
                }
                UTChour += parseC;
                ant = parseC;

                parseN = "";
                parseC = "";
            }


        }
        return UTChour;
    }

    public boolean validateEmail(String email) {
        boolean flag = false;
        String[] mails = email.split(",");

        String regex = "^[\\w!#$%&'*+/=?`{|}~^-]+(?:\\.[\\w!#$%&'*+/=?`{|}~^-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,6}$";
        Pattern pattern = Pattern.compile(regex);

        for (int i = 0; i <= mails.length - 1; i++) {
            Matcher matcher = pattern.matcher(mails[i].trim());
            if (!matcher.matches()) {
                flag = true;
                break;
            }
        }

        return flag;

    }

    public String parseCronhourLocal(String hourCron) {
        String UTChour = "", parseN = "", parseC = "", ant = "";
        boolean flag;

        String[] hora_split = hourCron.split(" ");

        for (int i = 0; i <= hora_split[1].length() - 1; i++) {

            try {
                flag = false;
                Integer.parseInt(Character.toString(hora_split[1].charAt(i)));
                parseN += hora_split[1].charAt(i);

            } catch (NumberFormatException e) {
                flag = true;
                parseC = Character.toString(hora_split[1].charAt(i));

            }
            if (flag || i == hora_split[1].length() - 1) {
                if ((i - 1) > 0) {
                    if (!ant.equals("/")) {
                        try {
                            int uh = Integer.parseInt(parseN);
                            if (uh >= 6)
                                uh -= 6;
                            else
                                uh += 18;

                            UTChour += Integer.toString(uh);
                        } catch (Exception e) {
                            UTChour += parseN;
                        }

                    } else {
                        UTChour += parseN;
                    }
                } else {
                    try {
                        int uh = Integer.parseInt(parseN);
                        if (uh >= 6)
                            uh -= 6;
                        else
                            uh += 18;
                        UTChour += Integer.toString(uh);
                    } catch (Exception e) {
                        UTChour += parseN;
                    }

                }
                UTChour += parseC;
                ant = parseC;

                parseN = "";
                parseC = "";
            }
        }

        //System.out.println(UTChour);
        String freq = "";
        for (int i = 0; i <= hora_split.length - 1; i++) {
            if (i == 1) {
                freq += UTChour + " ";
            } else {
                freq += hora_split[i] + " ";
            }

        }
        return freq;
    }

    public String formatQueryOracle(String consulta, String separador, String IP, String puerto, String sid, String Usuario, String password, String typeConexion) {
        String consulta_formateada = "";
        try {

            /*CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(consulta));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            List<SelectItem> selectitems = plain.getSelectItems();*/

            /*Cambios Realizados el dia 15 de Marzo 2019 a las 4pm */
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(consulta.replace("&", "")));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            List<SelectItem> selectitems = plain.getSelectItems();
            /*Finaliza Cambios*/

            int indexFrom = consulta.toUpperCase().indexOf("FROM");

            consulta_formateada = "SELECT replace(";

            if (selectitems.size() == 1 && selectitems.get(0).toString().trim().equals("*")) {
                String FROMBody = consulta.substring(indexFrom, consulta.length());
                String NombTabla = "";
                for (int i = 5; i <= FROMBody.length() - 1; i++) {
                    if (FROMBody.charAt(i) != ',' && FROMBody.charAt(i) != ' ' && FROMBody.charAt(i) != ';') {
                        NombTabla += FROMBody.charAt(i);
                    } else {
                        break;
                    }
                }

                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
                } catch (ClassNotFoundException e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                    return "Driver no encontrado";
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                    return "Driver no encontrado";
                } catch (InstantiationException e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                    return "Driver no encontrado";
                }

                Connection connection = null;

                try {
                    logger.info("jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid);
                    if (typeConexion.equalsIgnoreCase("SID")) {
                        connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid, Usuario, password);
                    } else {
                        connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@//" + IP + ":" + puerto + "/" + sid, Usuario, password);
                    }

                    Statement st = connection.createStatement();
                    ResultSet rs = st.executeQuery("select * from " + NombTabla);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        if (i == rsmd.getColumnCount())
                            consulta_formateada += rsmd.getColumnName(i) + " ";
                        else
                            consulta_formateada += rsmd.getColumnName(i) + "||'" + separador + "'||";
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage());
                    e.printStackTrace();
                    return e.getMessage();
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
                ////revisar en base de datos
            } else {
                for (int i = 0; i < selectitems.size(); i++) {
                    Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
                    if (i == selectitems.size() - 1) {
                        consulta_formateada = consulta_formateada + expression + " ";
                    } else {
                        consulta_formateada = consulta_formateada + expression + "||'" + separador + "'||";
                    }
                }
            }
            consulta_formateada += ",CHR(13)||CHR(10)) " + consulta.substring(indexFrom, consulta.length());
        } catch (JSQLParserException e) {
            consulta_formateada = "Consulta invalida";
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
            return "Consulta no se ha podido parsear";
        }
        return consulta_formateada;
    }

    public String get_pathconf() {


        String path = "";

        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }


         logger.info(prop.getProperty("home")+"/INGESTAS_BALAM/config/path_filesystem.txt");

        //  String Path_f = "/home/appbalam_ingestas/INGESTAS_BALAM/config/path_filesystem.txt";
        String Path_f = prop.getProperty("home").concat("/INGESTAS_BALAM/config/path_filesystem.txt");
        StringBuilder contentBuilder = new StringBuilder();

        try {
            BufferedReader br = new BufferedReader(new FileReader(Path_f));
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                contentBuilder.append(sCurrentLine);
            }
            path = contentBuilder.toString();

            File dir = new File(path + "/reg");
            dir.mkdirs();
            dir = new File(path + "/logs/procesar");
            dir.mkdirs();
            dir = new File(path + "/salida");
            dir.mkdirs();

        } catch (IOException e) {
            return e.getMessage();
        }

        return path;

    }

    public void update_Variables(String variables, String jobId) {
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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                   /* DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "update flow set variables2=? where job_id=?";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, variables);
            stmt.setString(2, jobId);
            stmt.execute();

            stmt.close();


        } catch (SQLException ex) {
            // handle any errors
            ex.printStackTrace();

        }

    }

    public void insert_dependencias(String arr) {
        String[] rows = arr.split(";");
        String p_nombre = "", p_coordinator = "", p_posicion = "", p_dependencia = "", p_max = "", p_col = "";

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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            for (int i = 0; i <= rows.length - 1; i++) {
                String[] cols = rows[i].split(",");
                for (int j = 0; j <= cols.length - 1; j++) {
                    p_nombre = cols[0];
                    p_coordinator = cols[1];
                    p_posicion = cols[2];
                    p_dependencia = cols[3];
                    p_max = cols[4];
                    p_col = cols[5];
                }
                //   System.out.println(p_nombre+"-"+p_coordinator+"-"+p_posicion+"-"+p_dependencia+"-"+p_max+"-"+p_col);
                PreparedStatement stmt = null;
                ResultSet rs = null;
                String query = "insert into wf_flujos_dependencias values(?,?,?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, p_nombre);
                stmt.setString(2, p_coordinator.trim());
                stmt.setInt(3, Integer.parseInt(p_posicion));
                stmt.setString(4, p_dependencia.trim());
                stmt.setInt(5, Integer.parseInt(p_max));
                stmt.setInt(6, Integer.parseInt(p_col));
                stmt.execute();

                stmt.close();


            }
            // Do something with the Connection
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
    }

    public boolean ValidaisExists(String nombre) {
        boolean status = false;
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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                   /* DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/
            PreparedStatement stmt = null;
            ResultSet rs = null;

            String query = "SELECT nombre FROM wf_bundles WHERE nombre = '" + nombre + "'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();

            if (rs.next()) {
                status = false;

            } else
                status = true;
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return status;
    }


    ///20190131

    public List<String> getflows_dependences(String p_nombre) {
        ArrayList<String> Wf_flujos_dep = new ArrayList<>();

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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                   /* DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/


            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select *from  wf_flujos_dependencias  where  nombre= ? ";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, p_nombre);
            rs = stmt.executeQuery();


            while (rs.next()) {
                // Wf_flujos_dep.add(new Wf_flujos_dependencias(rs.getString("nombre"), rs.getString("coordinator"), rs.getInt("posicion"), rs.getString("dependencia"), rs.getInt("max"), rs.getInt("col")));
                Wf_flujos_dep.add(rs.getString("coordinator") + ";" + rs.getInt("posicion") + ";" + rs.getInt("col"));
            }
            stmt.close();
            conn.close();

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
        return Wf_flujos_dep;

    }

    public List<String> FilterSelect() {
        ArrayList<String> details_flow = new ArrayList<>();

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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/


            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = " select name from flow ORDER BY name; ";

            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();


            while (rs.next()) {
                details_flow.add(rs.getString("name"));
            }
            stmt.close();
            conn.close();

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
        return details_flow;

    }


    public void delete_dependencies(String p_nombre) {
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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "delete from wf_flujos_dependencias where nombre=?";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, p_nombre);

            stmt.execute();

            stmt.close();
            // Do something with the Connection
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
    }


    public void create_wfxml(String nombDep) {

        ArrayList<String> WFlujos = new ArrayList<String>();
        //    /home/appbalam_ingestas/INGESTAS_BALAM/datos_fs  Esto ya estaba comentado antes de que  realizara cambios el dia 07/03/2019
        //String rootFolder = "C:\\\\Users\\\\escc_\\\\Documents\\\\DESARROLLOS\\\\2018\\\\EXTRACTORES BIG DATA\\\\wf_dependencias"; //<<<=======================================================
        String rootFolder = get_pathconf() + "/wf_dependencias";

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
            conn =DriverManager.getConnection(prop.getProperty("jdbcConnection"));
                    /*DriverManager.getConnection("jdbc:mysql://10.231.236.25:3306/ingestas_metadata?" +
                            "user=ingestas_balam&password=Ingestas123$");*/

            // Do something with the Connection
            PreparedStatement stmt = null;
            ResultSet rs = null;
            String query = "select coordinator,posicion,dependencia,col from wf_flujos_dependencias where nombre=? order by posicion";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, nombDep);
            rs = stmt.executeQuery();

            while (rs.next()) {
                WFlujos.add(rs.getString("coordinator") + "," + rs.getInt("posicion") + "," + rs.getString("dependencia") + "," + rs.getString("col"));
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


        File dir = new File(rootFolder + "/" + nombDep); //<<<=======================================================
        dir.mkdirs();
        try {
            hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombDep);
            hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/triggers/" + nombDep);
        } catch (IOException e) {
            e.printStackTrace();
        }


        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }


        //generando WORKFLOW.xml

        for (String depFlows : WFlujos) {
            String[] sTemp = depFlows.split(",");
            int indice = Integer.parseInt(sTemp[1]);
            int colum = Integer.parseInt(sTemp[3]);
            String coord = sTemp[0];
            File dir2 = new File(rootFolder + "/" + nombDep + "/" + coord); //<<<=======================================================
            dir2.mkdirs();


            String FILENAME = rootFolder + "/" + nombDep + "/" + coord + "/workflow.xml"; //<<<=======================================================
            BufferedWriter bw = null;
            FileWriter fw = null;

            try {


                String content = "<workflow-app name=\"${nombreWorkflow" + indice + colum + "}\" xmlns=\"uri:oozie:workflow:0.4\">\n";
                content = content + "<start to=\"decision_tipoWF\"/>\n";
                content = content + "<decision name=\"decision_tipoWF\">\n";
                content = content + "\t<switch>\n";
                content = content + "\t\t<case to=\"CleantriggerFile\">${tipo_WF" + indice + colum + " eq 'archivo'}</case>\n";
                content = content + "\t\t<case to=\"sshAction_Spool\">${tipo_WF" + indice + colum + " eq 'spool'}</case>\n";
                content = content + "\t\t<case to=\"CleantriggerSqoop\">${tipo_WF" + indice + colum + " eq 'sqoop'}</case>\n";
                content = content + "\t\t<default to=\"Mail_fail\" />\n";
                content = content + "\t</switch>\n";
                content = content + "</decision>\n";
                content = content + "<action name=\"sshAction_Spool\">\n";
                content = content + "\t<ssh xmlns=\"uri:oozie:ssh-action:0.1\">\n";
                // content = content + "\t\t<host>appbalam_ingestas@10.231.236.25</host>\n";
                content = content + "\t\t<host>" + prop.getProperty("remoteNode_putHDFS") + "</host>\n";


                content = content + "\t\t<command>${shellScriptPath_spool" + indice + colum + "}</command>\n";
                content = content + "\t\t<args>${argument_spool" + indice + colum + "}</args>\n";
                content = content + "\t\t<capture-output/>\n";
                content = content + "\t</ssh>\n";
                content = content + "\t<ok to=\"decision_sshSpool\"/>\n";
                content = content + "\t<error to=\"Mail_fail\"/>\n";
                content = content + "</action>\n";
                content = content + "<decision name=\"decision_sshSpool\">\n" +
                        "\t<switch>\n" +
                        "\t\t<case to=\"sshAction_putHDFS\">${wf:actionData('sshAction_Spool')['STATUS']}</case>\n" +
                        "\t\t<default to=\"Mail_spool\" />\n" +
                        "\t</switch>\n" +
                        "</decision>\n";
                content = content + "<action name='CleantriggerSqoop'>\n" +
                        "\t<ssh xmlns=\"uri:oozie:ssh-action:0.1\">\n" +
                        // "\t\t<host>appbalam_ingestas@10.231.236.25</host>\n" +
                        "\t\t<host>" + prop.getProperty("remoteNode_putHDFS") + "</host>\n" +

                        //  "\t\t<command>/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/dependencias/clean_trigger.sh</command>\n" +
                        "\t\t<command>" + prop.getProperty("home") + "/INGESTAS_BALAM/config/lib/dependencias/clean_trigger.sh</command>\n" +
                        "\t\t<args>${clean_sqoop" + indice + colum + "}</args>\n" +
                        "\t\t<capture-output/>\n" +
                        "\t</ssh>\n" +
                        "\t<ok to=\"spoolAction_qry\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<action name='CleantriggerFile'>\n" +
                        "\t<ssh xmlns=\"uri:oozie:ssh-action:0.1\">\n" +
                        //"\t\t<host>appbalam_ingestas@10.231.236.25</host>\n" +
                        "\t\t<host>" + prop.getProperty("remoteNode_putHDFS") + "</host>\n" +
                        //  "\t\t<command>/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/dependencias/clean_trigger.sh</command>\n" +
                        "\t\t<command>" + prop.getProperty("home") + "/INGESTAS_BALAM/config/lib/dependencias/clean_trigger.sh</command>\n" +

                        "\t\t<args>${clean_file" + indice + colum + "}</args>\n" +
                        "\t\t<capture-output/>\n" +
                        "\t</ssh>\n" +
                        "\t<ok to=\"sshAction_putHDFS\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<action name=\"sshAction_putHDFS\">\n" +
                        "\t<ssh xmlns=\"uri:oozie:ssh-action:0.1\">\n" +
                        "\t\t<host>${remoteNode_putHDFS" + indice + colum + "}</host>\n" +
                        "\t\t<command>${shellScriptPath_putHDFS" + indice + colum + "}</command>\n" +
                        "\t\t<args>${argument_putHDFS" + indice + colum + "}</args>\n" +
                        "\t\t<capture-output/>\n" +
                        "\t</ssh>\n" +
                        "\t<ok to=\"decision_putHDFS\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<decision name=\"decision_putHDFS\">\n" +
                        "\t<switch>\n" +
                        "\t\t<case to=\"decision_ALERTA\">${wf:actionData('sshAction_putHDFS')['STATUS']}</case>\n" +
                        "\t\t<default to=\"Mail_putHDFS\" />\n" +
                        "\t</switch>\n" +
                        "</decision>\n";
                content = content + "<decision name=\"decision_ALERTA\">\n" +
                        "\t<switch>\n" +
                        "\t\t<case to=\"Mail_ALERTA\">${wf:actionData('sshAction_putHDFS')['ALERTA']}</case>\n" +
                        "\t\t<default to=\"decision_DESTINO\" />\n" +
                        "\t</switch>\n" +
                        "</decision>\n";
                content = content + "<decision name=\"decision_DESTINO\">\n" +
                        "\t<switch>\n" +
                        "\t\t<case to=\"sparkAction_Insert\">${destino" + indice + colum + " eq 'tabla'}</case>\n" +
                        "\t\t<case to=\"triggerOK\">${destino" + indice + colum + " eq 'carpeta'}</case>\n" +
                        "\t\t<default to=\"Mail_fail\" />\n" +
                        "\t</switch>\n" +
                        "</decision>\n";
                content = content + "<action name=\"sparkAction_Insert\">\n" +
                        "\t<spark xmlns=\"uri:oozie:spark-action:0.1\">\n" +
                        "\t\t<job-tracker>${jobTracker" + indice + colum + "}</job-tracker>\n" +
                        "\t\t<name-node>${nameNode" + indice + colum + "}</name-node>      \t\t\n" +
                        "\t\t<master>yarn-client</master>\n" +
                        "\t\t<name>Spark Oozie Insert</name>\n" +
                        "\t\t<class>Main</class>\n" +
                        "\t\t<jar>${path_jarSpark" + indice + colum + "}/Spark_oozie-assembly-1.0.jar</jar>\n" +
                        "\t\t<spark-opts>--queue ${queueName" + indice + colum + "}</spark-opts>\n" +
                        "\t\t<arg>${path_archivoSpark" + indice + colum + "}</arg>\n" +
                        "\t\t<arg>${esquema" + indice + colum + "}</arg>\n" +
                        "\t\t<arg>${tabla" + indice + colum + "}</arg>\n" +
                        "\t\t<arg>${particionado" + indice + colum + "}</arg>\n" +
                        "\t\t<arg>${campo_particion" + indice + colum + "}</arg>\n" +
                        "\t\t<arg>${acumulado" + indice + colum + "}</arg>\n" +
                        "\t\t<arg>${separador" + indice + colum + "}</arg>\n" +
                        "\t</spark>\n" +
                        "    <ok to=\"triggerOK\" />\n" +
                        "    <error to=\"Mail_fail\" />\n" +
                        "</action>\n";
                content = content + "<action name=\"spoolAction_qry\">\n" +
                        "\t<sqoop xmlns=\"uri:oozie:sqoop-action:0.2\">\n" +
                        "\t\t<job-tracker>${jobTracker" + indice + colum + "}</job-tracker>\n" +
                        "\t\t<name-node>${nameNode" + indice + colum + "}</name-node>\n" +
                        "\t\t<prepare>\n" +
                        "\t\t\t<delete path=\"${sqoop_hdfs" + indice + colum + "}\"/>\n" +
                        "\t\t</prepare>\n" +
                        "\t\t<configuration>\n" +
                        "\t\t\t<property>\n" +
                        "\t\t\t\t<name>mapred.job.queue.name</name>\n" +
                        "\t\t\t\t<value>${queueName" + indice + colum + "}</value>\n" +
                        "\t\t\t</property>\n" +
                        "\t\t\t<property>\n" +
                        "\t\t\t\t<name>org.apache.sqoop.splitter.allow_text_splitter</name>\n" +
                        "\t\t\t\t<value>true</value>\n" +
                        "\t\t\t</property>\n" +
                        "\t\t</configuration>\n" +
                        "\t\t<arg>import</arg>\n" +
                        "\t\t<arg>--options-file</arg>\n" +
                        "\t\t<arg>${optionFile" + indice + colum + "}</arg>\n" +
                        "\t\t<file>${option_File_path_in_hdfs" + indice + colum + "}</file>\n" +
                        "\t\t<file>${share_lib_ojdbc" + indice + colum + "}</file>\n" +
                        "\t\t<file>${share_lib_sqooop_jar" + indice + colum + "}</file>\n" +
                        "\t</sqoop>\n" +
                        "\t<ok to=\"sshAction_putHDFS\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<action name='triggerOK'>\n" +
                        "\t<ssh xmlns=\"uri:oozie:ssh-action:0.1\">\n" +
                        //    "\t\t<host>appbalam_ingestas@10.231.236.25</host>\n" +
                        "\t\t<host>" + prop.getProperty("remoteNode_putHDFS") + "</host>\n" +


                        // "\t\t<command>/home/appbalam_ingestas/INGESTAS_BALAM/config/lib/dependencias/triggerOK.sh</command>\n" +
                        "\t\t<command>" + prop.getProperty("home") + "/INGESTAS_BALAM/config/lib/dependencias/triggerOK.sh</command>\n" +

                        "\t\t<args>${argument_trigger" + indice + colum + "}</args>\n" +
                        "\t\t<capture-output/>\n" +
                        "\t</ssh>\n" +
                        "\t<ok to=\"decision_trigger\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<decision name=\"decision_trigger\">\n" +
                        "\t<switch>\n" +
                        "\t\t<case to=\"end\">${wf:actionData('sshAction_putHDFS')['STATUS']}</case>\n" +
                        "\t\t<default to=\"Mail_spool\" />\n" +
                        "\t</switch>\n" +
                        "</decision>\n";
                content = content + "<action name=\"Mail_spool\">\n" +
                        "\t<email xmlns=\"uri:oozie:email-action:0.1\">\n" +
                        "\t\t<to>${cta_correo" + indice + colum + "}</to>\n" +
                        "\t\t<subject>Error en flujo: ${wf:name()}</subject>\n" +
                        "\t\t<body>Error en el proceso: ${wf:actionData('sshAction_Spool')['MENSAJE']}</body>\n" +
                        "\t</email>\n" +
                        "\t<ok to=\"killsshAction_spool\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<action name=\"Mail_putHDFS\">\n" +
                        "\t<email xmlns=\"uri:oozie:email-action:0.1\">\n" +
                        "\t\t<to>${cta_correo" + indice + colum + "}</to>\n" +
                        "\t\t<subject>Error en flujo: ${wf:name()}</subject>\n" +
                        "\t\t<body>Error en el proceso: ${wf:actionData('sshAction_putHDFS')['MENSAJE']}</body>\n" +
                        "\t</email>\n" +
                        "\t<ok to=\"killsshAction_spool\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<action name=\"Mail_fail\">\n" +
                        "\t<email xmlns=\"uri:oozie:email-action:0.1\">\n" +
                        "\t\t<to>${cta_correo" + indice + colum + "}</to>\n" +
                        "\t\t<subject>Error en flujo: ${wf:name()}</subject>\n" +
                        "\t\t<body>Error en el proceso: Error message[${wf:errorMessage(wf:lastErrorNode())}]</body>\n" +
                        "\t</email>\n" +
                        "\t<ok to=\"fail\"/>\n" +
                        "\t<error to=\"fail\"/>\n" +
                        "</action>\n";
                content = content + "<action name=\"Mail_ALERTA\">\n" +
                        "\t<email xmlns=\"uri:oozie:email-action:0.1\">\n" +
                        "\t\t<to>${cta_correo" + indice + colum + "}</to>\n" +
                        "\t\t<subject>Alerta: ${wf:name()}</subject>\n" +
                        "\t\t<body>Alerta: ${wf:actionData('sshAction_putHDFS')['MENSAJE_A']}</body>\n" +
                        "\t</email>\n" +
                        "\t<ok to=\"decision_DESTINO\"/>\n" +
                        "\t<error to=\"Mail_fail\"/>\n" +
                        "</action>\n";
                content = content + "<kill name=\"fail\">\n" +
                        "\t<message>Error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "</kill>\n";
                content = content + "<kill name=\"killsshAction_spool\">\n" +
                        "\t<message>\"Error en el proceso: ${wf:actionData('sshAction_Spool')['MENSAJE']}\"</message>\n" +
                        "</kill>\n";
                content = content + "<end name=\"end\"/>\n";
                content = content + "</workflow-app>";


                fw = new FileWriter(FILENAME);
                bw = new BufferedWriter(fw);
                bw.write(content);


                hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombDep + "/" + coord);
                hdfs.mkdir("/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/triggers/" + nombDep + "/" + coord);


            } catch (IOException e) {

                e.printStackTrace();

            } finally {

                try {

                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }

            }
            try {
                hdfs.copyFromLocal(FILENAME, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombDep + "/" + coord);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //indice++;
        }

        ////////////////////////////

        //generando COORDINATOR.xml
        //indice = 1;
        for (String depFlows : WFlujos) {
            String[] sTemp = depFlows.split(",");
            String coord = sTemp[0];
            int indice = Integer.parseInt(sTemp[1]);
            int colum = Integer.parseInt(sTemp[3]);
            String pos = sTemp[1];
            String[] depend = sTemp[2].split("\\|");

            String FILENAME = rootFolder + "/" + nombDep + "/" + coord + "/coordinator.xml"; //<<<=======================================================
            BufferedWriter bw = null;
            FileWriter fw = null;

            try {

                String content = "";
                if (pos.equals("1")) {
                    content = "<coordinator-app name=\"${nombreWorkflow" + indice + colum + "}\" start=\"${fecha_ini}\"\n";
                    content = content + "end=\"${fecha_fin}\" frequency=\"${frecuencia}\" timezone=\"UTC\"\n";
                    content = content + "xmlns=\"uri:oozie:coordinator:0.2\">\n";
                    content = content + "<action>\n" +
                            "<workflow>\n" +
                            "<app-path>${app_path" + indice + colum + "}</app-path>\n" +
                            "<configuration>\n" +
                            "<property>\n" +
                            "<name>nameNode</name>\n" +
                            "<value>${nameNode" + indice + colum + "}</value>\n" +
                            "</property>\n" +
                            "<property>\n" +
                            "<name>jobTracker</name>\n" +
                            "<value>${jobTracker" + indice + colum + "}</value>\n" +
                            "</property>\n" +
                            "</configuration>\n" +
                            "</workflow>\n" +
                            "</action>\n";
                    content = content + "</coordinator-app>\n";
                } else {
                    content = "<coordinator-app name=\"${nombreWorkflow" + indice + colum + "}\" start=\"${fecha_ini2}\"\n";
                    content = content + "end=\"${fecha_fin2}\" frequency=\"${frecuencia2}\" timezone=\"UTC\"\n";
                    content = content + "xmlns=\"uri:oozie:coordinator:0.2\">\n";
                    content = content + "<controls>\n" +
                            "        <timeout>720</timeout>\n" +
                            "        <concurrency>1</concurrency>\n" +
                            "        <throttle>1</throttle>\n" +
                            "</controls>\n";
                    content = content + "<datasets>\n";
                    for (int i = 0; i <= depend.length - 1; i++) {
                        content = content + "<dataset name=\"inputDS" + i + "\" frequency=\"${coord:days(1)}\" initial-instance=\"${fecha_ini2}\" timezone=\"UTC\">\n" +
                                "\t\t<uri-template>${triggerFileDir" + indice + colum + i + "}</uri-template>\n" +
                                "\t\t<done-flag>trigger.flag</done-flag>\n" +
                                "\t</dataset>\n";
                    }
                    content = content + "</datasets>\n";
                    content = content + "<input-events>\n";
                    for (int i = 0; i <= depend.length - 1; i++) {
                        content = content + "<data-in name=\"DepInput" + i + "\" dataset=\"inputDS" + i + "\">\n" +
                                "\t\t<instance>${fecha_ini2}</instance>\n" +
                                "    </data-in>\n";
                    }
                    content = content + "</input-events>\n";
                    content = content + "<action>\n" +
                            "<workflow>\n" +
                            "<app-path>${app_path" + indice + colum + "}</app-path>\n" +
                            "<configuration>\n" +
                            "<property>\n" +
                            "<name>nameNode</name>\n" +
                            "<value>${nameNode" + indice + colum + "}</value>\n" +
                            "</property>\n" +
                            "<property>\n" +
                            "<name>jobTracker</name>\n" +
                            "<value>${jobTracker" + indice + colum + "}</value>\n" +
                            "</property>\n" +
                            "</configuration>\n" +
                            "</workflow>\n" +
                            "</action>\n";
                    content = content + "</coordinator-app>\n";
                }

                fw = new FileWriter(FILENAME);
                bw = new BufferedWriter(fw);
                bw.write(content);


            } catch (IOException e) {

                e.printStackTrace();

            } finally {

                try {

                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }

            }
            try {
                hdfs.copyFromLocal(FILENAME, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombDep + "/" + coord);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //indice++;
        }

        ///////////////////////////

        //generando BUNDLE.xml
        //indice = 1;

        String FILENAME = rootFolder + "/" + nombDep + "/bundle.xml"; //<<<=======================================================
        BufferedWriter bw = null;
        FileWriter fw = null;

        try {

            String content = "<bundle-app name='${nombreWFDep}' xmlns='uri:oozie:bundle:0.2'>\n";
            content = content + "  <controls>\n";
            content = content + "       <kick-off-time>${fecha_ini}</kick-off-time>\n";
            content = content + "  </controls>\n";
            for (String depFlows : WFlujos) {
                String[] sTemp = depFlows.split(",");
                int indice = Integer.parseInt(sTemp[1]);
                int colum = Integer.parseInt(sTemp[3]);
                content = content + "  <coordinator name='${nombreWorkflow" + indice + colum + "}' >\n" +
                        "       <app-path>${app_path" + indice + colum + "}</app-path>\n" +
                        "   </coordinator>\n";
                //indice++;
            }
            content = content + "</bundle-app> \n";


            fw = new FileWriter(FILENAME);
            bw = new BufferedWriter(fw);
            bw.write(content);


        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {

                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();

            } catch (IOException ex) {

                ex.printStackTrace();

            }

        }
        try {
            hdfs.copyFromLocal(FILENAME, "/user/appbalam_ingestas/INGESTAS_BALAM/Bundle/" + nombDep);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String valida_dependencias(String cadena) {
        String mensaje = null;
        String[] arr = cadena.split(";");
        ArrayList<String> Array = new ArrayList<String>(Arrays.asList(arr));
        ArrayList<String> ArrbB = new ArrayList<String>();

        for (String datmp : Array) {
            String[] dato = datmp.split(",");
            int posicion = busqueda_binaria(ArrbB, dato[1]);
            if (posicion == -1) {
                ArrbB.add(dato[1]);
                Collections.sort(ArrbB);
            } else {
                return "Extractor " + dato[1] + " repetido en la maya de dependencias.";
            }
        }

        return mensaje;
    }

    public int busqueda_binaria(ArrayList<String> options, String dato) {
        int inicio = 0, pos, fin = options.size() - 1;
        Double posOp;

        while (inicio <= fin) {
            posOp = Math.floor((inicio + fin) / 2);
            pos = posOp.intValue();
            if (options.get(pos).toUpperCase().compareTo(dato.toUpperCase()) == 0) {
                return pos;
            } else if (options.get(pos).toUpperCase().compareTo(dato.toUpperCase()) < 0) {
                inicio = pos + 1;
            } else {
                fin = pos - 1;
            }
        }
        return -1;
    }

    public String formatFechaOracle(String consulta, String IP, String puerto, String sid, String Usuario, String
            password, String typeConexion) {
        String consulta_formateada = "";
        try {

            String format = get_dateformat("oracle");
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(consulta.replace("&", "")));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            List<SelectItem> selectitems = plain.getSelectItems();

            int indexFrom = consulta.toUpperCase().indexOf("FROM");

            String FROMBody = consulta.substring(indexFrom, consulta.length());
            String NombTabla = "";
            String paramfrom = FROMBody.substring(5, FROMBody.length()).trim();
            for (int i = 0; i <= paramfrom.length() - 1; i++) {
                if (paramfrom.charAt(i) != ',' && paramfrom.charAt(i) != ' ' && paramfrom.charAt(i) != ';') {
                    NombTabla += paramfrom.charAt(i);
                } else {
                    break;
                }
            }

            consulta_formateada = "SELECT ";

            try {
                Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
            } catch (ClassNotFoundException e) {
                //-------->logger.error(e.getMessage());
                e.printStackTrace();
                return "Driver no encontrado";
            } catch (IllegalAccessException e) {
                //-------->logger.error(e.getMessage());
                e.printStackTrace();
                return "Driver no encontrado";
            } catch (InstantiationException e) {
                //-------->logger.error(e.getMessage());
                e.printStackTrace();
                return "Driver no encontrado";
            }

            if (selectitems.size() == 1 && selectitems.get(0).toString().trim().equals("*")) {

                Connection connection = null;
                try {
                    //-------->logger.info("jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid);
                    if (typeConexion.equalsIgnoreCase("SID")) {
                        connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid, Usuario, password);
                    } else {
                        connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@//" + IP + ":" + puerto + "/" + sid, Usuario, password);
                    }

                    Statement st = connection.createStatement();
                    ResultSet rs = st.executeQuery("select * from " + NombTabla);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        if (i == rsmd.getColumnCount()) {
                            if (rsmd.getColumnTypeName(i).equals("DATE")) {
                                consulta_formateada += "to_char(" + rsmd.getColumnName(i) + ",'" + format + "') ";
                            } else {
                                consulta_formateada += rsmd.getColumnName(i) + " ";
                            }
                        } else {
                            if (rsmd.getColumnTypeName(i).equals("DATE")) {
                                consulta_formateada += "to_char(" + rsmd.getColumnName(i) + ",'" + format + "'), ";
                            } else {
                                consulta_formateada += rsmd.getColumnName(i) + ", ";
                            }
                        }
                    }
                } catch (SQLException e) {
                    //-------->logger.error(e.getMessage());
                    e.printStackTrace();
                    return e.getMessage();
                }
                ////revisar en base de datos
            } else {
                ArrayList<String> columnQuery = new ArrayList<String>();
                ArrayList<String> columnTable = new ArrayList<String>();

                for (int i = 0; i < selectitems.size(); i++) {
                    Expression expression = ((SelectExpressionItem) selectitems.get(i)).getExpression();
                    columnQuery.add(expression.toString().trim().toUpperCase());
                }
                /////////////////////////////
                Connection connection = null;
                try {
                    //-------->logger.info("jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid);
                    if (typeConexion.equalsIgnoreCase("SID")) {
                        connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@" + IP + ":" + puerto + ":" + sid, Usuario, password);
                    } else {
                        connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@//" + IP + ":" + puerto + "/" + sid, Usuario, password);
                    }

                    Statement st = connection.createStatement();
                    ResultSet rs = st.executeQuery("select * from " + NombTabla);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        columnTable.add(rsmd.getColumnName(i) + "," + rsmd.getColumnTypeName(i));
                    }

                    for (int i = 0; i <= columnQuery.size() - 1; i++) {

                        for (int j = 0; j <= columnTable.size() - 1; j++) {
                            String[] val = columnTable.get(j).split(",");
                            String nColumn = val[0];
                            String tColumn = val[1];

                            if (columnQuery.get(i).contains(nColumn.trim().toUpperCase())) {
                                if (i == columnQuery.size() - 1) {
                                    if (tColumn.equals("DATE")) {
                                        consulta_formateada += "to_char(" + columnQuery.get(i) + ",'" + format + "') ";
                                    } else {
                                        consulta_formateada += columnQuery.get(i) + " ";
                                    }
                                } else {
                                    if (tColumn.equals("DATE")) {
                                        consulta_formateada += "to_char(" + columnQuery.get(i) + ",'" + format + "'), ";
                                    } else {
                                        consulta_formateada += columnQuery.get(i) + ", ";
                                    }
                                }

                                break;
                            }
                        }
                    }


                } catch (SQLException e) {
                    //-------->logger.error(e.getMessage());
                    e.printStackTrace();
                    return e.getMessage();
                }

            }
            consulta_formateada += consulta.substring(indexFrom, consulta.length());
        } catch (JSQLParserException e) {
            consulta_formateada = "Consulta invalida";
            //-------->logger.error(e.getMessage());
            e.printStackTrace();
        } catch (Exception ex) {
            //-------->logger.error(ex.getMessage());
            ex.printStackTrace();
            return "Consulta no se ha podido parsear";
        }
        return consulta_formateada;
    }


}
