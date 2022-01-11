package net.crunchdroid.shell.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class GeneralesLoc {

    Logger logger = LoggerFactory.getLogger(GeneralesLoc.class);

    // Ejecutar un comando unix y devuelve arrayList con elementos de respuesta
    public ArrayList ejecCmdLista(String[] comando) {
        ArrayList lista = new ArrayList();

        try {
            Process proceso = Runtime.getRuntime().exec(comando);
            int estadoProc = proceso.waitFor();

            // Si proceso fue finalizado
            if (estadoProc == 0) {

                int codFin = proceso.exitValue();

                if (codFin == 0) {
                    BufferedReader valComando = new BufferedReader(new InputStreamReader(proceso.getInputStream()));
                    String linea = null;
                    while ((linea = valComando.readLine()) != null) {
                        lista.add(linea);
                    }
                }
            }
        } catch (IOException ioe) {
            System.out.println(ioe);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return lista;
    }

    // Ejecuta un comando unix y devuelve entero con valor de ejecucion
    public int ejecCmd(String[] comando) {
        int codFin = -666;

        try {
            logger.info("Comando: " + comando);
            for (String s : comando) {
                logger.info("split: " + s);
            }
            Process proceso = Runtime.getRuntime().exec(comando);
            int estadoProc = proceso.waitFor();
            logger.info("Estado ejecución linux: " + estadoProc);
            codFin = proceso.exitValue();

        } catch (IOException ioe) {
            logger.error(ioe.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        return codFin;
    }

    // Verificar si existe directorio
    public boolean existeDir(String directorio) {
        File dir = new File(directorio);
        if (dir.exists() && dir.isDirectory())
            return true;
        else
            return false;
    }

    // Verificar si existe archivo
    public boolean existeArch(String archivo) {
        File dir = new File(archivo);
        if (dir.exists() && dir.isFile())
            return true;
        else
            return false;
    }

    // Unifica directorio
    public String uneDir(String directorio, String archivo) {
        String dirUnido;

        if (directorio.substring(directorio.length() - 1).equals("/"))
            dirUnido = directorio + archivo;
        else
            dirUnido = directorio + "/" + archivo;

        return dirUnido;
    }

    // Crea comando
    public String[] creaCmd(String comando, String opciones, String dirFich) {
        ArrayList<String> cmdCompleto = new ArrayList<String>();
        cmdCompleto.add(comando);

        if (!opciones.isEmpty())
            cmdCompleto.add(opciones);

        cmdCompleto.add(dirFich);

        String[] cmdDev = new String[cmdCompleto.size()];
        cmdCompleto.toArray(cmdDev);

        return cmdDev;
    }
}
