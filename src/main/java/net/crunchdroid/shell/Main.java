package net.crunchdroid.shell;

import net.crunchdroid.shell.local.CreaLoc;
import net.crunchdroid.shell.local.GeneralesLoc;
import net.crunchdroid.shell.local.LeeLoc;
import net.crunchdroid.shell.remote.ConexionRem;
import net.crunchdroid.shell.remote.CreaRem;
import net.crunchdroid.shell.remote.GeneralesRem;
import net.crunchdroid.shell.remote.LeeRem;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        //String palabraP = args[0];

        LeeLoc prueba = new LeeLoc();
        CreaLoc pruebaCrear = new CreaLoc(); // listo
        GeneralesLoc utilerias = new GeneralesLoc();

        ConexionRem conexion = new ConexionRem("app_int_aa", "10.225.228.51");
        CreaRem crearRemoto = new CreaRem(conexion);
        GeneralesRem genRem = new GeneralesRem(conexion);
        LeeRem lectRem = new LeeRem(conexion);


        //prueba.pruebaExcribe(palabraP);
//
//        System.out.println("ls con opciones ------------------------");
//        ArrayList<String> listado = prueba.listaDir("/home/appbalam_ingestas/INGESTAS_BALAM/pruebasJava", "-ls");
//        for (String linea : listado) {
//            System.out.println(linea);
//        }
//
//        System.out.println("ls sin opciones y directorio vacío ----------------------");
//        ArrayList<String> listado2 = prueba.listaDir("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/archivos_entrada_e1");
//        if (listado2.isEmpty())
//            System.out.println("lista vacia");
//        for (String linea : listado2) {
//            System.out.println(linea);
//        }
//
//        System.out.println("ls dir malo --------------------");
//        ArrayList<String> listado3 = prueba.listaDir("/home/appbalam_ingestas/INGESTAS_BALA");
//        if (listado3 == null)
//            System.out.println("Error");
//        else
//            System.out.println("correcto");
//
//        System.out.println("cat malo -------------------");
//        ArrayList<String> listado4 = prueba.leeArch("/home/appbalam_ingestas/INGESTAS_BA","prueba.sh", "");
//        if (listado4 == null)
//            System.out.println("Error");
//        else
//            System.out.println("correcto");
//
//        System.out.println("cat -------------------");
//        ArrayList<String> listado5 = prueba.leeArch("/home/appbalam_ingestas/INGESTAS_BALAM","prueba.sh");
//        for (String linea : listado5) {
//            System.out.println(linea);
//        }
//
//        System.out.println("cat con opciones -------------------");
//        ArrayList<String> listado6 = prueba.leeArch("/home/appbalam_ingestas/INGESTAS_BALAM/","prueba.sh", "-n");
//        for (String linea : listado6) {
//            System.out.println(linea);
//        }
//
//        System.out.println("mkdir -------------------");
//        int valorRet = pruebaCrear.creaDir("/home/appbalam_ingestas/INGESTAS_BALAM", "pruebaFunc", "");
//        System.out.println(valorRet);

//        System.out.println("mkdir remoto ------------------");
//        int mkdirRet = crearRemoto.creaDirRem("/home/export/AA/BALAM", "pruebaRem");
//        System.out.println(mkdirRet);
//
//        System.out.println("mkdir remoto con opciones ------------------");
//        int mkdirRet2 = crearRemoto.creaDirRem("/home/export/AA/BALAM/dirParent", "pruebaRem", "-p");
//        System.out.println(mkdirRet2);
//
//        System.out.println("scp --------------------");
//        int scpRet = crearRemoto.copiaRem("/home/appbalam_ingestas/INGESTAS_BALAM/datos_fs/salida/aa_abopreptar_gt", "ABOPREPTAR_20181010195921.txt.bz2", "/home/export/AA/BALAM/pruebaRem");
//        System.out.println(scpRet);
//
//        System.out.println("scp con opciones --------------------");
//        int scpRet2 = crearRemoto.copiaRem("/home/appbalam_ingestas/INGESTAS_BALAM/config/", "pruebas2", "/home/export/AA/BALAM/dirParent/pruebaRem", "-r");
//        System.out.println(scpRet2);

//        System.out.println("valida si existe directorio remoto -------------");
//        if (genRem.existeDirRem("/home/export/AA/BALA"))
//            System.out.println("Existe directorio");
//        else
//            System.out.println("No existe directorio");
//
//        System.out.println("valida si existe archivo remoto -------------");
//        if (genRem.existeArchRem("/home/export/AA/", "BALAM"))
//            System.out.println("Existe archivo");
//        else
//            System.out.println("No existe archivo");
//
//        System.out.println("creacion de comandos -------------");
//        String[] valida = utilerias.creaCmd("ls", "-l", "fichero");
//        for (String val : valida)
//            System.out.println(val);
//
//        System.out.println("creacion de comandos remotos -------------");
//        String[] validaRem = genRem.creaCmdRem("ls", "-l", "fichero");
//        for (String val : validaRem)
//            System.out.println(val);

        System.out.println("Lee directorio remoto ----------------");
        ArrayList<String> list1 = lectRem.listaDirRem("/home/export/AA/BALAM");
        for (String linea : list1) {
            System.out.println(linea);
        }

        System.out.println("Lee directorio remoto con opciones ----------------");
        ArrayList<String> list2 = lectRem.listaDirRem("/home/export/AA/BALAM/sql", "-tl");
        for (String linea : list2) {
            System.out.println(linea);
        }

        System.out.println("Lee archivo remoto ----------------");
        ArrayList<String> list3 = lectRem.leeArchRem("/home/export/AA/BALAM", "test.sh");
        for (String linea : list3) {
            System.out.println(linea);
        }

        System.out.println("Lee archivo remoto con opciones ----------------");
        ArrayList<String> list4 = lectRem.leeArchRem("/home/export/AA/BALAM", "test.sh", "-n");
        for (String linea : list4) {
            System.out.println(linea);
        }
    }
}
