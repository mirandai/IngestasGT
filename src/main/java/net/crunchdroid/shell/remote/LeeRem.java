package net.crunchdroid.shell.remote;

import net.crunchdroid.shell.local.GeneralesLoc;

import java.util.*;

public class LeeRem {

    ConexionRem conexion = null;

    public LeeRem(ConexionRem conexionRem) {
        this.conexion = conexionRem;
    }

    // Lista un directorio
    public ArrayList listaDirRem(String directorio, String opciones) {
        ArrayList lista = null;
        GeneralesRem utilRem = new GeneralesRem(this.conexion);
        GeneralesLoc util = new GeneralesLoc();

        if (utilRem.existeDirRem(directorio)) {
            String[] comando = utilRem.creaCmdRem("ls", opciones, directorio);
            lista = util.ejecCmdLista(comando);
        }

        return lista;
    }

    public ArrayList listaDirRem(String directorio) {
        return listaDirRem(directorio, "");
    }

    // Lee archivo
    public ArrayList leeArchRem(String directorio, String nomArch, String opciones) {
        ArrayList lista = null;
        GeneralesLoc util = new GeneralesLoc();
        GeneralesRem utilRem = new GeneralesRem(this.conexion);

        String archCompleto = util.uneDir(directorio, nomArch);

        if (utilRem.existeArchRem(archCompleto)) {
            String[] comando = utilRem.creaCmdRem("cat", opciones, archCompleto);
            lista = util.ejecCmdLista(comando);
        }

        return lista;
    }

    public ArrayList leeArchRem(String directorio, String nomArch) {
        return leeArchRem(directorio, nomArch, "");
    }
}
