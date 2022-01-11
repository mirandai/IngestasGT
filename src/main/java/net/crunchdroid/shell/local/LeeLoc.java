package net.crunchdroid.shell.local;

import java.util.*;

public class LeeLoc {

    // Lista un directorio
    public ArrayList listaDir(String directorio, String opciones) {
        ArrayList lista = null;
        GeneralesLoc util = new GeneralesLoc();

        if (util.existeDir(directorio)) {
            String[] comando = util.creaCmd("ls", opciones, directorio);
            lista  = util.ejecCmdLista(comando);
        }

        return lista;
    }

    public ArrayList listaDir(String directorio) {
        return listaDir(directorio, "");
    }

    // Lee archivo
    public ArrayList leeArch(String directorio, String nomArch, String opciones) {
        ArrayList lista = null;
        GeneralesLoc util = new GeneralesLoc();

        String archCompleto = util.uneDir(directorio, nomArch);

        if (util.existeArch(archCompleto)) {
            String[] comando = util.creaCmd("cat", opciones, archCompleto);
            lista = util.ejecCmdLista(comando);
        }

        return lista;
    }

    public ArrayList leeArch(String directorio, String nomArch) {
        return leeArch(directorio, nomArch, "");
    }
}
