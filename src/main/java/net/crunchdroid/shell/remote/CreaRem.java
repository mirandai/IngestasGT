package net.crunchdroid.shell.remote;

import net.crunchdroid.shell.local.GeneralesLoc;

public class CreaRem {

    ConexionRem conexion = null;

    public CreaRem(ConexionRem conexionRem) {
        this.conexion = conexionRem;
    }

//    public int conecta() {
//        String destino = conexion.getUsuario() + "@" + conexion.getServidor();
//        String[] comando = {"ssh", destino};
//        GeneralesLoc util = new GeneralesLoc();
//
//        return util.ejecCmd(comando);
//    }

    //  Copia archivo local en servidor remoto
    public int copiaRem(String dirLocal, String archLocal, String dirRemoto, String opciones) {
        int respEJec = -666; // no existe directorio remoto
        String destino = conexion.getUsuario() + "@" + conexion.getServidor() + ":" + dirRemoto;
        String[] comando;
        GeneralesLoc util = new GeneralesLoc();
        GeneralesRem utilRem = new GeneralesRem(this.conexion);

        String fichLocal = util.uneDir(dirLocal, archLocal);

        if (utilRem.existeDirRem(dirRemoto)) {
            if (opciones.isEmpty())
                comando = new String[]{"scp", fichLocal, destino};
            else
                comando = new String[]{"scp", opciones, fichLocal, destino};

            respEJec = util.ejecCmd(comando);
        }

        return respEJec;
    }

    public int copiaRem(String dirLocal, String archLocal, String dirRemoto) {
        return copiaRem(dirLocal, archLocal, dirRemoto, "");
    }

    // Crea directorio en servidor remoto
    public int creaDirRem(String dirBaseRemoto, String newDir, String opciones) {
        int respEjec = -666;
        GeneralesLoc util = new GeneralesLoc();
        GeneralesRem utilRem = new GeneralesRem(this.conexion);

        String newDirRem = util.uneDir(dirBaseRemoto, newDir);

        if ((utilRem.existeDirRem(dirBaseRemoto) && !utilRem.existeDirRem(newDirRem) && opciones.isEmpty()) || !opciones.isEmpty()) {
            String[] comando = utilRem.creaCmdRem("mkdir", opciones, newDirRem);
            respEjec = util.ejecCmd(comando);
        }

        return respEjec;
    }

    public int creaDirRem(String dirBaseRemoto, String newDir) {
        return creaDirRem(dirBaseRemoto, newDir, "");
    }
}
