package net.crunchdroid.shell.remote;

import net.crunchdroid.shell.local.GeneralesLoc;

import java.util.*;

public class GeneralesRem {

    ConexionRem conexion = null;

    public GeneralesRem(ConexionRem conexionRem) {
        this.conexion = conexionRem;
    }

    // Verificar si existe directorio en servidor remoto
    public boolean existeDirRem(String directorio) {
        String destino = conexion.getUsuario() + "@" + conexion.getServidor();
        String valida = "[ -d " + directorio + " ]";
        String[] comando = {"ssh", destino, valida};
        GeneralesLoc util = new GeneralesLoc();

        int respVal = util.ejecCmd(comando);

        if (respVal == 0)
            return true;
        else
            return false;
    }

    // Verificar si existe archivo en servidor remoto
    public boolean existeArchRem(String archivo) {
        String destino = conexion.getUsuario() + "@" + conexion.getServidor();
        String valida = "[ -f " + archivo + " ]";
        String[] comando = {"ssh", destino, valida};
        GeneralesLoc util = new GeneralesLoc();

        int respVal = util.ejecCmd(comando);

        if (respVal == 0)
            return true;
        else
            return false;
    }

    // Crear comando remoto
    public String[] creaCmdRem(String cmdRem, String opciones, String dirFich) {
        ArrayList<String> cmdCompleto = new ArrayList<String>();
        String destino = conexion.getUsuario() + "@" + conexion.getServidor();

        cmdCompleto.add("ssh");
        cmdCompleto.add(destino);
        cmdCompleto.add(cmdRem);

        if (!opciones.isEmpty())
            cmdCompleto.add(opciones);

        cmdCompleto.add(dirFich);

        String[] cmdDev = new String[cmdCompleto.size()];
        cmdCompleto.toArray(cmdDev);

        return cmdDev;
    }
}
