package net.crunchdroid.shell.local;

public class CreaLoc {

    // Crea directorio
    public int creaDir (String dirBase, String newDir, String opciones) {
        GeneralesLoc util = new GeneralesLoc();

        String directorio = util.uneDir(dirBase, newDir);
        String[] comando = util.creaCmd("mkdir", opciones, directorio);

        return util.ejecCmd(comando);
    }

    public int creaDir (String dirBase, String newDir) {
        return creaDir(dirBase, newDir, "");
    }
}
