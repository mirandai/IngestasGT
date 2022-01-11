package net.crunchdroid.shell.remote;

public class ConexionRem {

    String usuario;
    String servidor;

    public ConexionRem (String usuario, String servidor) {
        this.usuario = usuario;
        this.servidor = servidor;
    }

    public String getUsuario() {
        return usuario;
    }

    public void setUsuario(String usuario) {
        this.usuario = usuario;
    }

    public String getServidor() {
        return servidor;
    }

    public void setServidor(String servidor) {
        this.servidor = servidor;
    }
}
