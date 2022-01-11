package net.crunchdroid.util;

import net.crunchdroid.model.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConnectionValidation {

    Logger logger = LoggerFactory.getLogger(ConnectionValidation.class);

    public ResultResponse validateFields(Connection connection) {

        List<String> validations = new ArrayList<>();

        if (stringIsNullOrEmpty(connection.getConnectionName())) {
            validations.add("Nombre de la conexión no puede ser nulo o vacío.");
        }

        if (intIsNullOrEmpty(connection.getDataBase())) {
            validations.add("Tipo de base de datos no puede ser nulo o vacío.");
        }

        if (stringIsNullOrEmpty(connection.getDbUser())) {
            validations.add("Usuario no puede ser nulo o vacío.");
        }

        if (stringIsNullOrEmpty(connection.getPassword())) {
            validations.add("Contraseña no puede ser nulo o vacío.");
        }

        if (stringIsNullOrEmpty(connection.getHost())) {
            validations.add("Dirección IP no puede ser nulo o vacío");
        }

        if (stringIsNullOrEmpty(connection.getPort())) {
            validations.add("Puerto no puede ser nulo o vacío.");
        }

        if (connection.getDataBase() == 1) {
            if (stringIsNullOrEmpty(connection.getType())) {
                validations.add("Tipo de conexiòn no pueder ser nulo o vacío cuando es una base de tipo oracle.");
            }
        }

        if (stringIsNullOrEmpty(connection.getSid())) {
            validations.add("SID no puede ser nulo o vacío.");
        }

        if (!validations.isEmpty() || validations.size() > 0) {
            return new ResultResponse(false, validations);
        }

        logger.info(connection.toString());

        return new ResultResponse(true, null);
    }

    public boolean stringIsNullOrEmpty(String str) {
        if (null == str || str.equalsIgnoreCase(null) || str.isEmpty() || str == "" || str.length() <= 0) {
            return true;
        }
        return false;
    }

    public boolean longIsNullOrEmpty(Long lng) {
        if (lng == null || lng <= 0) {
            return true;
        }
        return false;
    }

    public boolean intIsNullOrEmpty(int i) {
        if (i <= 0) {
            return true;
        }
        return false;
    }
}
