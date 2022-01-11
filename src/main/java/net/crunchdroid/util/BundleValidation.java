package net.crunchdroid.util;

import net.crunchdroid.model.Wf_bundles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;


public class BundleValidation {

    Logger logger = LoggerFactory.getLogger(FlowValidation.class);

    public ResultResponse validateFields(Wf_bundles bundle) {

        List<String> validations = new ArrayList<>();

        if (stringIsNullOrEmpty(bundle.getNombre())) {
            validations.add("Nombre no puede ser null o vació.");
        }
        if (isWhiteSpaceField(bundle.getNombre())) {
            validations.add("Nombre no debe contener espacios en blanco");
        }

        if (stringIsNullOrEmpty(bundle.getMinuto())) {
            validations.add("Minuto no puede ser nulo o vacío.");
        }

        if (isWhiteSpaceField(bundle.getMinuto())) {
            validations.add("Minuto no debe contener espacios en blanco.");
        }

        if (stringIsNullOrEmpty(bundle.getHora())) {
            validations.add("Hora no puede ser nulo o vacío.");
        }

        if (isWhiteSpaceField(bundle.getHora())) {
            validations.add("Hora no debe contener espacios en blanco.");
        }

        if (stringIsNullOrEmpty(bundle.getDiaSemana())) {
            validations.add("Día de la semana no puede ser nulo o vacío.");
        }

        if (isWhiteSpaceField(bundle.getDiaSemana())) {
            validations.add("Dias de la semana no debe contener espacios en blanco.");
        }

        if (stringIsNullOrEmpty(bundle.getMes())) {
            validations.add("Mes no puede ser nulo o vacío..");
        }

        if (isWhiteSpaceField(bundle.getMes())) {
            validations.add("Mes no debe contener espacio en blanco..");
        }

        if (stringIsNullOrEmpty(bundle.getDiaMes())) {
            validations.add("Día del mes no puede ser nulo o vacío.");
        }

        if (isWhiteSpaceField(bundle.getDiaMes())) {
            validations.add("Día del mes no debe contener espacios en blanco.");
        }

        if (stringIsNullOrEmpty(bundle.getFecha_inicio())) {
            validations.add("Fecha de inicio no puede ser nulo o vacío.");
        }

        if (stringIsNullOrEmpty(bundle.getFecha_inicio())) {
            validations.add("Fecha de finalización no puede ser nulo o vacío.");
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        if ((!stringIsNullOrEmpty(bundle.getFecha_inicio()) && bundle.getFecha_inicio() != "") && (!stringIsNullOrEmpty(bundle.getFecha_fin()) && bundle.getFecha_fin() != "")) {
            Date date1 = null;
            try {
                date1 = format.parse(bundle.getFecha_inicio());
            } catch (ParseException e) {
                validations.add("Fecha de inicio " + e.getMessage());
            }
            Date date2 = null;
            try {
                date2 = format.parse(bundle.getFecha_fin());
            } catch (ParseException e) {
                validations.add("Fecha de finalización " + e.getMessage());
            }

            Date current = new Date();
            try {
                current = format.parse(new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).format(current));
            } catch (ParseException e) {
                validations.add("Fecha actual " + e.getMessage());
            }

            if (date1.compareTo(current) < 0) {
                logger.info(date1.toString() + "" + current.toString());
                logger.info("Fecha de inicio es menor a la fecha actual.");
                validations.add("Fecha de inicio es menor a la fecha actual.");
            }

            if (date1.compareTo(date2) > 0) {
                logger.info("Fecha de finalización es menor a la fecha de inicio.");
                validations.add("Fecha de finalización es menor a la fecha de inicio.");
            }
        }//cierra llave de fechas

        if (!validations.isEmpty() || validations.size() > 0) {
            return new ResultResponse(false, validations);
        }

        logger.info(bundle.toString());
        return new ResultResponse(true, null);
    }//hasta aca cierra el if

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

    public boolean isWhiteSpaceField(String str) {
        if (str == null) {
            return false;
        }

        for (int i = 0; i < str.length(); i++) {
            if (Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            logger.info(str + " is numeric.");
            return true;
        } catch (Exception e) {
            logger.info(str + " is not numeric.");
            return false;
        }
    }
}

