package net.crunchdroid.util;

import net.crunchdroid.model.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class FlowValidation {

    Logger logger = LoggerFactory.getLogger(FlowValidation.class);

    public ResultResponse validateFields(Flow flow) {

        List<String> validations = new ArrayList<>();

        if (stringIsNullOrEmpty(flow.getName())) {
            validations.add("Nombre no puedse ser nulo o vació.");
        }

        if (isWhiteSpaceField(flow.getName())) {
            validations.add("Nombre no debe contener espacios en blanco.");
        }

        if (stringIsNullOrEmpty(flow.getDescription())) {
            validations.add("Descripción no puede ser nulo o vacío.");
        }

        if (stringIsNullOrEmpty(flow.getCountry())) {
            validations.add("País no puede ser nulo o vacío.");
        }

        if (!flow.getType().equalsIgnoreCase("archivo")) {
            if (longIsNullOrEmpty(flow.getConnectionId())) {
                validations.add("Conexión no puede ser nulo o vacío.");
            }

            if (stringIsNullOrEmpty(flow.getQuery())) {
                validations.add("Consulta no puede ser nulo o vacío.");
            }

            if (stringIsNullOrEmpty(flow.getSeparator_())) {
                validations.add("Separador no puede ser nulo o vacío.");
            }

            if (stringIsNullOrEmpty(flow.getFilename())) {
                validations.add("Nombre del archivo no puede ser nulo o vació.");
            }
        } else {
            if (stringIsNullOrEmpty(flow.getSourceDirectory())) {
                validations.add("Directorio fuente no puede ser nulo o vació.");
            }
            if (stringIsNullOrEmpty(flow.getFilename())) {
                validations.add("Nombre del archivo no puede ser nulo o vació.");
            }
        }


        if (isWhiteSpaceField(flow.getFilename())) {
            validations.add("Nombre del archivo debe contener espacios en blanco.");
        }


        if (stringIsNullOrEmpty(flow.getIsSchedule())) {
            validations.add("Programar ahora no puede ser nulo.");
        } else {
            if (flow.getIsSchedule().equalsIgnoreCase("yes")) {
                if (stringIsNullOrEmpty(flow.getMinute())) {
                    validations.add("Minuto no puede ser nulo o vacío.");
                }

                if (isWhiteSpaceField(flow.getMinute())) {
                    validations.add("Minuto no debe contener espacios en blanco.");
                }

                if (stringIsNullOrEmpty(flow.getHour())) {
                    validations.add("Hora no puede ser nulo o vacío.");
                }

                if (isWhiteSpaceField(flow.getHour())) {
                    validations.add("Hora no debe contener espacios en blanco.");
                }

                if (stringIsNullOrEmpty(flow.getWeekday())) {
                    validations.add("Día de la semana no puede ser nulo o vacío.");
                }

                if (isWhiteSpaceField(flow.getWeekday())) {
                    validations.add("Dias de la semana no debe contener espacios en blanco.");
                }

                if (stringIsNullOrEmpty(flow.getMonth())) {
                    validations.add("Mes no puede ser nulo o vacío..");
                }

                if (isWhiteSpaceField(flow.getMonth())) {
                    validations.add("Mes no debe contener espacio en blanco..");
                }

                if (stringIsNullOrEmpty(flow.getMonthday())) {
                    validations.add("Día del mes no puede ser nulo o vacío.");
                }

                if (isWhiteSpaceField(flow.getMonthday())) {
                    validations.add("Día del mes no debe contener espacios en blanco.");
                }

                if (stringIsNullOrEmpty(flow.getStartDate())) {
                    validations.add("Fecha de inicio no puede ser nulo o vacío.");
                }

                if (stringIsNullOrEmpty(flow.getEndDate())) {
                    validations.add("Fecha de finalización no puede ser nulo o vacío.");
                }
            }//fin es si programable ahora
        }//fin if programación ahora.

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        if ((!stringIsNullOrEmpty(flow.getStartDate()) && flow.getStartDate() != "") && (!stringIsNullOrEmpty(flow.getEndDate()) && flow.getEndDate() != "")) {
            Date date1 = null;
            try {
                date1 = format.parse(flow.getStartDate());
            } catch (ParseException e) {
                validations.add("Fecha de inicio " + e.getMessage());
            }
            Date date2 = null;
            try {
                date2 = format.parse(flow.getEndDate());
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
        }

        if (flow.getType().equalsIgnoreCase("sqoop")) {
            if (stringIsNullOrEmpty(flow.getQueueSqoop())) {
                validations.add("Cola sqoop no puede ser nulo o vacío.");
            }

            if (isWhiteSpaceField(flow.getQueueSqoop())) {
                validations.add("Cola sqoop no debe contener espacios en blanco.");
            }

            if (stringIsNullOrEmpty(flow.getTableSize())) {
                validations.add("Tamaño de la tabla no puede ser nulo o vacío.");
            } else {
                if (flow.getTableSize().equalsIgnoreCase("GRANDE") || flow.getTableSize().equalsIgnoreCase("MEDIANA")) {
                    if (stringIsNullOrEmpty(flow.getSplit())) {
                        validations.add("Ha seleccionado " + flow.getTableSize() + ", no debe dejar el campo split vacío.");
                    }

                    if (isWhiteSpaceField(flow.getSplit())) {
                        validations.add("Split no debe contener espacios en blanco.");
                    }

                    if (stringIsNullOrEmpty(flow.getFileSize())) {
                        validations.add("Ha seleccionado " + flow.getTableSize() + ", no debe dejar el campo file size en blanco.");
                    }

                    if (isWhiteSpaceField(flow.getFileSize())) {
                        validations.add("File size no debe contener espacios en blanco.");
                    }

                    if (!isNumeric(flow.getFileSize())) {
                        validations.add("File size debe ser numérico.");
                    }

                    if (stringIsNullOrEmpty(flow.getMappers())) {
                        validations.add("Ha seleccionado " + flow.getTableSize() + ", no debe dejar el campo mappers en blanco.");
                    }

                    if (isWhiteSpaceField(flow.getMappers())) {
                        validations.add("Mappers no debe contener espacios en blanco.");
                    }

                    if (!isNumeric(flow.getMappers())) {
                        validations.add("Mappers debe ser numérico.");
                    }
                }
            }

        }

        if (stringIsNullOrEmpty(flow.getEmails())) {
            validations.add("Correos no puede ser nulo o vacío.");
        }

        Utils utils = new Utils();

        if (utils.validateEmail(flow.getEmails())) {
            validations.add("Una o más direcciones de correo son inválidas.");
        }

        if (flow.getAlertByEmail()) {
            if (stringIsNullOrEmpty(flow.getPercentageToleranceRecords())) {
                validations.add("Porcentage de tolerancia de registros no puede ser nulo o vacío.");
            }
            if (isWhiteSpaceField(flow.getPercentageToleranceRecords())) {
                validations.add("Porcentaje de tolerancia de registros no debe contener espacios en blanco.");
            }
            if (!isNumeric(flow.getPercentageToleranceRecords())) {
                validations.add("Porcentaje de tolerancia de registros debe ser numérico.");
            }
        }

        if (stringIsNullOrEmpty(flow.getIsDirectory())) {
            validations.add("Destino no puede ser nulo o vacío.");
        } else {

            if (flow.getIsDirectory().equalsIgnoreCase("carpeta")) {
                if (stringIsNullOrEmpty(flow.getDirectory())) {
                    validations.add("Ha seleccionado directorio, no debe dejar el campo directorio en blanco.");
                }

                if (isWhiteSpaceField(flow.getDirectory())) {
                    validations.add("Directorio no debe contener espacios en blanco.");
                }
            } else if (flow.getIsDirectory().equalsIgnoreCase("tabla")) {
                if (stringIsNullOrEmpty(flow.getQueue()) && flow.getType().equalsIgnoreCase("spool")) {
                    validations.add("Ha seleccionado tabla, no debe dejar el campo cola en blanco.");
                }
                if (isWhiteSpaceField(flow.getQueue())) {
                    validations.add("Cola no pude contener espacios en blanco.");
                }

                if (stringIsNullOrEmpty(flow.getSchemaDatabase())) {
                    validations.add("Ha seleccionado tabla, no debe dejar el campo esquema en blanco.");
                }
                if (isWhiteSpaceField(flow.getSchemaDatabase())) {
                    validations.add("Esquema no debe contener espacios en blanco.");
                }

                if (stringIsNullOrEmpty(flow.getTablename())) {
                    validations.add("Ha seleccionado tabla, no debe dejar el campo nombre de la tabla en blanco.");
                }
                if (isWhiteSpaceField(flow.getTablename())) {
                    validations.add("Nombre de la tabla no debe contener espacios en blanco.");
                }
                if (flow.getParticioned()) {
                    if (stringIsNullOrEmpty(flow.getParticionedField())) {
                        validations.add("Ha seleccionado tabla y particinado, no debe dejar el campo partición en blanco.");
                    }
                    if (isWhiteSpaceField(flow.getParticionedField())) {
                        validations.add("Campo de partición no debe contener espacios en blanco.");
                    }
                }
            }
        }

        if (!validations.isEmpty() || validations.size() > 0) {
            return new ResultResponse(false, validations);
        }

        logger.info(flow.toString());

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
