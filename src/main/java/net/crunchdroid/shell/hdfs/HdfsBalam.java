package net.crunchdroid.shell.hdfs;


import net.crunchdroid.util.ResultResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsBalam {

    Logger logger = LoggerFactory.getLogger(HdfsBalam.class);

    public HdfsBalam() {
    }

    public void copyFromLocal(String source, String dest) throws IOException {
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://10.231.236.4:8020");

        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        conf.set("fs.defaultFS", prop.getProperty("fs_defaultFS"));
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem fileSystem = FileSystem.get(conf);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(dstPath))) {
            System.out.println("No such destination " + dstPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try {
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            System.out.println("File " + filename + "copied to " + dest);
        } catch (Exception e) {
            System.err.println("Exception caught! :" + e);
            System.exit(1);
        } finally {
            fileSystem.close();
        }
    }

    public void mkdir(String dir) throws IOException {
        Configuration conf = new Configuration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        //ver achivo path.Properties para ver el valor de la variable fs_defaultFS
        conf.set("fs.defaultFS", prop.getProperty("fs_defaultFS"));
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already exists!");
            return;
        } else {
            System.out.println("Dir" + " " + dir + " " + "creado con exito");
        }

        fileSystem.mkdirs(path);
        fileSystem.close();
    }

    //Checking if a file exists in HDFS
    public boolean validateIfExists(String source) throws IOException {
        Boolean status = false;

        Configuration conf = new Configuration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        conf.set("fs.defaultFS", prop.getProperty("fs_defaultFS"));
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(source);
        if (hdfs.exists(path)) {
            System.out.println("Dir " + source + " " + " ya existe!");
            status = true;
            return status;

        } else {
            System.out.println("Dir" + source + " " + "No existe!");
            return status;
        }
    }

    public boolean ifExists(Path source) throws IOException {

        Configuration config = new Configuration();

        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        config.set("fs.defaultFS", prop.getProperty("fs_defaultFS"));
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        config.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem hdfs = FileSystem.get(config);
        boolean isExists = hdfs.exists(source);
        System.out.println("------------->:" + " " + isExists);
        return isExists;
    }

    //Funcion para Listar Archivos del Hdfs
    public ResultResponse listFilesHadoop(String hdfsPath, String fileName) {

        List<String> listaArchivos = new ArrayList<>();

        Configuration config = new Configuration();
        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        config.set("fs.defaultFS", prop.getProperty("fs_defaultFS"));
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        config.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem fs = null;

        try {
            fs = FileSystem.get(config);
        } catch (IOException e) {
            logger.info(e.toString());
            return new ResultResponse(false, null, e.getMessage());
        }

        FileStatus[] fileStatus = new FileStatus[0];

        try {
            fileStatus = fs.listStatus(new Path(hdfsPath));
        } catch (IOException e) {
            logger.info(e.toString());
            return new ResultResponse(false, null, e.getMessage());

        }
        String parametro = "";
        if (fileName.contains(".")) {
            String[] var = fileName.split("\\.");
            parametro = var[0];
        } else {
            parametro = fileName;
        }

        for (FileStatus status : fileStatus) {
            String filename = status.getPath().getName();

            String escapedName = Pattern.quote(parametro);
           // String pattern = escapedName + "(_[0-9]+)+(.[a-zA-z0-9]+)+";
            String pattern = escapedName + "(_[0-9]+)*(.[a-zA-z0-9]+)+";
            Pattern regex = Pattern.compile(pattern);
            Matcher matcher = regex.matcher(filename);

            if (matcher.matches()) {
                listaArchivos.add(status.getPath().toString());
            }
        }
        return new ResultResponse(true, listaArchivos, null);
    }


    //Función para borrar arcivos del hdfs
    public String deleteFile(String file) throws IOException {
        Configuration config = new Configuration();

        Properties prop = new Properties();
        InputStream is = null;

        try {
            is = new FileInputStream("path.properties");
            prop.load(is);
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        config.set("fs.defaultFS", prop.getProperty("fs_defaultFS"));
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        config.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem fileSystem = FileSystem.get(config);
        Path path = new Path(file);

        System.out.println("file:" + file);

        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
        return file;
    }


}
