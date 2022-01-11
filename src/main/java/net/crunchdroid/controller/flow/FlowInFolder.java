package net.crunchdroid.controller.flow;


import net.crunchdroid.model.Flow;
import net.crunchdroid.oozie.OozieClientConfig;
import net.crunchdroid.service.FlowService;
import net.crunchdroid.shell.hdfs.HdfsBalam;
import net.crunchdroid.util.ResultResponse;
import org.apache.oozie.client.OozieClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

@Controller
@RequestMapping("/flows-folder")
public class FlowInFolder {

    private HdfsBalam hdfs = new HdfsBalam();

    @Autowired
    private FlowService flowService;


    @GetMapping("/list")
    public String list(Model model) {
        model.addAttribute("getFlowsInDirectory", flowService.getFlowsInDirectory());
        return "flow/list-flow-folder";
    }

    @RequestMapping("/showFile")
    public String ShowPath(
            @RequestParam("path") String path,
            @RequestParam("fileName") String fileName,
            Model model) throws OozieClientException {

        if (path != null && fileName != null) {
            ResultResponse result = hdfs.listFilesHadoop(path, fileName);
            if (!result.getResult()) {
                model.addAttribute("mensaje", result.getMessageError());
                model.addAttribute("getFlowsInDirectory", flowService.getFlowsInDirectory());
                return "flow/list-flow-folder";
            } else {
                model.addAttribute("FolderAndFile", (List<String>) result.getObject());
                return "flow/edit-folder";
            }
        } else {
            model.addAttribute("mensaje", "No se encontrarón archivos para este directorio");
            return "flow/list-flow-folder";
        }
    }

    @RequestMapping("/deleteFile")
    public String delete(@RequestParam("path") String path, Model model) throws IOException {

        model.addAttribute("deleteFile", hdfs.deleteFile(path));
        model.addAttribute("getFlowsInDirectory", flowService.getFlowsInDirectory());
        model.addAttribute("mensaje", "Se ha eliminado el archivo" + path);
        return "flow/list-flow-folder";

        //return "redirect:/flows-folder/list";
    }
}
