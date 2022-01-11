package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Flow;
import net.crunchdroid.model.Wf_bundles;
import net.crunchdroid.oozie.OozieClientConfig;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.service.FlowService;
import net.crunchdroid.service.SeparatorFieldService;
import net.crunchdroid.service.TableTypeService;
import net.crunchdroid.util.Utils;
import org.apache.oozie.client.OozieClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/flows")
public class FlowController {

    Logger logger = LoggerFactory.getLogger(SpoolFlowController.class);
    private Utils utils = new Utils();

    @Autowired
    private FlowService flowService;

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private SeparatorFieldService separatorFieldService;

    @Autowired
    private TableTypeService tableTypeService;

    private OozieClientConfig oozieClientConfig = new OozieClientConfig();

    @GetMapping("/list")
    public String list(@RequestParam(name = "value", required = false) String value, Model model) {
        if (value != null) {
            model.addAttribute("key", value);
            model.addAttribute("flowList", flowService.findByNameContainingIgnoreCase(value));
        } else {
            model.addAttribute("flowList", flowService.findAll());
        }
        return "flow/list-flow";
    }

    @GetMapping("/edit")
    public String edit(@RequestParam("id") Long id, Model model) {
        Flow flow = flowService.findOne(id);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        flow.setStartDate(sdf.format(new Date()));
        model.addAttribute("connectionList", connectionService.findAll());
        model.addAttribute("separatorList", separatorFieldService.findAll());
        model.addAttribute("flow", flow);

        if (flow.getType().equalsIgnoreCase("spool")) {
            return "flow/edit-spool-flow";
        } else if (flow.getType().equalsIgnoreCase("sqoop")) {
            model.addAttribute("tableList", tableTypeService.findAll());
            return "flow/edit-sqoop-flow";
        } else if (flow.getType().equalsIgnoreCase("archivo")) {
            return "flow/edit-file-flow";
        } else {
            return "redirect:/flows/list";
        }
    }

    @RequestMapping("/delete")
    public String delete(@RequestParam("id") Long id, @RequestParam("jobId") String jobId) {
        Flow foundFlow = flowService.findOne(id);
        if (foundFlow.getJobId() != null && foundFlow.getJobId().length() > 0) {
            try {
                oozieClientConfig.kill(jobId);
            } catch (OozieClientException e) {
                return "redirect:/flows/list";
            }
        }
        flowService.delete(id);
        return "redirect:/flows/list";
    }

    //Controlador que estoy usando para llenar los ComboBox de la vista flow-dependencies.
    @GetMapping("/dependencies")
    public String add_dependencies(Model model) {
        List<Flow> flows = flowService.findAll();
        Map<Long, String> name_flows = new HashMap<>();
        for (Flow flow : flows) {
            name_flows.put(flow.getId(), flow.getName());
        }
        //System.out.println("print name_flows" + name_flows);
        model.addAttribute("fill_select", name_flows);
        // model.addAttribute("dependencias", new Wf_flujos_dependencias());
        model.addAttribute("bundles", new Wf_bundles());
        return "flow/flow-dependencies";

    }

    @PostMapping("save_dependecies")
    public String save_dependencies(Wf_bundles bundles, Model model) {

        // Inician Métodos para Insertar
        utils.insert_dependencias(bundles.getCadena());
        utils.create_wfxml(bundles.getNombre());

        try {
            oozieClientConfig.CreateBundle(bundles.getNombre(), bundles.getFecha_inicio(), bundles.getFecha_fin(), bundles.getMinuto(), bundles.getHora(), bundles.getDiaSemana(), bundles.getMes(), bundles.getDiaMes());
        } catch (OozieClientException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "flow/flow-dependencies";

        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            model.addAttribute("message", e.getMessage());
            return "flow/flow-dependencies";
        }
        return "redirect:/flows/dependencies";
    }

}
