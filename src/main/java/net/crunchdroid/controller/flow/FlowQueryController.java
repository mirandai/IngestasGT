package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Connection;
import net.crunchdroid.model.Flow;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.service.FlowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/flowsQuery")
public class FlowQueryController {
    @Autowired
    private FlowService flowService;

    @Autowired
    private ConnectionService connectionService;

    @GetMapping("/list")
    public String ListFlow2(Model model) {
        List<Flow> flows = flowService.findAll();
        Map<Long, String> connections = new HashMap<>();
        for (Flow flow : flows) {
            if (flow.getConnectionId() != null) {
                String connectionName = connectionService.findOne(flow.getConnectionId()).getConnectionName();
                connections.put(flow.getConnectionId(), connectionName);
            }
        }

        model.addAttribute("flowsList", flows);
        model.addAttribute("connectionList", connections);
        return "flow/consulta-flujos";

    }

}
