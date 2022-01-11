package net.crunchdroid.controller.flow;

import net.crunchdroid.model.Flow;
import net.crunchdroid.repository.ConnectionRepository;
import net.crunchdroid.repository.FlowRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/flows-submit")
public class SubmitFlowController {

    @GetMapping("/list")
    public String list(@RequestParam(name = "value", required = false) String value, Model model) {
        return "flow/submit-flow";
    }

}
