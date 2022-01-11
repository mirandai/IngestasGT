package net.crunchdroid.controller.database;

import net.crunchdroid.model.Connection;
import net.crunchdroid.repository.ConnectionRepository;
import net.crunchdroid.service.ConnectionService;
import net.crunchdroid.util.ConnectionValidation;
import net.crunchdroid.util.ResultResponse;
import net.crunchdroid.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("/connections")
public class ConnectionController {

    Logger logger = LoggerFactory.getLogger(ConnectionController.class);

    @Autowired
    private ConnectionService connectionService;

    private ConnectionValidation validation = new ConnectionValidation();

    @GetMapping("/list")
    public String list(@RequestParam(name = "value", required = false) String value, Model model) {

        if (value != null) {
            model.addAttribute("key", value);
            model.addAttribute("connectionList", connectionService.findByConnectionNameContainingIgnoreCase(value));
        } else {
            model.addAttribute("connectionList", connectionService.findAll());
        }
        return "database/list-connection";
    }

    @GetMapping("/create")
    public String create(Model model) {
        model.addAttribute("connection", new Connection());
        return "database/create-connection";
    }

    @PostMapping("/save")
    public String save(Connection connection, Model model) {


        ResultResponse result = validation.validateFields(connection);
        if (!result.getResult()) {
            List<String> validations = (List<String>) result.getObject();
            model.addAttribute("messageValidation", "Has errors");
            model.addAttribute("validations", validations);
            return "database/create-connection";
        }

        Connection saveConnection = connectionService.save(connection);
        return "redirect:/connections/list";
    }

    @GetMapping("/edit")
    public String edit(@RequestParam("id") Long id, Model model) {
        model.addAttribute("connection", connectionService.findOne(id));
        return "database/edit-connection";
    }

    @RequestMapping("/delete")
    public String delete(@RequestParam("id") Long id) {
        connectionService.delete(id);
        return "redirect:/connections/list";
    }

    @GetMapping("/test")
    public String test(@RequestParam("id") Long id, Model model) {

        Utils utils = new Utils();

        Connection connTest = connectionService.findOne(id);
        String resultInfo = "";

        String basetype = "";
        if (connTest.getDataBase() == 1) {
            basetype = "ORACLE";
        } else if (connTest.getDataBase() == 2) {
            basetype = "MYSQL";
        } else if (connTest.getDataBase() == 3) {
            basetype = "HIVE";
        }

        try {
            resultInfo = utils.setTest(basetype,
                    connTest);
            model.addAttribute("message", null);
            model.addAttribute("messageSuccess", resultInfo);
            logger.info("result: " + resultInfo);
        } catch (Exception e) {
            model.addAttribute("messageSuccess", null);
            model.addAttribute("message", e.getMessage());
            logger.error(e.getMessage());
        }

        //return "redirect:/connections/list";
        model.addAttribute("connectionList", connectionService.findAll());
        return "database/list-connection";
    }
}
