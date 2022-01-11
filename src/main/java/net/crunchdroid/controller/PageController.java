package net.crunchdroid.controller;

import net.crunchdroid.model.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.validation.Valid;

/**
 * @author CrunchDroid
 */
@Controller
public class PageController {

    Logger logger = LoggerFactory.getLogger(PageController.class);

    @GetMapping("/plain-page")
    public String plainPage(Model model) {
        logger.info("get plain page");
        model.addAttribute("base", new DataBaseType());
        return "plain-page";
    }

    @GetMapping("/pricing-tables")
    public String pricingTables() {
        return "pricing-tables";
    }


    private String errorMessage;

    @PostMapping("/plain-page")
    public String test(@ModelAttribute("base") DataBaseType base, Model model) {

        logger.info("post plain page");

        logger.info(base.getDbName());

        if (base.getDbName().equalsIgnoreCase("error")) {
            logger.info("dice error");
            errorMessage = "algo";
            model.addAttribute("message", errorMessage);
            model.addAttribute("typeMessage", "alert alert-error");
            return "plain-page";
        }

        model.addAttribute("messageSuccess", "OK");
        model.addAttribute("typeMessage", "alert alert-success");
        return "/plain-page";
    }

    @GetMapping("/")
    public String root() {
        return "redirect:/monitor/workflow";
    }

    @GetMapping("/login")
    public String login() {
        return "login";
    }

    @GetMapping("/login1")
    public String login1() {
        return "login1";
    }

    @GetMapping("/access-denied")
    public String accessDenied() {
        return "access-denied";
    }

    /*@GetMapping("/spool-flow")
    public String programmingFlow() {
        return "flow/spool-flow";
    }*/

    /*@GetMapping("/delete-user")
    public String deleteUser() {
        return "security/delete-user";
    }*/

}
