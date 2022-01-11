package net.crunchdroid.controller.security;

import net.crunchdroid.repository.UserRepository;
import net.crunchdroid.model.User;
import net.crunchdroid.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/users")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping("/list")
    public String list(@RequestParam(name = "value", required = false) String value, Model model) {
        if (value != null) {
            model.addAttribute("key", value);
            model.addAttribute("userList", userService.findByUsernameContainingIgnoreCase(value));
        } else {
            model.addAttribute("userList", userService.findAll());
        }
        return "security/list-user";
    }

    @GetMapping("/create")
    public String create(Model model) {
        model.addAttribute("user", new User());
        return "security/create-user";
    }

    @GetMapping("/edit")
    public String edit(@RequestParam("id") Long id, Model model) {
        model.addAttribute("user", userService.findOne(id));
        return "security/edit-user";
    }

    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @PostMapping("/save")
    public String save(User user) {
        BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
        user.setPassword(passwordEncoder.encode(user.getPassword()));
        user.setRole("ROLE_USER");
        User saveUser = userService.save(user);
        return "redirect:/users/list";
    }

    @RequestMapping("/delete")
    public String delete(@RequestParam("id") Long id) {
        userService.delete(id);
        return "redirect:/users/list";
    }


}
