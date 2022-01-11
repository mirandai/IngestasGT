package net.crunchdroid.service;

import net.crunchdroid.model.User;
import net.crunchdroid.repository.RoleRepository;
import net.crunchdroid.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;


import java.util.HashSet;
import java.util.List;

@Service
@Component
public class UserServiceImpl implements UserService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RoleRepository roleRepository;

    @Autowired
    private BCryptPasswordEncoder bCryptPasswordEncoder;


    @Override
    public List<User> findAll() {
        return userRepository.findAll();
    }

    @Override
    public User findOne(Long id) {
        //return userRepository.findById(id);
        return userRepository.findOne(id);
    }

    @Override
    public User save(User user) {
        //user.setPassword(bCryptPasswordEncoder.encode(user.getPassword()));
        //user.setRoles(new HashSet<>(roleRepository.findAll()));
        return userRepository.save(user);
    }

    @Override
    public void delete(Long id) {
        userRepository.delete(id);
    }

    @Override
    public List<User> findByUsernameContainingIgnoreCase(String value) {
        return userRepository.findByUsernameContainingIgnoreCase(value);
    }

    @Override
    public User findByUsername(String username) {
        return null;
    }

}
