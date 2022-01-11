package net.crunchdroid.service;

import net.crunchdroid.model.User;

import java.util.List;

public interface UserService {

    List<User> findAll();

    User findOne(Long id);

    User save(User user);

    void delete(Long id);

    List<User> findByUsernameContainingIgnoreCase(String value);

    User findByUsername(String username);

}
