package net.crunchdroid.repository;

import net.crunchdroid.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    List<User> findByUsernameContainingIgnoreCase(String value);

    User findByUsername(String username);

    void deleteById(Long id);

}
