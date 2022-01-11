package net.crunchdroid.config;

import com.jcraft.jsch.UserInfo;
import net.crunchdroid.model.User;
import net.crunchdroid.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class MyAppUserDetailsService implements UserDetailsService {

    @Autowired
    private UserRepository userRepository;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        User activeUserInfo = userRepository.findByUsername(username);
        GrantedAuthority authority = new SimpleGrantedAuthority(activeUserInfo.getRole());
        UserDetails userDetails = new org.springframework.security.core.userdetails.User(activeUserInfo.getUsername(),
                activeUserInfo.getPassword(), Arrays.asList(authority));
        return userDetails;
    }
}
