package org.imokkkk.cortroller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ImOkkkk
 * @date 2021/10/24 18:01
 * @since 1.0
 */
@RestController
@RequestMapping("test")
public class TestController {

    @GetMapping("/hello")
    public String hello() {
        return "Hello World!";
    }
}
