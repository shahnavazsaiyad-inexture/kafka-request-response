package com.inexture.controller;

import com.inexture.dto.KafkaPartitions;
import com.inexture.dto.Message;
import com.inexture.service.KafkaClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Map;
import java.util.Objects;

@Controller
public class KafkaRequestController {

    @Autowired
    private KafkaClientService kafkaClientService;

    @GetMapping("/welcome/{name}")
    public ResponseEntity<?> welcome(@PathVariable String name){
        Message response = kafkaClientService.sendAndReceiveMessage(name, KafkaPartitions.WELCOME);
        if(Objects.isNull(response)){
            return ResponseEntity.badRequest().body(Map.of("error","Something is wrong"));
        }else{
            return ResponseEntity.ok(response);
        }
    }


    @GetMapping("/greet/{name}")
    public ResponseEntity<?> greet(@PathVariable String name){
        Message response = kafkaClientService.sendAndReceiveMessage(name, KafkaPartitions.GREET);
        if(Objects.isNull(response)){
            return ResponseEntity.badRequest().body(Map.of("error","Something is wrong"));
        }else{
            return ResponseEntity.ok(response);
        }
    }
}
