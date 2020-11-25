package io.confluent.examples.streams.processor;//package com.immomo.moaservice.recommend.stream;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author ceyhunuzunoglu
 */
@Api(value = "State Store Rest API")
@RestController
@RequestMapping("api")
public class StateStoreRestController {

    private final StateStoreQueryService stateStoreQueryService;

    public StateStoreRestController(StateStoreQueryService stateStoreQueryService) {
        this.stateStoreQueryService = stateStoreQueryService;
    }

    @ApiOperation("Get all listeners ids")
    @GetMapping("/get/all")
    public List<UserHeartBeat> getAll() {
        return stateStoreQueryService.getAllListeners();
    }

    @ApiOperation("Get songs of a listener by id")
    @GetMapping("/get/songs")
    public String getById(@RequestParam String id) {
        return stateStoreQueryService.getListenerSongs(id);
    }
}


