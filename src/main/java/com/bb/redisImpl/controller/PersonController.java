package com.bb.redisImpl.controller;

import com.bb.redisImpl.models.Person;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class PersonController {
    private static final String PERSON_KEY_PREFIX = "per::";
    private static final String PERSON_LIST_KEY = "person_list";
    private static final String PERSON_HASH_KEY_PREFIX = "person_hash";

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    @Autowired
    ObjectMapper objectMapper;

    //Key(String) - Value(String)
    @PostMapping("/string/person")
    public void savePerson(@RequestBody Person person){
        if(person.getId() == 0){
            return;
        }
        String key = getKey(person.getId());
        redisTemplate.opsForValue().set(key, person);
    }

    @GetMapping("/string/person")
    public Person getPerson(@RequestParam("id") long id){
        String key = getKey(id);
        return (Person) redisTemplate.opsForValue().get(key);
    }

    private String getKey(long id){
        return PERSON_KEY_PREFIX + id;
    }

    //Key(String) - Value(List)
    @PostMapping("/lpush/person")
    public void lpush(@RequestBody List<Person> person){
        redisTemplate.opsForList().leftPush(PERSON_LIST_KEY , person);
    }

    @PostMapping("/rpush/person")
    public void rpush(@RequestBody List<Person> person){
        redisTemplate.opsForList().rightPush(PERSON_LIST_KEY , person);
    }

    @GetMapping("/lrange/person")
    public List<Person> lrange(@RequestParam(value = "start", required = false, defaultValue = "0") int start,
                               @RequestParam(value = "end", required = false, defaultValue = "-1") int end){
        return redisTemplate
                .opsForList()
                .range(PERSON_LIST_KEY, start, end)
                .stream()
                .map(x -> (Person)x)
                .collect(Collectors.toList());
    }

    @GetMapping("/lpop/person")
    public List<Person> lpop(@RequestParam(value="count", required = false, defaultValue = "1") int count){
        return redisTemplate.opsForList()
                .leftPop(PERSON_LIST_KEY, count)
                .stream()
                .map(x -> (Person)x)
                .collect(Collectors.toList());
    }

    @GetMapping("/rpop/person")
    public List<Person> rpop(@RequestParam(value="count", required = false, defaultValue = "1") int count){
        return redisTemplate.opsForList()
                .rightPop(PERSON_LIST_KEY, count)
                .stream()
                .map(x -> (Person)x)
                .collect(Collectors.toList());
    }


    //Key(String) - Value([Field - Value] Map)
    @PostMapping("/hash/person")
    public void savePersonInHash(@RequestBody List<Person> people) {
        people.stream()
                .filter(x -> x.getId()!=0)
                .forEach(x -> {
                        Map map = objectMapper.convertValue(x, Map.class);
                        redisTemplate.opsForHash().putAll(getHashKey(x.getId()), map);
                });
    }

    @GetMapping("/hash/person/all")
    public List<Person> getPeople(@RequestParam("ids") List<Long> peopleIds){
        return peopleIds.stream()
                .map(x -> redisTemplate.opsForHash().entries(getHashKey(x)))
                .map(entryMap -> objectMapper.convertValue(entryMap, Person.class))
                .collect(Collectors.toList());

    }

    private String getHashKey(long id){
        return PERSON_HASH_KEY_PREFIX + id;
    }

    }


