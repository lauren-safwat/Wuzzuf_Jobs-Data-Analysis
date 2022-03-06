package com.example.Wuzzuf;

import org.springframework.http.ResponseEntity;

import java.util.List;

public interface JobDAO {
    ResponseEntity<List<Job>> getSample();
}
