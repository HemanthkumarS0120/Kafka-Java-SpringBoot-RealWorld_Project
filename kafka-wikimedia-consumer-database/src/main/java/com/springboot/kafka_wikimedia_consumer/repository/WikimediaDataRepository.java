package com.springboot.kafka_wikimedia_consumer.repository;

import com.springboot.kafka_wikimedia_consumer.entity.WikimediaData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaDataRepository extends JpaRepository<WikimediaData, Long> {
}
