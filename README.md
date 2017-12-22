# Logstash Input ElastiCache

    input {
      elasticache {
        region => "us-west-2"
        source_type => "replication-group"
        source_name => "development"
      }
    }

