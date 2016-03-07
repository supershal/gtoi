# gtoi
graphite to influxdb

Convert Graphite line protocol to influxdb line protocol

##Config

[[pointconverters.whisper]]
	pointRegex = "regex"
	measurement = "?measurement"
	[[pointconverters.whisper.tag]]
		key = "host"
		value ="?host"
	[[pointconverters.whisper.tag]]
		key = "org"
		value ="?org"
	[[pointconverters.whisper.field]]
		key = "4xx"
		#whisper's "value" field will be used.
[[pointconverters.whisper]]
	pointRegex = "regex"
	measurement = "?measurement"
	[[pointconverters.whisper.tag]]
		key = "host"
		value ="?host"
	[[pointconverters.whisper.field]]
		key = "4xx"
		#whisper's "value" field will be used.

 [influxclient]
      enabled = true
      addresses = ["localhost:8086","localhost:1234","localhost:5678"] # stress_test_server runs on port 1234
      database = "traffic"
      precision = "s"
      batch_size = 10000
      batch_interval = "0s"
      concurrency = 10









- Read config.
- compile regexp for each converter.
- for each converter create database if not exists
- Collect all files with .wsp
- Create tables and retention policies.
  for each file
  	for each matchedConverter
  		- create database if not exists
  		- create retenttion policy if not exists
- for each file
    for each converter
      -> match()
         true:

         	convert
         false:
            continue
      -> no match