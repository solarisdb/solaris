[![build](https://github.com/solarisdb/solaris/actions/workflows/build.yaml/badge.svg)](https://github.com/solarisdb/solaris/actions/workflows/build.yaml) [![docker](https://github.com/solarisdb/solaris/actions/workflows/docker.yaml/badge.svg)](https://github.com/solarisdb/solaris/actions/workflows/docker.yaml) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/solarisdb/solaris/blob/main/LICENSE)

# Solaris DB
Solaris is a streaming database that facilitates the storage and retrieval of unstructured data records within streams, which are groups of ordered records. As a horizontally scalable cloud database, Solaris supports billions of streams storing petabytes of data, and enables efficient, low-latency operations.

__Highlights__
* _Scalable storage capacity_. Solaris supports billions of streams with petabytes of data records that can be persisted in long-term cloud-storages as AWS S3.
* _High-speed data processing_. Writing and reading millions of records per second (gigabytes of data).
* _Low latency_. Data becomes available for read within milliseconds after it is written.
* _Horizontally Scalable_. Solaris allows to add storage instances to improve the performance and the data throughput.
* _Highly available_. The load is automatically distributed between Solaris instances.
* _Reliable_. Solaris allows to replicate the data from its instances into the second long-term storage like AWS S3.
* _Built-in stream processing capabilities_. Merging, filtering and search records using Solaris Query Language.
* _Open Source_. Solaris is 100% open-source. It can be used for building trustworthy data storages.
* _Simple setup and deployment_. Basic installation includes a stand-alone Solaris executable, which can be run on local machine.
* _Cloud native_. Solaris was born as a distributed AWS-based cloud service to store billions of streams.

## Quick introduction
We started to build Solaris as a cloud service to persist streams for storing chat conversations as a part of contact center product. The requirements were to have a low-latency service that would allow storing billions of conversations, each with a relatively small number of records (thousands). Even though the use case sounds very specific, we found the idea of having a database to store a tremendous amount of streams with records to be pretty interesting, as it can be extended to other use cases. We found that Solaris may be used as the storage of application logs of workflows, that were executed in the system. Later on we conclued that Solaris can be used as audit logs storage and even as a buffer to substitute Kafka. This is how Solaris became open source.

### Functionalities
Solaris has very simple functionality - you can store a sequence of events (records) in a stream. Of course, the records may be read from the stream in the order they were added to the stream or in reverse order. Records from different streams can be merged together into one "virtual" stream by time, so the records from multiple streams can be consumed as one stream where records are ordered by the time they were added to the streams.

Not complex, right? The problem Solaris solves is the data amount and the latency. Now, presume that the number of streams can be billions and the read-write speed should be as fast as possible. The data amount and the speed are where Solaris shines compared to "traditional" data storages like NoSQL or RDBMS products.

Solaris offers long-term storage replication out of the box, so you don't need to worry about retention policy and integrate Solaris with long-term big data storage like AWS S3. The data is replicated automatically, so you can store petabytes of data into Solaris without worrying about how the data should be moved. Solaris does it itself.

### Infrastructure
Solaris is a cloud service primarily built for storing a large amount of data in a cloud storages like AWS S3. To achieve its maximum capabilities, in addition to Solaris instances, you need to configure and utilize other AWS services like AWS S3 or AWS RDS. However, this is for production or highly scalable and performant solutions.

If scale or data size is not a factor, but simplicity and speed are still required, Solaris can be run in a stand-alone configuration. In this configuration, there are no dependencies on any external services. The stand-alone Solaris instance can be run in a Docker container, as a Kubernetes instance, or even on a local machine to serve as the streaming database for a small amount of data (that may fit into the local filesystem) or as part of the development environment. In this case, Solaris is very lightweight, fast, and easy to run (as it consists of a single executable).

## License
This project is licensed under the Apache Version 2.0 License - see the [LICENSE](LICENSE) file for details

## Acknowledgments
* GoLand IDE by [JetBrains](https://www.jetbrains.com/go/) is used for the code development