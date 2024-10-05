## Stream Processing and Real time
- Latency is less to get results faster
- Processing system needs to be capable of ingesting data at high speed

## Azure Events HUB
- This is a data streaming service that can stream millions of events per second
- This can be from any Source/Destination

#### Event Hub Architecture

![Event Hub Architecture](https://github.com/ShauryaRawat10/Data-Engineering/blob/92af1d993e92899e1af7eacb8d77505459b2e1cd/Azure%20Cloud/Introduction/Storage/EventHubsArchitecture.png)

- Event hub namespace: This is container for multiple event hub
- Event data is stored accross multiple partitions. It helps increase througput of system
 - Each partition generally sustain 1 MB/s throughput
 - We can use partition key to map events to specific partition
- Each event can have body of events (Metadata - Offset of partitions, stream sequence number)
- Event Publisher sends data to Events hub
- Event Consumer consumes events from Event Hub
- Event Consumer is logical grouping of consumers that read data from events hub

Properties
- Event Retention - Standard (7 days), Premium/Dedicated - 90 days max
- Ingress - 1 MB per second or 1000 events per second
- Egress - 2 MB per second or 4096 events per second
