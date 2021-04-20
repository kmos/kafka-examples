# kafka-examples

Some examples for kafka

### Examples kafka
 - [X] simple text producer and textConsumer
 - [X] simple json producer and textConsumer
 - simple filter messages based on header
 - avro producer and textConsumer with registry

### Project Structure

```text

└── src
    ├── main
    │   ├── generated-sources // genereted avro resources
    │   ├── java
    │   │   └── ...
    │   │       └─── 
    │   │           ├── json // kafka example with message serialized with json
    │   │           │   ├── consumer
    │   │           │   │   ├── configuration
    │   │           │   │   ├── listener
    │   │           │   │   └── service
    │   │           │   ├── message
    │   │           │   └── producer
    │   │           │       ├── configuration
    │   │           │       └── pub
    │   │           └── text // kafka example with simple text message
    │   │               ├── consumer
    │   │               │   ├── configuration
    │   │               │   ├── listener
    │   │               │   └── service
    │   │               └── producer
    │   │                   ├── configuration
    │   │                   └── pub
    │   └── resources
    │       └── avro
    └── test
        ├── java
        │   └── ...
        │       └───
        │           └── simple
        │               ├── configuration
        │               ├── json // kafka test example with message serialized with json
        │               ├── registry
        │               ├── text // kafka example with simple text message
        │               └── util
        │                   └── containers
        └── resources
            └── ...
                └── mosfet
```