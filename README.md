# A Distributed Data Storage Solution containerized with Docker

This is a distributed storage solution built from scratch using Python & Flask. I used Docker to emulate multiple servers that work together to form a single storage system. 

## Storage Architecture

This system is sharded with replicas for each shard. On startup, an admin can specify how many replicas each shard should contain. Data is fully replicated among all nodes in a shard. If the number of replicas drops below the minumum threshold, resharding can be initiated through the ```/key-value-store-shard/reshard``` endpoint.

## Maintaining View of Active Nodes

Many operations require a correct view of the entire system. When a node is determined to be offline or inoperable by another node, this information can be communicated through the ```/key-value-store-view/``` endpoint. When a new node is added, this endpoint can also be used to add itself to the system's view.

## Ensuring Causal Consistency

Messages have the potential to be received out of order. To prevent this, nodes maintain a message versioning system, and return a version value to a client. This value must be incremented and attached to future messages to ensure proper ordering of messages.