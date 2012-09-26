# cascading.avro-scheme

Cascading scheme for reading and writing data serialized using Apache Avro. This project provides several
schemes that work off an Avro record schema.

- AvroScheme - sources and sinks tuples with fields named and ordered according to a given Avro schema. If no schema is specified in a source it will peek at the data and get the schema. Currently a sink schema is required but this may change soon. 

The current implementation supports all Avro types including nested records. A nested record will be written as a new Cascading TupleEntry inside the proper Tuple Field. 

# cascading.avro-maven-plugin

An Apache Maven plugin that generates classes with field name constants based on Avro record schema. This plugin
is similar to standard Avro schema plugin used to generate specific objects for Avro records. For The plugin names
generated classes by appending the word "Fields" to the record name. The generated class will have constant fields
for all record fields, as well as, a field named ALL that lists all fields in the expected order.

## License

Distributed under Apache License, Version 2.0
