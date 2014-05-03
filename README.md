# cascading.avro-scheme

Cascading scheme for reading and writing data serialized using Apache Avro. This project provides several
schemes that work off an Avro record schema.

- AvroScheme - sources and sinks tuples with fields named and ordered according to a given Avro schema or a list of Fields and Types. If no schema is specified in a source it will peek at the data and get the schema. A sink schema is required (for now). 

Avro Maps will be read in and converted to Java Maps. Avro Arrays will be read in and converted to Java Lists. In order to use this feature you will need to provide Hadoop with a way to serialize Java Maps and Lists, such as cascading.kryo. 

When writing to Avro an Avro Array can be made from either a Java List or a Cascading Tuple. The same applies for an Avro Map. In the case of a Map, the incoming Cascading Tuple will be taken two entries at a time, the first will be the key for the Avro Map and the second will be the value. 

The current implementation supports all Avro types including nested records. A nested record will be written as a new Cascading TupleEntry inside the proper Tuple Field. To write a nested record to Avro you must provide a TupleEntry with proper field names. 

The current version of cascading.avro is compatibile with Cascading 2.x. Please see the 1.0 branch for a Cascading 1.2.x version. 

# cascading.avro-maven-plugin

An Apache Maven plugin that generates classes with field name constants based on an Avro record schema. This plugin
is similar to the standard Avro schema plugin used to generate specific objects for Avro records. The plugin creates
names for generated classes by appending the word "Fields" to the record name. The generated class will have constant fields
for all record fields, as well as a field named ALL that lists all fields in the expected order.

The advantage of using the plugin is that given an Avro record Foo with field bar, you can use FooFields.BAR rather than 
the string "bar" in your Flow. Also it adds FooFields.ALL which lists all fields in the record, which is helpful as the
last step of an assembly to ensure you're producing all fields.


## Acknowledgements
This project has components of the original cascading.avro project as well as some from the [cascading-avro](https://github.com/MaxPoint/cascading-avro) project. 

## License

Distributed under Apache License, Version 2.0
