package cascading.avro;

public class PackedAvroScheme extends AvroScheme {
  /**
   * This scheme should be used when you don't want cascading.avro to automatically unpack or pack your Avro objects.
   * The constructors are similar to the super class but there is only ever one field incoming or outgoing.
   */


  public PackedAvroScheme() {

  }

}
