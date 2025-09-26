use std::io;
use umbra::sql::statement::{Column, Type, Value, Constraint};
use umbra::db::schema::Schema;
use umbra::core::storage::tuple;

fn main() -> io::Result<()> {
    // Simulate the exact scenario from the failing test
    println!("=== Testing unique constraint serialization ===");
    
    // Create the phone column exactly as in the test
    let phone_col = Column {
        name: "phone".to_string(),
        data_type: Type::Varchar(15),
        constraints: vec![Constraint::Nullable, Constraint::Unique],
    };
    
    // Create the row ID column (primary key)
    let id_col = Column {
        name: "id".to_string(),
        data_type: Type::Serial,
        constraints: vec![Constraint::PrimaryKey],
    };
    
    // This is the index schema (nullable constraint removed)
    let mut index_phone_col = phone_col.clone();
    let mut index_id_col = id_col.clone();
    index_phone_col.constraints.retain(|c| !matches!(c, Constraint::Nullable));
    index_id_col.constraints.retain(|c| !matches!(c, Constraint::Nullable));
    
    let index_schema = Schema::new(vec![index_phone_col, index_id_col]);
    
    println!("Index schema nullable: {}", index_schema.has_nullable());
    println!("Index schema columns: {:?}", index_schema.columns.len());
    
    // Alice's index entry: [phone_value, row_id]
    let alice_phone = Value::String("+15551234567".to_string());
    let alice_id = Value::Number(1);
    let alice_index_tuple = [&alice_phone, &alice_id];
    
    // Bob's index entry: [phone_value, row_id] 
    let bob_phone = Value::String("+15559876543".to_string());
    let bob_id = Value::Number(2);
    let bob_index_tuple = [&bob_phone, &bob_id];
    
    println!("Alice phone: {:?}", alice_phone);
    println!("Bob phone: {:?}", bob_phone);
    println!("Are phones equal? {}", alice_phone == bob_phone);
    
    // Serialize both index tuples
    let alice_serialized = tuple::serialize_tuple(&index_schema, alice_index_tuple);
    let bob_serialized = tuple::serialize_tuple(&index_schema, bob_index_tuple);
    
    println!("Alice serialized: {:?}", alice_serialized);
    println!("Bob serialized: {:?}", bob_serialized);
    println!("Serialized equal? {}", alice_serialized == bob_serialized);
    
    // Test deserialization
    let alice_deserialized = tuple::deserialize(&alice_serialized, &index_schema);
    let bob_deserialized = tuple::deserialize(&bob_serialized, &index_schema);
    
    println!("Alice deserialized: {:?}", alice_deserialized);
    println!("Bob deserialized: {:?}", bob_deserialized);
    
    Ok(())
}
