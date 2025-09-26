// Let me create a simple test to understand what's being serialized
use umbra::sql::statement::{Column, Type, Value, Constraint};
use umbra::db::schema::Schema;
use umbra::core::storage::tuple;

fn main() {
    println!("=== Testing tuple serialization for nullable unique constraint ===");
    
    // Create a schema similar to what the failing test uses
    let phone_col = Column {
        name: "phone".to_string(),
        data_type: Type::Varchar(15),
        constraints: vec![Constraint::Nullable, Constraint::Unique],
    };
    let id_col = Column {
        name: "id".to_string(),
        data_type: Type::Serial,
        constraints: vec![Constraint::PrimaryKey],
    };
    
    // Index schema for unique constraint: [phone_value, row_id]
    let index_schema = Schema::new(vec![phone_col.clone(), id_col.clone()]);
    
    println!("Index schema has nullable: {}", index_schema.has_nullable());
    
    // Alice's phone number
    let alice_values = [&Value::String("+15551234567".to_string()), &Value::Number(1)];
    let alice_serialized = tuple::serialize_tuple(&index_schema, alice_values);
    println!("Alice serialized: {:?}", alice_serialized);
    
    // Bob's phone number  
    let bob_values = [&Value::String("+15559876543".to_string()), &Value::Number(2)];
    let bob_serialized = tuple::serialize_tuple(&index_schema, bob_values);
    println!("Bob serialized: {:?}", bob_serialized);
    
    println!("Are they equal? {}", alice_serialized == bob_serialized);
    
    // Let's also test deserialization
    let alice_deserialized = tuple::deserialize(&alice_serialized, &index_schema);
    let bob_deserialized = tuple::deserialize(&bob_serialized, &index_schema);
    
    println!("Alice deserialized: {:?}", alice_deserialized);
    println!("Bob deserialized: {:?}", bob_deserialized);
}
