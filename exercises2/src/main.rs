use bson::Document;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::{fs, str};

#[derive(Debug, Serialize, Deserialize)]
enum Direction {
    Up,
    Down,
    Left,
    Right,
}

/// Imagine you have a game character that every turn may move any number of squares in a single direction.
/// Define a type, Move that represents a single move of that character.
#[derive(Debug, Serialize, Deserialize)]
struct Move {
    direction: Direction,
    num_squares: i32,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Write a main function that defines a variable, a, of type Move, serializes it with serde to a File, then deserializes it back again to a variable, b, also of type Move.
    // Use JSON as the serialization format.
    // Print a and b with println! and the {:?} format specifier to verify successful deserialization.

    let file_name = "/tmp/foo";

    let a = Move {
        direction: Direction::Up,
        num_squares: 15,
    };
    println!("move a = {:?}", a);

    let out_json = serde_json::to_string(&a)?;
    fs::write(file_name, out_json)?;

    let in_json = fs::read_to_string(file_name)?;

    let b: Move = serde_json::from_str(&in_json)?;
    println!("move b = {:?}", b);

    // Do the same as above, except this time, instead of serializing to a File, serialize to a Vec<u8> buffer, and after that try using RON instead of JSON as the format.
    // Are there any differences in serialization to a Vec instead of a File? What about in using the RON crate vs the JSON crate?
    // Convert the Vec<u8> to String with str::from_utf8, unwrapping the result, then print that serialized string representation to see what Move looks like serialized to RON.

    let mut buf: Vec<u8> = Vec::new();
    ron::ser::to_writer(&mut buf, &a)?;
    println!("Serialized to RON buffer: {:?}", buf);
    println!("RON as string: {:?}", str::from_utf8(&buf).unwrap());

    // Serialize 1000 different Move values to a single file, back-to-back, then deserialize them again. This time use the BSON format.
    let file_name2 = "/tmp/lots_of_foo";
    {
        let mut f = File::create(file_name2).expect("Unable to create file");

        for i in 0..1000 {
            let m = Move {
                direction: Direction::Up,
                num_squares: i,
            };
            let doc = bson::to_document(&m)?;
            doc.to_writer(&mut f)?
        }
        println!("Done writing 1000 moves to file {}", file_name2);
    }

    {
        let f = File::open(file_name2)?;
        let mut br = BufReader::new(f);
        let mut many_moves = Vec::new();
        while let Ok(doc) = Document::from_reader(&mut br) {
            let m: Move = bson::from_document(doc)?;
            many_moves.push(m);
        }
        println!("Got {} moves from file", many_moves.len());
    }

    // Repeating the above, but serializing/deserializing to Vec<u8> in memory as opposed to file
    let mut big_buf: Vec<u8> = Vec::new();
    for i in 0..1000 {
        let m = Move {
            direction: Direction::Up,
            num_squares: i,
        };
        let doc = bson::to_document(&m)?;
        doc.to_writer(&mut big_buf)?
    }
    println!("Size of buffer after writing 1000 moves: {}", big_buf.len());
    let mut many_moves = Vec::new();
    // Cool trick I discovered from https://stackoverflow.com/a/42241174
    // u8 slice supports Read trait, whereas Vec<u8> does not.
    let mut br = BufReader::new(&big_buf[..]);
    while let Ok(doc) = Document::from_reader(&mut br) {
        let m: Move = bson::from_document(doc)?;
        many_moves.push(m);
    }
    println!("Got {} moves from buffer", many_moves.len());

    Ok(())
}
