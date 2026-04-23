use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use crate::jwt_secret_key;
use chrono::Utc;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub aud: String,
    pub exp: usize,
}

pub fn create_jwt_token() -> Result<String, jsonwebtoken::errors::Error> {
    let encoding_key: EncodingKey = EncodingKey::from_secret(jwt_secret_key().as_ref());
    println!("Creating JWT token with secret key: {}", jwt_secret_key());
    let header: Header = Header::new(Algorithm::HS256);
    let claims: Claims = Claims {
        aud: "cbtm".to_string(),
        exp: (Utc::now().timestamp() + 3600) as usize, // Token expires in 1 hour
    };
    encode(&header, &claims, &encoding_key)
}