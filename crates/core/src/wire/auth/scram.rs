//! SCRAM-SHA-256 server-side state machine (RFC 5802 + PG dialect).
//!
//! Phase 7.3: completes wire-layer SCRAM auth against PG-stored
//! verifiers loaded by [`super::verifier`]. PG stores
//! `(salt, iterations, StoredKey, ServerKey)` in
//! `pg_authid.rolpassword`; pgwire's `ScramAuth` API expects the
//! pre-derivation `SaltedPassword` instead (which PG does **not**
//! store), so we implement the server side ourselves rather than
//! use the pgwire helper.
//!
//! ## Verification path
//!
//! We have: `StoredKey`, `ServerKey`, `salt`, `iterations`.
//! Client sends: `ClientProof` (and the SCRAM message exchange).
//!
//! 1. `ClientSignature = HMAC(StoredKey, AuthMessage)`
//! 2. `ClientKey_candidate = ClientProof XOR ClientSignature`
//! 3. Accept iff `H(ClientKey_candidate) == StoredKey`
//! 4. Reply with `v = base64(HMAC(ServerKey, AuthMessage))`
//!
//! where `AuthMessage = client-first-bare || "," || server-first ||
//! "," || client-final-without-proof`.
//!
//! ## What's intentionally NOT here
//!
//! - **Channel binding (SCRAM-SHA-256-PLUS).** Lands when TLS does
//!   (phase 8); for v0 we advertise only `SCRAM-SHA-256` and
//!   reject channel-binding requests at parse time.
//! - **SASLprep on usernames/passwords.** PG itself doesn't
//!   normalize at verification time — the password was normalized
//!   by `password_encryption` at SET time and is baked into the
//!   stored `StoredKey`. Our server side never sees a cleartext
//!   password, so SASLprep is moot.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use hmac::{Hmac, Mac, digest::KeyInit};
use sha2::{Digest, Sha256};

use super::verifier::ScramVerifier;

type HmacSha256 = Hmac<Sha256>;

/// 32 bytes = SHA-256 output size + HMAC-SHA-256 output size.
const SHA256_BYTES: usize = 32;

/// SCRAM nonce size in raw bytes. PG uses 18 (-> 24 base64-no-pad
/// chars); we match.
const NONCE_BYTES: usize = 18;

/// Mechanism string we advertise + the only one we accept.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// Server-side per-connection SCRAM state machine. Built fresh
/// per connection by the startup handler; one instance services
/// one client_first + one client_final.
pub struct ScramServer {
    verifier: ScramVerifier,
    /// Combined nonce (`client_nonce || server_nonce`), stored
    /// after `on_client_first` so we can validate the echo in
    /// `on_client_final`.
    combined_nonce: String,
    /// `client-first-message-bare` (`n=...,r=...`) — needed for
    /// the `AuthMessage` we'll compute at `on_client_final`.
    client_first_bare: String,
    /// `server-first-message` (`r=...,s=...,i=...`), same.
    server_first: String,
    /// `gs2-header` from client first (e.g. `"n,,"`) — must match
    /// the base64-decoded channel-binding field in client final to
    /// prevent CB-downgrade attacks.
    gs2_header: String,
}

impl ScramServer {
    pub fn new(verifier: ScramVerifier) -> Self {
        Self {
            verifier,
            combined_nonce: String::new(),
            client_first_bare: String::new(),
            server_first: String::new(),
            gs2_header: String::new(),
        }
    }

    /// Process a SCRAM `client-first-message` and return the
    /// `server-first-message` to send as `Authentication::SASLContinue`.
    pub fn on_client_first(&mut self, msg: &[u8]) -> Result<String, ScramError> {
        let msg_str =
            std::str::from_utf8(msg).map_err(|_| ScramError::Malformed("client first not utf8"))?;
        let parsed = ClientFirst::parse(msg_str)?;

        self.gs2_header = parsed.gs2_header.to_owned();
        self.client_first_bare = parsed.bare.to_owned();

        let server_nonce = generate_nonce()?;
        self.combined_nonce = format!("{}{}", parsed.client_nonce, server_nonce);

        self.server_first = format!(
            "r={},s={},i={}",
            self.combined_nonce,
            STANDARD.encode(&self.verifier.salt),
            self.verifier.iterations,
        );

        Ok(self.server_first.clone())
    }

    /// Process a SCRAM `client-final-message`, verify the proof,
    /// and return the `server-final-message` (`v=…`) to send as
    /// `Authentication::SASLFinal`. Returns `ScramError::AuthFailed`
    /// for a wrong-password proof (translates to wire SQLSTATE
    /// `28P01`).
    pub fn on_client_final(&mut self, msg: &[u8]) -> Result<String, ScramError> {
        let msg_str =
            std::str::from_utf8(msg).map_err(|_| ScramError::Malformed("client final not utf8"))?;
        let parsed = ClientFinal::parse(msg_str)?;

        // Nonce echo check (RFC 5802 §5.1 e=other-error).
        if parsed.combined_nonce != self.combined_nonce {
            return Err(ScramError::Malformed("nonce mismatch"));
        }

        // Channel-binding downgrade check: the client must echo
        // base64(gs2-header) in the `c=` field. Mismatch means
        // the client sent a different gs2-header in the second
        // round, which is a MITM signal.
        let expected_cb = STANDARD.encode(self.gs2_header.as_bytes());
        if parsed.channel_binding != expected_cb {
            return Err(ScramError::Malformed("channel binding mismatch"));
        }

        // AuthMessage = client_first_bare || "," || server_first || "," || client_final_no_proof
        let client_final_no_proof =
            format!("c={},r={}", parsed.channel_binding, parsed.combined_nonce);
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_no_proof
        );

        // Decode + length-check the proof.
        let client_proof = STANDARD
            .decode(parsed.client_proof.as_bytes())
            .map_err(|_| ScramError::Malformed("client proof not base64"))?;
        if client_proof.len() != SHA256_BYTES {
            return Err(ScramError::Malformed("client proof wrong length"));
        }

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes());
        // ClientKey_candidate = ClientProof XOR ClientSignature
        let mut client_key_candidate = [0u8; SHA256_BYTES];
        for i in 0..SHA256_BYTES {
            client_key_candidate[i] = client_proof[i] ^ client_signature[i];
        }
        // Accept iff H(ClientKey_candidate) == StoredKey.
        let stored_key_check = sha256(&client_key_candidate);
        if !constant_time_eq(&stored_key_check, &self.verifier.stored_key) {
            return Err(ScramError::AuthFailed);
        }

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_signature = hmac_sha256(&self.verifier.server_key, auth_message.as_bytes());
        Ok(format!("v={}", STANDARD.encode(server_signature)))
    }
}

/// Errors from the SCRAM state machine. The startup handler maps
/// these to wire `ErrorResponse` frames with appropriate SQLSTATEs.
#[derive(Debug)]
#[allow(dead_code)] // Malformed's static str is captured via Debug for log lines.
pub enum ScramError {
    /// Wire-format violation: malformed SCRAM message, bad nonce,
    /// channel-binding mismatch, bad base64, etc. SQLSTATE 28P01
    /// (matches PG's behaviour for any SASL-level protocol error).
    Malformed(&'static str),
    /// Proof verification failed (wrong password). SQLSTATE 28P01.
    AuthFailed,
    /// Internal failure during nonce generation
    /// (`pg_strong_random` returned false). SQLSTATE XX000 — should
    /// be exceedingly rare.
    NonceGenerationFailed,
}

// ---------------------------------------------------------------------------
// Message parsers
// ---------------------------------------------------------------------------

/// Parsed `client-first-message`.
///
/// Format (RFC 5802 §7):
/// ```text
/// gs2-header "," client-first-message-bare
/// gs2-header = ("n,," | "y,," | "p=" cb-name ",,")
/// client-first-message-bare = ["a=" auth-id ","] "n=" username "," "r=" nonce *("," attr-val)
/// ```
struct ClientFirst<'a> {
    /// `"n,,"` or `"y,,"` (we reject `"p=…,,"` since channel
    /// binding lands in phase 8).
    gs2_header: &'a str,
    /// The `n=…,r=…[…]` slice; preserved verbatim for AuthMessage.
    bare: &'a str,
    /// `r=` value from the bare.
    client_nonce: &'a str,
}

impl<'a> ClientFirst<'a> {
    fn parse(s: &'a str) -> Result<Self, ScramError> {
        // gs2-header ends at the SECOND comma (the first is inside
        // the header itself, between `n`/`y`/`p=…` and the optional
        // authzid).
        let bare_start = match find_nth_comma(s, 2) {
            Some(pos) => pos + 1,
            None => return Err(ScramError::Malformed("client first missing gs2-header")),
        };
        let gs2_header = &s[..bare_start];
        let bare = &s[bare_start..];

        // We support only the no-channel-binding flavours in v0;
        // accept `n,,` (no CB) and `y,,` (client supports CB but
        // server didn't advertise SCRAM-SHA-256-PLUS, which we
        // didn't). Reject `p=…,,`.
        if !(gs2_header.starts_with("n,") || gs2_header.starts_with("y,")) {
            return Err(ScramError::Malformed("channel binding not supported"));
        }

        let mut client_nonce = "";
        for part in bare.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                client_nonce = v;
            }
        }
        if client_nonce.is_empty() {
            return Err(ScramError::Malformed("client first missing nonce"));
        }

        Ok(Self {
            gs2_header,
            bare,
            client_nonce,
        })
    }
}

/// Parsed `client-final-message`.
///
/// Format: `c=<channel-binding-b64>,r=<combined-nonce>[,…],p=<proof-b64>`
struct ClientFinal<'a> {
    channel_binding: &'a str,
    combined_nonce: &'a str,
    client_proof: &'a str,
}

impl<'a> ClientFinal<'a> {
    fn parse(s: &'a str) -> Result<Self, ScramError> {
        let mut cb = "";
        let mut nonce = "";
        let mut proof = "";
        for part in s.split(',') {
            if let Some(v) = part.strip_prefix("c=") {
                cb = v;
            } else if let Some(v) = part.strip_prefix("r=") {
                nonce = v;
            } else if let Some(v) = part.strip_prefix("p=") {
                proof = v;
            }
            // ignore other attr-vals (e.g. extension fields)
        }
        if cb.is_empty() || nonce.is_empty() || proof.is_empty() {
            return Err(ScramError::Malformed("client final missing c/r/p field"));
        }
        Ok(Self {
            channel_binding: cb,
            combined_nonce: nonce,
            client_proof: proof,
        })
    }
}

// ---------------------------------------------------------------------------
// Crypto + nonce primitives
// ---------------------------------------------------------------------------

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; SHA256_BYTES] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    let out = mac.finalize().into_bytes();
    let mut buf = [0u8; SHA256_BYTES];
    buf.copy_from_slice(&out);
    buf
}

fn sha256(msg: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(msg);
    let out = hasher.finalize();
    let mut buf = [0u8; SHA256_BYTES];
    buf.copy_from_slice(&out);
    buf
}

/// Constant-time byte-slice equality. Important for stored-key
/// comparison: a timing-dependent equality would leak partial
/// information about the StoredKey to a connected attacker.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Generate a SCRAM nonce using PG's CSPRNG (`pg_strong_random`,
/// backed by `/dev/urandom` or equivalent OS source). 18 random
/// bytes → 24 base64-no-pad characters, matching PG's own SCRAM
/// nonce length.
fn generate_nonce() -> Result<String, ScramError> {
    let mut bytes = [0u8; NONCE_BYTES];
    // SAFETY: pg_strong_random takes (buf, len) and writes len
    // bytes into buf; we pass the address + length of our stack
    // buffer. Returns true on success, false on entropy source
    // failure.
    let ok = unsafe {
        pgrx::pg_sys::pg_strong_random(bytes.as_mut_ptr() as *mut core::ffi::c_void, bytes.len())
    };
    if !ok {
        return Err(ScramError::NonceGenerationFailed);
    }
    Ok(STANDARD_NO_PAD.encode(bytes))
}

/// Find the byte index of the Nth (1-based) comma in `s`, or
/// `None` if there are fewer than N commas.
fn find_nth_comma(s: &str, n: usize) -> Option<usize> {
    let mut count = 0;
    for (i, b) in s.bytes().enumerate() {
        if b == b',' {
            count += 1;
            if count == n {
                return Some(i);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// These ALL use `#[pg_test]` (not plain `#[test]`) because the SCRAM
// module references `pgrx::pg_sys::pg_strong_random` for nonce
// generation — that's a PG symbol that doesn't resolve in a normal
// cargo-test link. `#[pg_test]` runs the tests inside a real PG
// backend; the pure-crypto cases (no SPI, no transaction state)
// just don't use that surface.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::prelude::*;

    #[pg_test]
    fn parses_client_first_no_channel_binding() {
        let m = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        let p = ClientFirst::parse(m).unwrap();
        assert_eq!(p.gs2_header, "n,,");
        assert_eq!(p.bare, "n=user,r=fyko+d2lbbFgONRv9qkxdawL");
        assert_eq!(p.client_nonce, "fyko+d2lbbFgONRv9qkxdawL");
    }

    #[pg_test]
    fn parses_client_first_y_flag() {
        let m = "y,,n=user,r=abc";
        let p = ClientFirst::parse(m).unwrap();
        assert_eq!(p.gs2_header, "y,,");
        assert_eq!(p.client_nonce, "abc");
    }

    #[pg_test]
    fn rejects_channel_binding_request() {
        let m = "p=tls-server-end-point,,n=user,r=abc";
        assert!(matches!(
            ClientFirst::parse(m),
            Err(ScramError::Malformed("channel binding not supported"))
        ));
    }

    #[pg_test]
    fn parses_client_final() {
        let m =
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";
        let p = ClientFinal::parse(m).unwrap();
        assert_eq!(p.channel_binding, "biws");
        assert_eq!(
            p.combined_nonce,
            "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"
        );
        assert_eq!(p.client_proof, "v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=");
    }

    #[pg_test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }

    /// End-to-end SCRAM exchange against a known-good verifier.
    /// Tests both proof verification and server signature.
    #[pg_test]
    fn full_scram_exchange_rfc7677_vector() {
        // RFC 7677 §3 test vectors. password = "pencil", salt =
        // base64("QSXCR+Q6sek8bf92"), iters = 4096. The published
        // SaltedPassword is the PBKDF2 output for those inputs;
        // StoredKey + ServerKey are derived from it via the SCRAM
        // recipe.
        let salt = STANDARD.decode("QSXCR+Q6sek8bf92").unwrap();
        let salted_password =
            hex_decode("c66dd80b73a8bd0a0a13d62e3e74c45c66bf94d12cb6c75d796ad65a8a3eaad0");
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");

        let verifier = ScramVerifier {
            iterations: 4096,
            salt,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        };
        let mut server = ScramServer::new(verifier);

        // Step 1: client-first-message with a known client nonce.
        // Server generates its own server nonce; we read the
        // combined value out afterwards.
        let client_first = b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = server.on_client_first(client_first).unwrap();
        assert!(server_first.starts_with("r=rOprNGfwEbeRWgbNEkqO"));
        assert!(server_first.contains(",s=QSXCR+Q6sek8bf92,i=4096"));

        // Step 2: synthesise the client-final the way a real
        // client would, with the correct ClientKey-derived proof.
        let combined_nonce = server.combined_nonce.clone();
        let client_final_no_proof = format!("c=biws,r={combined_nonce}");
        let auth_message = format!(
            "{},{},{}",
            "n=user,r=rOprNGfwEbeRWgbNEkqO", server.server_first, client_final_no_proof
        );
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut client_proof = [0u8; SHA256_BYTES];
        for i in 0..SHA256_BYTES {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }
        let client_final = format!(
            "c=biws,r={combined_nonce},p={}",
            STANDARD.encode(client_proof)
        );

        let server_final = server.on_client_final(client_final.as_bytes()).unwrap();
        let expected_v = STANDARD.encode(hmac_sha256(&server_key, auth_message.as_bytes()));
        assert_eq!(server_final, format!("v={expected_v}"));
    }

    #[pg_test]
    fn rejects_wrong_password() {
        let salt = STANDARD.decode("QSXCR+Q6sek8bf92").unwrap();
        let salted_password =
            hex_decode("c66dd80b73a8bd0a0a13d62e3e74c45c66bf94d12cb6c75d796ad65a8a3eaad0");
        let stored_key = sha256(&hmac_sha256(&salted_password, b"Client Key"));
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let verifier = ScramVerifier {
            iterations: 4096,
            salt,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        };
        let mut server = ScramServer::new(verifier);

        server
            .on_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .unwrap();
        let combined_nonce = server.combined_nonce.clone();
        // 32 zero bytes is definitely not a valid proof for any
        // non-zero ClientSignature.
        let bad_proof = STANDARD.encode([0u8; SHA256_BYTES]);
        let client_final = format!("c=biws,r={combined_nonce},p={bad_proof}");
        assert!(matches!(
            server.on_client_final(client_final.as_bytes()),
            Err(ScramError::AuthFailed)
        ));
    }

    /// Tiny hex decoder for test vectors. We deliberately don't
    /// pull `hex` as a dep just for tests.
    fn hex_decode(s: &str) -> Vec<u8> {
        let bytes = s.as_bytes();
        assert!(bytes.len().is_multiple_of(2));
        (0..bytes.len())
            .step_by(2)
            .map(|i| {
                let hi = (bytes[i] as char).to_digit(16).unwrap();
                let lo = (bytes[i + 1] as char).to_digit(16).unwrap();
                (hi * 16 + lo) as u8
            })
            .collect()
    }
}
