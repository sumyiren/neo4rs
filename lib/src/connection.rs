use crate::errors::{unexpected, Error};
use crate::messages::*;
use crate::version::Version;
use bytes::*;
use std::mem;
use bolt_client::bolt_proto::version::{V4, V4_3, V4_4};
use bolt_client::{Client, Metadata, Stream};
use bolt_client::bolt_proto::Message;
use bolt_client::bolt_proto::message::Success;
use tokio::io::BufStream;
use tokio_util::compat::*;
use std::iter::FromIterator;
use std::convert::TryFrom;

const MAX_CHUNK_SIZE: usize = 65_535 - mem::size_of::<u16>();

#[derive(Debug)]
pub struct Connection {
    pub(crate) client: Client<Compat<BufStream<Stream>>>
}

impl Connection {
    pub async fn new(uri: &str, username: &str, password: &str) -> Result<Connection, Error> {

        // None::<String>.as_ref() ? but why
        let stream = Stream::connect(uri, None::<String>.as_ref()).await?;

        let stream = BufStream::new(stream).compat();

        let mut client = Client::new(stream, &[V4_4, V4_3, V4, 0]).await.unwrap();

        let response: Message = client.hello(
            Metadata::from_iter(vec![
                ("user_agent", "my-client-name/1.0"),
                ("scheme", "basic"),
                ("principal", &username),
                ("credentials", &password),
            ])).await?;

        match Success::try_from(response) {
            Ok(_) =>  Ok(Connection { client }),
            Err(err) => {
                println!("{}", err.to_string());
                Err(Error::ConversionError)
            },
        }

        // match client.hello(Metadata::from_iter(vec![
        //         ("user_agent", "neo4rs"),
        //         ("scheme", "basic"),
        //         ("principal", &username),
        //         ("credentials", &password),
        //     ])).await {
        //     Message::Success(_msg) => Ok(Connection {
        //         client
        //     }),
        //     Message::Failure(msg) => Err(Error::AuthenticationError(msg.get("message").unwrap())),
        //     msg => Err(unexpected(msg, "HELLO")),
        // }
        // match connection.send_recv(hello).await? {
        //     BoltResponse::SuccessMessage(_msg) => Ok(connection),
        //     BoltResponse::FailureMessage(msg) => {
        //         Err(Error::AuthenticationError(msg.get("message").unwrap()))
        //     }
        //
        //     msg => Err(unexpected(msg, "HELLO")),
        // }
    }

    pub async fn reset(&mut self) -> Result<(), Error> {
        self.client.reset().await?;
        Ok(())
    }
    //
    // pub async fn send_recv(&mut self, message: BoltRequest) -> Result<BoltResponse> {
    //     self.send(message).await?;
    //     self.recv().await
    // }
    //
    // pub async fn send(&mut self, message: BoltRequest) -> Result<()> {
    //     let end_marker: [u8; 2] = [0, 0];
    //     let bytes: Bytes = message.into_bytes(self.version)?;
    //     for c in bytes.chunks(MAX_CHUNK_SIZE) {
    //         self.stream.write_u16(c.len() as u16).await?;
    //         self.stream.write_all(c).await?;
    //     }
    //     self.stream.write_all(&end_marker).await?;
    //     self.stream.flush().await?;
    //     Ok(())
    // }
    //
    // pub async fn recv(&mut self) -> Result<BoltResponse> {
    //     let mut bytes = BytesMut::new();
    //     let mut chunk_size = 0;
    //     while chunk_size == 0 {
    //         chunk_size = self.read_u16().await?;
    //     }
    //
    //     while chunk_size > 0 {
    //         let chunk = self.read(chunk_size).await?;
    //         bytes.put_slice(&chunk);
    //         chunk_size = self.read_u16().await?;
    //     }
    //
    //     Ok(BoltResponse::parse(self.version, bytes.freeze())?)
    // }
    //
    // async fn read(&mut self, size: u16) -> Result<Vec<u8>> {
    //     let mut buf = vec![0; size as usize];
    //     self.stream.read_exact(&mut buf).await?;
    //     Ok(buf)
    // }
    //
    // async fn read_u16(&mut self) -> Result<u16> {
    //     let mut data = [0, 0];
    //     self.stream.read_exact(&mut data).await?;
    //     Ok(u16::from_be_bytes(data))
    // }
}
