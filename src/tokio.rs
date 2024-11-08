// Inspired by the incredible tokio-rusqlite crate
// https://github.com/programatik29/tokio-rusqlite/blob/master/src/lib.rs

use crossbeam_channel::Sender;
use tokio::sync::oneshot;

pub use crate::{FromRow, Value};

type Result<T> = std::result::Result<T, crate::Error>;

type CallFn = Box<dyn FnOnce(&mut crate::ffi::Sqlite) + Send + 'static>;

enum Message {
    Execute(CallFn),
    Close(oneshot::Sender<std::result::Result<(), crate::Error>>),
}

#[derive(Clone)]
pub struct Sqlite {
    sender: Sender<Message>,
}

impl Sqlite {
    pub async fn close(self) -> Result<()> {
        let (sender, receiver) = oneshot::channel::<std::result::Result<(), crate::Error>>();

        if let Err(crossbeam_channel::SendError(_)) = self.sender.send(Message::Close(sender)) {
            return Ok(());
        }

        let result = receiver.await;

        if result.is_err() {
            return Ok(());
        }

        result
            .unwrap()
            .map_err(|e| crate::Error::Sqlite(e.to_string()))
    }

    pub async fn call<F, R>(&self, function: F) -> Result<R>
    where
        F: FnOnce(&crate::ffi::Sqlite) -> Result<R> + 'static + Send,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<Result<R>>();

        self.sender
            .send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))
            .map_err(|_| crate::Error::ConnectionClosed)?;

        receiver.await.map_err(|_| crate::Error::ConnectionClosed)?
    }
}

pub async fn open(path: impl ToString) -> Result<Sqlite> {
    let path = path.to_string();
    start(move || crate::ffi::Sqlite::open(&path)).await
}

async fn start<F>(open: F) -> Result<Sqlite>
where
    F: FnOnce() -> Result<crate::ffi::Sqlite> + Send + 'static,
{
    let (sender, receiver) = crossbeam_channel::unbounded::<Message>();
    let (result_sender, result_receiver) = oneshot::channel();

    std::thread::spawn(move || {
        let mut conn = match open() {
            Ok(c) => c,
            Err(e) => {
                let _ = result_sender.send(Err(e));
                return;
            }
        };

        if let Err(_e) = result_sender.send(Ok(())) {
            return;
        }

        while let Ok(message) = receiver.recv() {
            match message {
                Message::Execute(f) => f(&mut conn),
                Message::Close(_s) => {
                    todo!("Message::Close")
                    // let result = drop(conn);

                    // match result {
                    //     Ok(v) => {
                    //         s.send(Ok(v)).expect("failed to send message");
                    //         break;
                    //     }
                    //     Err((c, e)) => {
                    //         conn = c;
                    //         s.send(Err(e)).expect("failed to receive message");
                    //     }
                    // }
                }
            }
        }
    });

    result_receiver
        .await
        .expect("failed to receive message")
        .map(|_| Sqlite { sender })
}

pub async fn execute(conn: &Sqlite, sql: String, params: Vec<Value>) -> Result<i32> {
    conn.call(move |conn| conn.execute(&sql, params)).await
}

pub async fn execute_all(conn: &Sqlite, sql: &'static str) -> Result<()> {
    let _ = conn.call(move |conn| conn.execute(sql, vec![])).await;
    Ok(())
}

pub async fn query<T: FromRow + Send + 'static>(
    conn: &Sqlite,
    sql: &'static str,
    params: &'static [Value],
) -> Result<Vec<T>> {
    conn.call(|conn| conn.query(sql, params)).await
}

pub async fn rows(
    conn: Sqlite,
    sql: &'static str,
    params: &'static [Value],
) -> Result<Vec<Vec<(String, Value)>>> {
    conn.call(|conn| conn.rows(sql, params)).await
}
