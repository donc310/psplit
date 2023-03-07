mod parser;

use libc::{c_int, mkfifo, mode_t, EACCES, EEXIST, ENOENT};
use parser::{Parser, SplitIn, SplitOut};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::{thread, time};

use std::os::fd::AsRawFd;
use std::os::unix::io::FromRawFd;

use mio::unix::pipe;
use mio::{Events, Interest, Poll, Token};
use std::os::unix::io::IntoRawFd;

const PIPE_RECV: Token = Token(0);
const PIPE_SEND: Token = Token(1);
const TIME_OUT: time::Duration = time::Duration::from_millis(100);
///
#[derive(Debug)]
enum Message {
    Write(Box<String>),
    Terminate,
}

///
///
///
enum WriteFlow {
    Break,
    Restart,
}
///
struct Writer {
    signal: Arc<Mutex<u8>>,
    config: Arc<SplitOut>,
    receiver: mpsc::Receiver<Message>,
    ignore_first_message: bool,
}
///
impl<'a> Writer {
    ///
    ///
    ///
    ///
    fn create<P: AsRef<Path>>(path: P, mode: Option<u32>) -> io::Result<()> {
        let path = CString::new(path.as_ref().to_str().unwrap())?;
        let mode = mode.unwrap_or(0o644);
        let result: c_int = unsafe { mkfifo(path.as_ptr(), mode as mode_t) };

        let result: i32 = result.into();
        if result == 0 {
            return Ok(());
        }

        let error = errno::errno();
        match error.0 {
            EACCES => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("could not open {:?}: {}", path, error),
                ));
            }
            EEXIST => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("could not open {:?}: {}", path, error),
                ));
            }
            ENOENT => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("could not open {:?}: {}", path, error),
                ));
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("could not open {:?}: {}", path, error),
                ));
            }
        }
    }
    ///
    ///
    ///
    ///
    fn open_pipe(&mut self) -> Result<File, std::io::Error> {
        let pipe = self.config.pipe.clone();

        match Self::create(&pipe, Some(0o777)) {
            Ok(_) => {}
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => {}
                _ => return Err(e),
            },
        };

        let f = OpenOptions::new()
            .write(true)
            .append(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(Path::new(&pipe));

        f
    }
    ///
    ///
    ///
    ///
    fn should_stop(&mut self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == 1
    }
    ///
    ///
    ///
    ///
    fn write(&mut self, contents: &[u8], sender: &pipe::Sender) -> Result<usize, io::Error> {
        let op = sender.try_io(|| {
            let buf_ptr = contents as *const _ as *const _;
            let res = unsafe { libc::write(sender.as_raw_fd(), buf_ptr, contents.len()) };
            if res != -1 {
                Ok(res as usize)
            } else {
                Err(io::Error::last_os_error())
            }
        });
        op
    }

    ///
    ///
    ///
    ///
    ///
    fn run_loop(&mut self) -> Result<(), std::io::Error> {
        loop {
            if self.should_stop() {
                break;
            }

            let pipe = match self.open_pipe() {
                Ok(f) => f,
                Err(e) => match e.kind() {
                    io::ErrorKind::PermissionDenied => {
                        return Err(e);
                    }
                    _ => {
                        thread::sleep(TIME_OUT);
                        continue;
                    }
                },
            };

            let mut poll = Poll::new()?;

            let mut sender = unsafe { pipe::Sender::from_raw_fd(pipe.into_raw_fd()) };

            poll.registry()
                .register(&mut sender, PIPE_SEND, Interest::WRITABLE)?;

            println!("Writing data -> {}", &self.config.pipe);

            match self.loop_till_stopped(&mut poll, &sender) {
                WriteFlow::Break => {
                    break;
                }
                WriteFlow::Restart => {
                    continue;
                }
            }
        }

        Ok(())
    }

    ///
    ///
    ///
    ///
    fn loop_till_stopped(&mut self, poll: &mut Poll, sender: &pipe::Sender) -> WriteFlow {
        let mut events = Events::with_capacity(8);
        loop {
            if self.should_stop() {
                break;
            }

            match poll.poll(&mut events, Some(TIME_OUT)) {
                Ok(_) => {}
                Err(_) => {
                    return WriteFlow::Restart;
                }
            };

            for event in &events {
                if event.token() == PIPE_SEND && event.is_writable() {
                    match self.loop_write_messages(event, sender) {
                        WriteFlow::Break => return WriteFlow::Break,
                        WriteFlow::Restart => return WriteFlow::Restart,
                    };
                }
            }
        }
        WriteFlow::Break
    }

    /// Read messages from channel while sender is writable
    fn loop_write_messages(
        &mut self,
        event: &mio::event::Event,
        sender: &pipe::Sender,
    ) -> WriteFlow {
        loop {
            if event.is_write_closed() || self.should_stop() {
                break;
            }

            match self.receiver.recv_timeout(TIME_OUT) {
                Ok(m) => match m {
                    Message::Write(data) => {
                        let contents = data.as_bytes();
                        if self.ignore_first_message{
                            self.ignore_first_message = false;
                            continue;
                        }
                        match self.write(contents, sender) {
                            Err(e) => match e.kind() {
                                io::ErrorKind::BrokenPipe => {
                                    self.ignore_first_message = true;
                                    return WriteFlow::Restart;
                                }
                                _others => {
                                    println!("{}", e)
                                }
                            },
                            _ => {}
                        }
                    }

                    Message::Terminate => {
                        return WriteFlow::Break;
                    }
                },
                Err(e) => match e {
                    mpsc::RecvTimeoutError::Timeout => {
                        thread::sleep(TIME_OUT);
                        continue;
                    }
                    mpsc::RecvTimeoutError::Disconnected => {
                        return WriteFlow::Break;
                    }
                },
            };
        }

        WriteFlow::Break
    }

    ///
    ///
    ///
    fn new(
        signal: Arc<Mutex<u8>>,
        config: Arc<SplitOut>,
        receiver: mpsc::Receiver<Message>,
    ) -> Writer {
        Writer {
            ignore_first_message: false,
            signal,
            config,
            receiver,
        }
    }
}

struct MessageSender {
    /// if the sender has been dropped
    disconnected: bool,
    /// send channel
    sender: mpsc::SyncSender<Message>,
}

///
struct Reader {
    signal: Arc<Mutex<u8>>,
    config: Arc<SplitIn>,
    send_channels: Vec<MessageSender>,
    write_signal: Arc<Mutex<u8>>,
}

impl<'a> Reader {
    ///
    fn should_stop(&self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == 1
    }

    ///
    fn send_message(&mut self, m: Message) {
        for c in self.send_channels.iter_mut() {
            if c.disconnected {
                continue;
            }
            match m {
                Message::Write(ref data) => {
                    match c.sender.try_send(Message::Write(Box::new(*data.clone()))) {
                        Err(e) => match e {
                            mpsc::TrySendError::Disconnected(_) => {
                                c.disconnected = true;
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }

                Message::Terminate => match c.sender.try_send(Message::Terminate) {
                    // On failure disconnect
                    Err(_e) => {
                        c.disconnected = true;
                    }
                    _ => {}
                },
            };
        }
    }

    ///
    fn new(signal: Arc<Mutex<u8>>, config: Arc<SplitIn>) -> Reader {
        let cap = config.outputs.len();
        Reader {
            signal,
            config,
            write_signal: Arc::new(Mutex::new(0)),
            send_channels: Vec::with_capacity(cap),
        }
    }

    ///
    fn open_pipe(&mut self) -> Result<File, std::io::Error> {
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(Path::new(&self.config.pipe));

        f
    }

    /// Create output workers defined in the
    fn start_write_channels(&'a mut self) -> &'a mut Self {
        self.send_channels.clear();

        for out in self.config.outputs.iter() {
            if !out.configuration.enabled {
                continue;
            }
            let signal = Arc::clone(&self.write_signal);
            let config = Arc::clone(out);

            let (sender, receiver) = mpsc::sync_channel(1);

            self.send_channels.push(MessageSender {
                disconnected: false,
                sender,
            });

            thread::spawn(move || -> Result<(), std::io::Error> {
                let mut witter = Writer::new(signal, config, receiver);
                witter.run_loop()
            });
        }
        self
    }

    ///
    fn run_read_loop(&mut self) -> Result<(), std::io::Error> {
        let pipe = match self.open_pipe() {
            Ok(f) => f,
            Err(e) => {
                println!("File -> {} Error {:?} ", &self.config.pipe, e);
                return Err(e);
            }
        };
        let mut poll = Poll::new()?;

        let mut receiver = unsafe { pipe::Receiver::from_raw_fd(pipe.into_raw_fd()) };
        let mut reader =
            unsafe { std::io::BufReader::new(File::from_raw_fd(receiver.as_raw_fd())) };

        poll.registry()
            .register(&mut receiver, PIPE_RECV, Interest::READABLE)?;

        println!("Reading data <- {}", &self.config.pipe);

        match self.loop_till_stopped(&mut poll, &mut reader) {
            Ok(_) => return Ok(()),
            Err(err) => {
                println!("{:?}", err);
                return Err(err);
            }
        }
    }

    ///
    fn loop_till_stopped(
        &mut self,
        poll: &mut Poll,
        reader: &mut BufReader<File>,
    ) -> Result<(), std::io::Error> {
        let mut events = Events::with_capacity(8);
        loop {
            if self.should_stop() {
                self.send_message(Message::Terminate);
                break;
            }
            poll.poll(&mut events, Some(TIME_OUT))?;

            for event in &events {
                if event.token() == PIPE_RECV && event.is_readable() {
                    self.loop_read_pipe(event, reader);
                }
            }
        }

        Ok(())
    }

    ///
    fn loop_read_pipe(&mut self, event: &mio::event::Event, reader: &mut BufReader<File>) {
        loop {
            if event.is_read_closed() {
                break;
            }
            let mut buffer = String::new();

            match std::io::BufRead::read_line(reader, &mut buffer) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        break;
                    }
                    self.send_message(Message::Write(Box::new(buffer)));
                }
                Err(err) => match err.kind() {
                    io::ErrorKind::BrokenPipe => {
                        println!("{:?}", err)
                    }
                    io::ErrorKind::WouldBlock => {
                        thread::sleep(TIME_OUT);
                    }
                    _ => {
                        println!("{:?}", err)
                    }
                },
            };
        }
    }
}
///
pub struct PSplit {
    reading_threads: Vec<thread::JoinHandle<Result<(), std::io::Error>>>,
    running: bool,
    signal: Arc<Mutex<u8>>,
    configs: Vec<Arc<SplitIn>>,
}

impl PSplit {
    fn loop_for_ever() -> ! {
        loop {
            thread::sleep(TIME_OUT);
        }
    }
}

impl<'a> PSplit {
    ///
    pub fn new() -> PSplit {
        PSplit {
            signal: Arc::new(Mutex::new(0)),
            configs: Vec::new(),
            reading_threads: Vec::new(),
            running: false,
        }
    }

    ///
    pub fn config_from_file(&'a mut self, config_path: &str) -> &'a mut Self {
        let config = match Parser::load_from_file(config_path) {
            Ok(r) => r,
            Err(e) => panic!("{}", e),
        };
        self.configs = config;
        self
    }

    ///
    pub fn start(&mut self) {
        if self.configs.len() == 0 || self.running {
            return;
        }

        for input in self.configs.iter() {
            if !input.configuration.enabled || input.enabled_outputs() == 0 {
                continue;
            }

            let signal = Arc::clone(&self.signal);
            let config = Arc::clone(input);

            let handle = thread::spawn(move || -> Result<(), std::io::Error> {
                let mut reader = Reader::new(signal, config);

                reader.start_write_channels().run_read_loop()
            });

            self.reading_threads.push(handle);
        }

        self.running = true;
        Self::loop_for_ever();
    }

    ///
    fn shut_down(&mut self) {
        let mut num = self.signal.lock().unwrap();
        *num = 1;
        self.running = false;
    }

    ///
    pub fn stop(&mut self) {}
}

impl Drop for PSplit {
    fn drop(&mut self) {
        self.shut_down()
    }
}

impl Drop for Writer {
    fn drop(&mut self) {}
}

impl Drop for Reader {
    fn drop(&mut self) {
        let mut num = self.signal.lock().unwrap();
        *num = 1;
    }
}
