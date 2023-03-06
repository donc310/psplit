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
const TIME_OUT: time::Duration = time::Duration::from_secs(1);
///
pub enum Message {
    Write(Box<String>),
    Terminate,
}
///
struct Writer {
    signal: Arc<Mutex<u8>>,
    config: SplitOut,
    receiver: mpsc::Receiver<Message>,
}

///
struct Reader {
    signal: Arc<Mutex<u8>>,
    write_signal: Arc<Mutex<u8>>,
}
///
///
///
pub struct PSplit {
    reading_threads: Vec<thread::JoinHandle<Result<(), std::io::Error>>>,
    running: bool,
    signal: Arc<Mutex<u8>>,
}
///
///
///
pub enum WriteFlow {
    Break,
    Restart,
}

impl Writer {
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
    fn open_pipe(&self) -> Result<File, std::io::Error> {
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
    fn should_stop(&self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == 1
    }
    ///
    ///
    ///
    ///
    fn write(&self, contents: &[u8], sender: &pipe::Sender) -> Result<usize, io::Error> {
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
    fn run_loop(&self) -> Result<(), std::io::Error> {
        let one_secs = time::Duration::from_secs(1);

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
                    io::ErrorKind::Unsupported => todo!(),
                    io::ErrorKind::UnexpectedEof => todo!(),
                    io::ErrorKind::OutOfMemory => todo!(),
                    io::ErrorKind::Other => todo!(),
                    _ => {
                        thread::sleep(one_secs);
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
    fn loop_till_stopped(&self, poll: &mut Poll, sender: &pipe::Sender) -> WriteFlow {
        let mut events = Events::with_capacity(8);
        let timeout = Some(time::Duration::from_secs(2));

        loop {
            if self.should_stop() {
                break;
            }

            match poll.poll(&mut events, timeout) {
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
    ///
    ///
    ///
    fn loop_write_messages(&self, event: &mio::event::Event, sender: &pipe::Sender) -> WriteFlow {
        loop {
            if event.is_write_closed() || self.should_stop() {
                break;
            }

            match self.receiver.recv_timeout(TIME_OUT) {
                Ok(m) => match m {
                    Message::Write(_data) => {
                        let contents = _data.as_bytes();
                        match self.write(contents, sender) {
                            Err(e) => match e.kind() {
                                io::ErrorKind::BrokenPipe => return WriteFlow::Restart,
                                _ => {}
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
    fn new(signal: Arc<Mutex<u8>>, config: SplitOut, receiver: mpsc::Receiver<Message>) -> Writer {
        Writer {
            signal,
            config,
            receiver,
        }
    }
}

impl Reader {
    ///
    ///
    ///
    fn should_stop(&self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == 1
    }
    ///
    ///
    ///
    ///
    fn send_message(&self, m: Message, channels: &Vec<mpsc::SyncSender<Message>>) {
        for c in channels {
            match m {
                Message::Write(ref data) => {
                    match c.try_send(Message::Write(Box::new(*data.clone()))) {
                        Ok(_) => {}
                        Err(e) => match e {
                            mpsc::TrySendError::Full(_) => {
                                println!("{}", e)
                            }
                            mpsc::TrySendError::Disconnected(_) => {
                                println!("{}", e)
                            }
                        },
                    }
                }
                Message::Terminate => match c.try_send(Message::Terminate) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e)
                    }
                },
            };
        }
    }
    ///
    ///
    ///
    ///
    fn new(signal: Arc<Mutex<u8>>) -> Reader {
        Reader {
            signal,
            write_signal: Arc::new(Mutex::new(0)),
        }
    }
    ///
    ///
    ///
    ///
    ///
    fn open_pipe(pipe: &String) -> Result<File, std::io::Error> {
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(Path::new(&pipe));

        f
    }

    // Create output workers defined in the
    //
    //
    fn start_write_channels(&self, split_config: SplitIn) -> Vec<mpsc::SyncSender<Message>> {
        let mut channels = Vec::new();

        for out in split_config.outputs.into_iter() {
            if !out.configuration.enabled {
                continue;
            }
            let signal = Arc::clone(&self.write_signal);
            let (sender, receiver) = mpsc::sync_channel(1);

            channels.push(sender);

            thread::spawn(move || -> Result<(), std::io::Error> {
                let witter = Writer::new(signal, out, receiver);
                witter.run_loop()
            });
        }

        channels
    }
    ///
    ///
    ///
    ///
    fn loop_read_pipe(
        &self,
        event: &mio::event::Event,
        reader: &mut BufReader<File>,
        channels: &Vec<mpsc::SyncSender<Message>>,
    ) {
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
                    self.send_message(Message::Write(Box::new(buffer)), channels);
                }
                Err(err) => match err.kind() {
                    io::ErrorKind::BrokenPipe => {
                        println!("{:?}", err)
                    }
                    io::ErrorKind::AlreadyExists => {
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
    ///
    ///
    ///
    ///
    fn loop_till_stopped(
        &self,
        poll: &mut Poll,
        reader: &mut BufReader<File>,
        channels: &Vec<mpsc::SyncSender<Message>>,
    ) -> Result<(), std::io::Error> {
        let mut events = Events::with_capacity(8);
        let timeout = Some(time::Duration::from_secs(2));

        loop {
            if self.should_stop() {
                self.send_message(Message::Terminate, channels);
                break;
            }
            poll.poll(&mut events, timeout)?;

            for event in &events {
                if event.token() == PIPE_RECV && event.is_readable() {
                    self.loop_read_pipe(event, reader, channels)
                }
            }
        }

        Ok(())
    }
    ///
    ///
    ///
    fn start(&self, split_config: SplitIn) -> Result<(), std::io::Error> {
        let pipe_name = split_config.pipe.clone();

        let pipe = match Self::open_pipe(&pipe_name) {
            Ok(f) => f,
            Err(e) => {
                println!("File -> {} Error {:?} ", &pipe_name, e);
                return Err(e);
            }
        };

        let channels = self.start_write_channels(split_config);

        if channels.len() == 0 {
            return Ok(());
        }

        let mut poll = Poll::new()?;

        let mut receiver = unsafe { pipe::Receiver::from_raw_fd(pipe.into_raw_fd()) };
        let mut reader =
            unsafe { std::io::BufReader::new(File::from_raw_fd(receiver.as_raw_fd())) };

        poll.registry()
            .register(&mut receiver, PIPE_RECV, Interest::READABLE)?;

        println!("Reading data <- {}", &pipe_name);

        match self.loop_till_stopped(&mut poll, &mut reader, &channels) {
            Ok(_) => return Ok(()),
            Err(err) => {
                println!("{:?}", err);
                return Err(err);
            }
        }
    }
}

impl PSplit {
    fn loop_for_ever() -> ! {
        loop {
            thread::sleep(TIME_OUT);
        }
    }

    pub fn new() -> PSplit {
        PSplit {
            signal: Arc::new(Mutex::new(0)),
            reading_threads: Vec::new(),
            running: false,
        }
    }

    fn shut_down(&mut self) {
        let mut num = self.signal.lock().unwrap();
        *num = 1;
        self.running = false;
    }

    fn count_enabled_outputs(split_config: &SplitIn) -> i32 {
        let mut i = 0;

        for out in split_config.outputs.iter() {
            if out.configuration.enabled {
                i += 1;
            }
        }

        return i;
    }

    pub fn start(&mut self, config_path: &str) {
        let config = match Parser::load_from_file(config_path) {
            Ok(r) => r,
            Err(e) => panic!("{}", e),
        };

        self.running = true;

        for input in config.into_iter() {
            if !input.configuration.enabled || Self::count_enabled_outputs(&input) == 0 {
                continue;
            }

            let signal = Arc::clone(&self.signal);

            let handle = thread::spawn(move || -> Result<(), std::io::Error> {
                let reader = Reader::new(signal);
                let response = reader.start(input);
                response
            });

            self.reading_threads.push(handle);
        }

        Self::loop_for_ever();
    }

    pub fn stop(&mut self) {}
}

impl Drop for PSplit {
    fn drop(&mut self) {
        self.shut_down()
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        println!("Dropping!!!")
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let mut num = self.signal.lock().unwrap();
        *num = 1;
    }
}
