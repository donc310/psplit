use ini::{Error as IniError, Ini};
use libc::{c_int, mkfifo, mode_t, EACCES, EEXIST, ENOENT};
use mio::unix::pipe;
use mio::{Events, Interest, Poll, Token};
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::{thread, time};

const PIPE_RECV: Token = Token(0);
const PIPE_SEND: Token = Token(1);
const TIME_OUT: time::Duration = time::Duration::from_millis(100);
const SIG_RUN: u8 = 0;
const SIG_EXIT: u8 = 1;
const SIG_CLOSE: u8 = 2;

#[derive(Debug)]
/// Parse Error
enum ParseError {
    /// Error while parsing an INI document
    Ini(IniError),
    /// Error while generating SplitConfiguration
    Configuration(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParseError::Ini(ref err) => err.fmt(f),
            ParseError::Configuration(ref err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ParseError::Ini(ref err) => err.source(),
            ParseError::Configuration(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
enum OperationMode {
    StringRead,
    StringWrite,
    BytesRead,
    BytesWrite,
}

impl OperationMode {
    fn code(&self) -> &str {
        let code = match self {
            OperationMode::BytesRead => "rb",
            OperationMode::StringRead => "rt",
            OperationMode::StringWrite => "wt",
            OperationMode::BytesWrite => "wb",
        };
        code
    }
}

#[derive(Clone, Copy)]
struct Config {
    ///
    pub enabled: bool,
    ///
    pub mode: Option<OperationMode>,
}

impl Config {
    ///
    pub fn default_read() -> Config {
        Config {
            enabled: true,
            mode: Some(OperationMode::StringRead),
        }
    }
    ///
    pub fn default_write() -> Config {
        Config {
            enabled: true,
            mode: Some(OperationMode::StringWrite),
        }
    }
}

struct SplitOut {
    ///
    pub pipe: String,
    ///
    pub configuration: Config,
}

struct SplitIn {
    ///
    pub configuration: Config,
    ///
    pub outputs: Vec<Arc<SplitOut>>,
    ///
    pub pipe: String,
}

impl SplitIn {
    /// Count of enabled outputs
    pub fn enabled_outputs(&self) -> usize {
        self.outputs
            .iter()
            .filter(|x| x.configuration.enabled)
            .count()
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match self.mode {
            Some(op) => op.code().to_owned(),
            None => "*".to_string(),
        };
        write!(f, "[enabled: {}, mode: {}]", self.enabled, mode)
    }
}

impl fmt::Display for SplitOut {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OUT(pipe: {}, configuration: {})",
            self.pipe, self.configuration,
        )
    }
}

impl fmt::Display for SplitIn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IN(pipe: {}, configuration: {}, outputs: [count: {}, enabled: {}])",
            self.pipe,
            self.configuration,
            self.outputs.len(),
            self.enabled_outputs()
        )
    }
}

struct Parser;

impl Parser {
    ///
    fn get_read_config(config: &str) -> Result<Config, ParseError> {
        if config.is_empty() {
            return Ok(Config::default_read());
        }
        Self::get_split_configuration(config)
    }
    ///
    fn get_write_config(config: &str) -> Result<Config, ParseError> {
        if config.is_empty() {
            return Ok(Config::default_write());
        }
        Self::get_split_configuration(config)
    }
    ///
    fn get_root_directory(conf: &Ini) -> &str {
        let root = conf.get_from_or(Some("DEFAULT"), "root", "/tmp/cvnpipes");
        root
    }
    ///
    fn get_split_configuration(config: &str) -> Result<Config, ParseError> {
        let operation_config: Vec<&str> = config.split(",").collect();

        let enabled = match operation_config.get(0) {
            Some(s) => s.to_lowercase().as_str().eq("1"),
            None => false,
        };

        let mode = match operation_config.get(1) {
            Some(s) => match s.to_lowercase().as_str() {
                "rt" => Some(OperationMode::StringRead),
                "rb" => Some(OperationMode::BytesRead),
                "wt" => Some(OperationMode::StringWrite),
                "wb" => Some(OperationMode::BytesWrite),
                &_ => {
                    return Err(ParseError::Configuration(format!(
                        "Unknown operation type '{s}'"
                    )))
                }
            },
            None => None,
        };

        Ok(Config { enabled, mode })
    }
    ///
    fn get_split_outputs(
        conf: &Ini,
        input_pipe: &str,
        root: &str,
    ) -> Result<Vec<Arc<SplitOut>>, ParseError> {
        let outputs = if let Some(arg) = conf.section(Some(input_pipe)) {
            let mut out_puts = Vec::new();

            for (key, value) in arg.iter() {
                out_puts.push(Arc::new(SplitOut {
                    pipe: format!("{root}/{key}"),
                    configuration: Self::get_write_config(value)?,
                }))
            }

            out_puts
        } else {
            Vec::new()
        };
        Ok(outputs)
    }
    ///
    fn get_split_inputs(
        root: &str,
        input_pipes: &ini::Properties,
        conf: &Ini,
    ) -> Result<Vec<Arc<SplitIn>>, ParseError> {
        let mut split_configs = Vec::new();

        for (input_pipe, read_configuration) in input_pipes.iter() {
            let split_in = SplitIn {
                pipe: format!("{root}/{input_pipe}"),
                configuration: Self::get_read_config(read_configuration)?,
                outputs: Self::get_split_outputs(&conf, input_pipe, root)?,
            };

            split_configs.push(Arc::new(split_in));
        }
        Ok(split_configs)
    }
    ///
    fn parse_config(conf: &Ini) -> Result<Vec<Arc<SplitIn>>, ParseError> {
        let root = Self::get_root_directory(&conf);
        let root_path = Path::new(root);

        if !root_path.exists() {
            match fs::create_dir_all(root_path) {
                Err(_e) => {
                    return Err(ParseError::Configuration(
                        "Could not create pipe root directory".into(),
                    ));
                }
                _ => {}
            }
        }

        let input_pipes = match conf.section(Some("PIPES")) {
            Some(arg) => arg,
            None => {
                return Err(ParseError::Configuration(
                    "configuration must contain a 'PIPES' section".into(),
                ))
            }
        };

        Self::get_split_inputs(root, input_pipes, &conf)
    }

    ///
    fn load_ini_configuration<P: AsRef<Path>>(file_path: P) -> Result<Ini, ParseError> {
        let conf = match Ini::load_from_file(file_path) {
            Ok(config) => config,
            Err(e) => return Err(ParseError::Ini(e)),
        };

        Ok(conf)
    }

    /// Loading Splitting configuration from an INI formatted configuration file
    pub fn load_from_file<P: AsRef<Path>>(file_path: P) -> Result<Vec<Arc<SplitIn>>, ParseError> {
        let conf = match Self::load_ini_configuration(file_path) {
            Ok(value) => value,
            Err(value) => return Err(value),
        };

        let split_configs = match Self::parse_config(&conf) {
            Ok(value) => value,
            Err(value) => return Err(value),
        };

        Ok(split_configs)
    }
}

///
struct Writer {
    /// Flag to control Writing thread
    signal: Arc<Mutex<u8>>,
    /// Write output configuration
    config: Arc<SplitOut>,
    /// Receiving channel for write data
    receiver: mpsc::Receiver<String>,
    /// Flag to ignore first data from channel
    ignore_first_message: bool,
}

enum WriteFlow {
    ///
    Break,
    ///
    Restart,
    ///
    ClosePipe,
}
///
impl<'a> Writer {
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
    fn should_stop(&mut self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == SIG_EXIT
    }
    ///
    fn should_close_pipe(&mut self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == SIG_CLOSE
    }

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
    fn run_loop(&mut self) -> Result<(), std::io::Error> {
        loop {
            // Exit loop
            if self.should_stop() {
                break;
            }

            // At this point reader is'nt reading any data, so don't open the pipe
            if self.should_close_pipe() {
                thread::sleep(TIME_OUT);
                continue;
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

            let mut sender = unsafe {
                let fd = pipe.into_raw_fd();
                pipe::Sender::from_raw_fd(fd)
            };

            poll.registry()
                .register(&mut sender, PIPE_SEND, Interest::WRITABLE)?;

            println!("Writing data -> {}", &self.config);

            match self.loop_till_stopped(&mut poll, &sender) {
                WriteFlow::Break => {
                    break;
                }
                WriteFlow::Restart | WriteFlow::ClosePipe => {
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
            // Exit loop
            if self.should_stop() {
                break;
            }

            // If the reader is'nt reading any data close the target pipe
            if self.should_close_pipe() {
                return WriteFlow::ClosePipe;
            }

            match poll.poll(&mut events, Some(TIME_OUT)) {
                Ok(_) => {}
                Err(_) => {
                    return WriteFlow::Restart;
                }
            };

            for event in &events {
                if event.token() == PIPE_SEND && event.is_writable() {
                    let flow = self.loop_write_messages(event, sender);
                    println!("Stopping write <> {}", &self.config);
                    return flow;
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
            // If the reader is'nt reading any data close the target pipe
            if self.should_close_pipe() {
                return WriteFlow::ClosePipe;
            }

            match self.receiver.recv_timeout(TIME_OUT) {
                Ok(m) => {
                    if self.ignore_first_message {
                        self.ignore_first_message = false;
                        continue;
                    }
                    let contents = m.as_bytes();

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
                Err(e) => match e {
                    mpsc::RecvTimeoutError::Timeout => {
                        thread::sleep(TIME_OUT);
                        continue;
                    }
                    mpsc::RecvTimeoutError::Disconnected => {
                        // Sending End has disconnected
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
        receiver: mpsc::Receiver<String>,
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
    sender: mpsc::SyncSender<String>,
}

///
struct Reader {
    signal: Arc<Mutex<u8>>,
    config: Arc<SplitIn>,
    send_channels: Vec<MessageSender>,
    write_signal: Arc<Mutex<u8>>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stop_writers()
    }
}

///
impl<'a> Reader {
    ///
    fn should_stop(&self) -> bool {
        let state = self.signal.lock().unwrap();
        *state == SIG_EXIT
    }
    ///
    fn stop_writers(&mut self) {
        // Signal exit
        let mut num = self.write_signal.lock().unwrap();
        *num = SIG_EXIT;
    }
    ///
    fn close_writing_pipes(&mut self) {
        let mut num = self.write_signal.lock().unwrap();
        *num = SIG_CLOSE;
    }
    ///
    fn open_writing_pipes(&mut self) {
        let mut num = self.write_signal.lock().unwrap();
        *num = SIG_RUN;
    }
    ///
    fn send_message(&mut self, m: String) {
        for c in self.send_channels.iter_mut() {
            if c.disconnected {
                continue;
            }
            match c.sender.try_send(m.clone()) {
                Err(e) => match e {
                    mpsc::TrySendError::Disconnected(_) => {
                        c.disconnected = true;
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }

    ///
    fn new(signal: Arc<Mutex<u8>>, config: Arc<SplitIn>) -> Reader {
        let cap = config.outputs.len();

        Reader {
            signal,
            config,
            write_signal: Arc::new(Mutex::new(SIG_CLOSE)),
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
    fn run(&mut self) -> Result<(), std::io::Error> {
        let pipe = match self.open_pipe() {
            Ok(f) => f,
            Err(e) => {
                println!("File -> {} Error {:?} ", &self.config.pipe, e);
                return Err(e);
            }
        };
        let mut poll = Poll::new()?;
        let mut receiver = unsafe {
            let fd = pipe.into_raw_fd();
            pipe::Receiver::from_raw_fd(fd)
        };
        let mut reader = unsafe {
            let fd = receiver.as_raw_fd();
            std::io::BufReader::new(File::from_raw_fd(fd))
        };

        poll.registry()
            .register(&mut receiver, PIPE_RECV, Interest::READABLE)?;

        println!("Reading data <- {}", &self.config);

        match self.loop_till_stopped(&mut poll, &mut reader) {
            Ok(_) => return Ok(()),
            Err(err) => {
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
                self.stop_writers();
                break;
            }

            poll.poll(&mut events, Some(TIME_OUT))?;

            for event in &events {
                if event.token() == PIPE_RECV && event.is_readable() {
                    self.open_writing_pipes();
                    self.loop_read_pipe(event, reader);
                    println!("Stopping read <> {}", &self.config);
                    self.close_writing_pipes();
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
                    self.send_message(buffer);
                }
                Err(err) => match err.kind() {
                    io::ErrorKind::BrokenPipe => {
                        println!("{:?}", err)
                    }
                    io::ErrorKind::WouldBlock => {
                        // Pipe has no data to be read
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

fn create_splitting_threads(
    entries: &Vec<Arc<SplitIn>>,
    signal: &Arc<Mutex<u8>>,
) -> Vec<thread::JoinHandle<Result<(), std::io::Error>>> {
    let mut reading_threads = Vec::with_capacity(entries.len());

    for input in entries.iter() {
        if !input.configuration.enabled || input.enabled_outputs() == 0 {
            continue;
        }

        let signal = Arc::clone(&signal);
        let config = Arc::clone(input);

        let handle = thread::spawn(move || -> Result<(), std::io::Error> {
            let mut reader = Reader::new(signal, config);
            reader.start_write_channels().run()
        });

        reading_threads.push(handle);
    }
    
    reading_threads
}

///
pub fn split_pipes<P: AsRef<Path>>(config_path: P) -> Result<(), std::io::Error> {
    let entries = match Parser::load_from_file(config_path) {
        Ok(r) => r,
        Err(e) => panic!("{}", e),
    };

    if entries.len() == 0 {
        return Ok(());
    }

    let signal = Arc::new(Mutex::new(SIG_RUN));
    let _splitting_threads = create_splitting_threads(&entries, &signal);

    loop {
        thread::sleep(TIME_OUT);
    }
}
///
#[cfg(test)]
mod test {
    use super::*;
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn load_from_file() {
        let file_name = temp_dir().join("p_split_split_config");
        let file_content = "
[DEFAULT]
root=/tmp
[PIPES]
cvAnalogsMapperExt=
[cvAnalogsMapperExt]
cvAnalogsMapperExtFuelApp=
"
        .as_bytes();

        {
            let mut file = File::create(&file_name).expect("create");
            file.write_all(file_content).expect("write");
        }
        let config = Parser::load_from_file(&file_name).expect("Should load configuration ");

        assert_eq!(1, config.len());

        let first_config = config.get(0).unwrap();

        assert_eq!(1, first_config.outputs.len());
        assert!(first_config.configuration.enabled, "Should be enabled");

        assert_eq!(
            1,
            first_config
                .outputs
                .iter()
                .filter(|x| x.configuration.enabled)
                .count()
        )
    }
    #[test]
    fn needs_pipes_section() {
        let file_name = temp_dir().join("p_split_bad_config_pipes");
        let file_content = "
[DEFAULT]
root=/tmp
[cvAnalogsMapperExt]
cvAnalogsMapperExtFuelApp=
"
        .as_bytes();

        {
            let mut file = File::create(&file_name).expect("create");
            file.write_all(file_content).expect("write");
        }
        let config = Parser::load_from_file(&file_name);
        assert_eq!(config.is_err(), true);
        let error_matches = match config {
            Err(e) => match e {
                ParseError::Configuration(s) => {
                    s.as_str() == "configuration must contain a 'PIPES' section"
                }
                _ => false,
            },
            Ok(_) => false,
        };
        assert_eq!(error_matches, true);
    }
    #[test]
    fn valid_pipe_configuration() {
        let file_name = temp_dir().join("p_split_bad_config_configuration");
        let file_content = "
[DEFAULT]
root=/tmp
[PIPES]
cvAnalogsMapperExt=
[cvAnalogsMapperExt]
cvAnalogsMapperExtFuelApp=1,wf
"
        .as_bytes();

        {
            let mut file = File::create(&file_name).expect("create");
            file.write_all(file_content).expect("write");
        }
        let config = Parser::load_from_file(&file_name);
        assert_eq!(config.is_err(), true);

        let error_matches = match config {
            Err(e) => match e {
                ParseError::Configuration(s) => s.as_str() == "Unknown operation type 'wf'",
                _ => false,
            },
            Ok(_) => false,
        };

        assert_eq!(error_matches, true);
    }
    #[test]
    fn test_it_works() {
        let file_name = temp_dir().join("pipe_split");
        let file_content = "
[DEFAULT]
root=/tmp
[PIPES]
cvAnalogsMapperExt=
[cvAnalogsMapperExt]
cvAnalogsMapperExtFuelApp=
"
        .as_bytes();

        {
            let mut file = File::create(&file_name).expect("create");
            file.write_all(file_content).expect("write");
        }

        let _handle =
            thread::spawn(move || -> Result<(), std::io::Error> { split_pipes(&file_name) });

        thread::sleep(time::Duration::from_secs(20))
    }
}
