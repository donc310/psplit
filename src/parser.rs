use ini::{Error as IniError, Ini};
use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;
#[derive(Debug)]
pub enum Error {
    /// Error while parsing an INI document
    Ini(IniError),
    /// Error while generating SplitConfiguration
    Configuration(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Ini(ref err) => err.fmt(f),
            Error::Configuration(ref err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Error::Ini(ref err) => err.source(),
            Error::Configuration(_) => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OperationMode {
    StringRead,
    StringWrite,
    BytesRead,
    BytesWrite,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Config {
    pub enabled: bool,
    pub mode: Option<OperationMode>,
}

impl Config {
    pub fn default_read() -> Config {
        Config {
            enabled: true,
            mode: Some(OperationMode::StringRead),
        }
    }
    pub fn default_write() -> Config {
        Config {
            enabled: true,
            mode: Some(OperationMode::StringWrite),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SplitOut {
    pub pipe: String,
    pub configuration: Config,
}

#[derive(Debug, PartialEq)]
pub struct SplitIn {
    pub configuration: Config,
    pub outputs: Vec<Arc<SplitOut>>,
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

impl fmt::Display for SplitIn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SplitConfig(pipe: {} enabled:{}, outputs: {})",
            self.pipe,
            self.configuration.enabled,
            self.outputs.len()
        )
    }
}

impl fmt::Display for OperationMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mode = match self {
            OperationMode::BytesRead => "rb",
            OperationMode::StringRead => "rt",
            OperationMode::StringWrite => "wt",
            OperationMode::BytesWrite => "wb",
        };
        write!(f, "{}", mode)
    }
}

impl fmt::Display for SplitOut {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SplitOut (pipe: {} enabled:{}, mode: {:?})",
            self.pipe, self.configuration.enabled, self.configuration.mode
        )
    }
}

pub struct Parser;

impl Parser {
    ///
    fn get_read_config(config: &str) -> Result<Config, Error> {
        if config.is_empty() {
            return Ok(Config::default_read());
        }
        Self::get_split_configuration(config)
    }
    ///
    fn get_write_config(config: &str) -> Result<Config, Error> {
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
    fn get_split_configuration(config: &str) -> Result<Config, Error> {
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
                    return Err(Error::Configuration(format!(
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
    ) -> Result<Vec<Arc<SplitOut>>, Error> {
        let outputs = if let Some(arg) = conf.section(Some(input_pipe)) {
            let mut out_puts = Vec::new();

            for (key, value) in arg.iter() {
                let configuration = Parser::get_write_config(value)?;
                let pipe = format!("{root}/{key}");

                out_puts.push(Arc::new(SplitOut {
                    pipe,
                    configuration,
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
    ) -> Result<Vec<Arc<SplitIn>>, Error> {
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
    fn parse_config(conf: &Ini) -> Result<Vec<Arc<SplitIn>>, Error>{
        let root = Self::get_root_directory(&conf);
        let root_path = Path::new(root);

        if !root_path.exists() {
            match fs::create_dir_all(root_path) {
                Err(_e) => {
                    return Err(Error::Configuration(
                        "Could not create pipe root directory".into(),
                    ));
                }
                _ => {}
            }
        }

        let input_pipes = match conf.section(Some("PIPES")) {
            Some(arg) => arg,
            None => {
                return Err(Error::Configuration(
                    "configuration must contain a 'PIPES' section".into(),
                ))
            }
        };

        Self::get_split_inputs(root, input_pipes, &conf)
    }
    
    ///
    fn load_ini_configuration<P: AsRef<Path>>(file_path: P) -> Result<Ini, Error> {
        let conf = match Ini::load_from_file(file_path) {
            Ok(config) => config,
            Err(e) => return Err(Error::Ini(e)),
        };

        Ok(conf)
    }

    /// Loading Splitting configuration from an INI formatted configuration file
    pub fn load_from_file<P: AsRef<Path>>(file_path: P) -> Result<Vec<Arc<SplitIn>>, Error> {
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

#[cfg(test)]
mod tests {
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
                Error::Configuration(s) => {
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
                Error::Configuration(s) => s.as_str() == "Unknown operation type 'wf'",
                _ => false,
            },
            Ok(_) => false,
        };

        assert_eq!(error_matches, true);
    }
}
