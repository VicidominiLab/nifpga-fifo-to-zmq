extern crate nifpga_dll;

use std::path::PathBuf;
use std::process;
use std::time::Duration;

//use std::thread;
//use std::thread::available_parallelism;
//use std::sync::mpsc;
//use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crossbeam;

use clap::{Parser};
use nifpga_dll::{NifpgaError, Session, ReadFifo, WriteFifo, ReadElements};


#[derive(Parser)]
#[command(name = "nifpga-fifo-to-zmq")]
#[command(author = "Mattia Donato")]
#[command(version = "1.0")]
#[command(about = "Read data from NI FPGA FIFO and transmit data via ZMQ PUSH.")]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Set the bit_file name
    bit_file: Option<PathBuf>,

    /// Sets a custom config file
    #[arg(short, long, value_name = "SIGNATURE",  default_value = "")]
    signature: Option<String>,

    /// Turn debugging information on
    #[arg(short, long, value_name = "NI_ADDR",  default_value = "RIO0")]
    ni_address: Option<String>,

    #[arg(short, long, value_name = "false",  default_value = "false")]
    run: Option<bool>,

    #[arg(short,long, value_name = "false",  default_value = "false")]
    close_on_reset: Option<bool>,

    #[arg(short, long, value_name = "0",  default_value = "0")]
    fifo: Option<u32>,

    #[arg(long, value_name = "50000",  default_value = "50000")]
    dma_buffer_size: Option<usize>,

    #[arg(long, value_name = "5000",  default_value = "5000")]
    fifo_reading_buffer: Option<usize>,

    #[arg(short, long, value_name = "13123",  default_value = "13123")]
    port: Option<usize>,

    #[arg(short, long, value_name = "1",  default_value = "1")]
    min_packet: Option<usize>,

}

#[derive(Clone)]
struct Configuration {
    bit_file: String,
    signature: String,
    ni_address: String,
    run: bool,
    close_on_reset: bool,
    fifo: u32,
    port: usize,
    dma_buffer_size: usize,
    fifo_reading_buffer: usize,
    min_packet: usize
}

impl Configuration{
    fn new()->Configuration{
        Configuration {
            bit_file: String::from(""),
            signature: String::from(""),
            ni_address: String::from(""),
            run: false,
            close_on_reset: false,
            fifo: 0,
            port: 0,
            dma_buffer_size: 0,
            fifo_reading_buffer: 0,
            min_packet:1
        }
    }
}

fn import_args_as_configuration() -> Configuration{
    let cli = Cli::parse();
    let mut conf: Configuration = Configuration::new();

    if let Some(bit_file) = cli.bit_file {
        conf.bit_file = bit_file.display().to_string();
        println!("bit file: {}", conf.bit_file);
    } else {
        println!("THe BIT FILE IS NEEDED!");
        process::exit(0x0);
    }

    if let Some(ni_address) = cli.ni_address {
        conf.ni_address = ni_address;
        println!("ni_address: {}", conf.ni_address);
    }

    if let Some(signature) = cli.signature {
        conf.signature = signature;
        println!("signature: {}", conf.signature);
    }

    if let Some(run) = cli.run {
        conf.run = run;
        println!("Run: {}", conf.run);
    }

    if let Some(close_on_reset) = cli.close_on_reset {
        conf.close_on_reset = close_on_reset;
        println!("close_on_reset: {}", conf.close_on_reset);
    }


    if let Some(fifo) = cli.fifo {
        conf.fifo = fifo;
        println!("fifo: {}", conf.fifo);
    }

    if let Some(port) = cli.port {
        conf.port = port;
        println!("port: {}", conf.port);
    }

    if let Some(dma_buffer_size) = cli.dma_buffer_size {
        conf.dma_buffer_size = dma_buffer_size;
        println!("dma_buffer_size: {}", conf.dma_buffer_size);
    }

    if let Some(fifo_reading_buffer) = cli.fifo_reading_buffer {
        conf.fifo_reading_buffer = fifo_reading_buffer;
        println!("fifo_reading_buffer: {}", conf.fifo_reading_buffer);
    }

    if let Some(min_packet) = cli.min_packet {
        conf.min_packet = min_packet;
        println!("min_packet: {}", conf.min_packet);
    }

    conf
}

fn zmq_loop(mut conf: &Configuration, rx: crossbeam::channel::Receiver<Vec<u64>>) {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::PUSH).unwrap();
    let tcp_string = "tcp://127.0.0.1:".to_string() + &conf.port.to_string();
    println!("Connected {}", tcp_string);
    socket.connect(&tcp_string).unwrap();
    println!("Connected to {}!",tcp_string);
    println!("zmq_loop started");
    loop {
        // println!("rx.recv()");
        match rx.recv() {
            Ok(chunk) => {
                let (head, chunk_u8, tail) = unsafe { chunk.align_to::<u8>() };
                assert!(head.is_empty()); //paranoid check that the previous call was not messing up
                assert!(tail.is_empty());


                if chunk.len()>0 {
                    match socket.send(chunk_u8, 0) {
                        Ok(T) => {
                            //println!("tx: {}", chunk.len());
                        }
                        Err(E) => {
                            println!("ERR {:?}", E);
                        }
                    }
                }
            }
            Err(E) => {
                println!("zmq_loop () {:?}",E);
                // The sender channel was closed, exit the loop
                process::exit(0x0100);
            },
        }
    }
}

fn fpga_loop(mut conf: &Configuration, tx: crossbeam::channel::Sender<Vec<u64>>) -> Result<(), NifpgaError> {

    let session = Session::open(
        conf.bit_file.as_str(),
        conf.signature.as_str(),//signature from generated header
        conf.ni_address.as_str(),
        conf.run, //run on open
        conf.close_on_reset //close_on_reset on close
    )?;

    let (reader, depth) = session.open_read_fifo::<u64>(conf.fifo, conf.dma_buffer_size)?;

    println!("Actual DMA FIFO  {} set depth: {} actual depth: {}", conf.fifo, conf.dma_buffer_size, depth);
    println!("conf.fifo_reading_buffer: {}", conf.fifo_reading_buffer);

    let mut read_buff:Vec<u64> = Vec::with_capacity(conf.fifo_reading_buffer);
    read_buff.resize(conf.fifo_reading_buffer, 0);

    let mut read_buff_zero_size:Vec<u64> = Vec::with_capacity(0);
    let mut data_available=0;

    let mut now_time = Instant::now();

    loop {
        let last_time = Instant::now();
        if data_available==0 {
            data_available = (reader.read(&mut read_buff_zero_size, 0)? / conf.min_packet)*conf.min_packet;
        }

        if data_available>0 {
            //println!("f:{}", data_available);
            read_buff.resize(data_available, 0);
            data_available = (reader.read(&mut read_buff, 10000)? / conf.min_packet)*conf.min_packet ;
            tx.send(read_buff.to_vec()).unwrap();
        }
        if data_available>0 {
            //println!("r {:?}", data_available);
        }
        now_time = Instant::now();
        let delta_time
            = now_time - last_time;

    }

    Ok(())
}




fn main() -> Result<(), NifpgaError>{
    let mut conf = import_args_as_configuration();
    let mut conf2 = conf.clone();
    // const channel_buffer_size = 10000;

    let (tx,rx) = crossbeam::channel::unbounded();

    println!("Start two threads.");

    crossbeam::thread::scope( |s| {
        let t1 = s.spawn(move |_| zmq_loop(&conf2.clone(),rx));
        let t2 = s.spawn(move |_| fpga_loop(&conf,tx));
    }).unwrap();

    println!("End two threads.");

    Ok(())
}



