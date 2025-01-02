// Imene Khebouri 500460 - extra assignment 15
// Results are saved in results.txt file
// Run using  cargo run -- ./measurements_sample.txt
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write}; 
use memmap2::Mmap;
use std::sync::Arc;
use std::env;
use rayon::prelude::*;
use std::time::Instant;
use regex::Regex;

fn main() -> io::Result<()> {
    let start = Instant::now();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <file_path>", args[0]);
        std::process::exit(1);
    }
    let file_path = &args[1];

    let file = File::open(file_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mmap = Arc::new(mmap);

    let num_chunks = num_cpus::get();
    let chunk_size = mmap.len() / num_chunks;

    let results: Vec<HashMap<String, Vec<f64>>> = (0..num_chunks)
        .into_par_iter()
        .map(|chunk_index| {
            let start = chunk_index * chunk_size;
            let end = if chunk_index == num_chunks - 1 {
                mmap.len()
            } else {
                adjust_chunk_boundary(&mmap, chunk_index * chunk_size, (chunk_index + 1) * chunk_size)
            };

            process_chunk(&mmap[start..end])
        })
        .collect();

    let global_data = merge_results(results);

    let stats = calculate_statistics(global_data)?;
    let duration = start.elapsed();

    save_results_to_file(&stats, "results.txt")?;
    println!("Time elapsed: {:.6} seconds", duration.as_secs_f64());

    Ok(())
}

fn adjust_chunk_boundary(mmap: &Mmap, _start: usize, end: usize) -> usize {
    let mut adjusted_end = end;
    while adjusted_end < mmap.len() && mmap[adjusted_end] != b'\n' {
        adjusted_end += 1;
    }
    adjusted_end
}

fn process_chunk(chunk: &[u8]) -> HashMap<String, Vec<f64>> {
    let content = std::str::from_utf8(chunk).unwrap_or("");
    let mut station_data: HashMap<String, Vec<f64>> = HashMap::new();

    let station_name_regex = Regex::new(r"^[^;\n]{1,100}$").unwrap();

    for line in content.lines() {
        if let Some((station, temperature)) = line.split_once(';') {
            if !station_name_regex.is_match(station) {
                eprintln!("Invalid station name: {}", station);
                continue;
            }

            if let Ok(temp) = temperature.trim().parse::<f64>() {
                if temp < -99.9 || temp > 99.9 || !temperature.trim().contains('.') {
                    eprintln!("Invalid temperature: {}", temperature);
                    continue;
                }
                station_data
                    .entry(station.to_string())
                    .or_insert_with(Vec::new)
                    .push(temp);
            }
        }
    }

    station_data
}

fn merge_results(results: Vec<HashMap<String, Vec<f64>>>) -> HashMap<String, Vec<f64>> {
    let mut global_data: HashMap<String, Vec<f64>> = HashMap::new();

    for result in results {
        for (station, temperatures) in result {
            global_data
                .entry(station)
                .or_insert_with(Vec::new)
                .extend(temperatures);
        }
    }

    global_data
}

fn calculate_statistics(global_data: HashMap<String, Vec<f64>>) -> Result<HashMap<String, (f64, f64, f64)>, io::Error> {
    if global_data.len() > 10_000 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Too many unique stations (>10,000)."));
    }

    let mut stats: HashMap<String, (f64, f64, f64)> = HashMap::new();

    for (station, mut temperatures) in global_data {
        temperatures.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let min = *temperatures.first().unwrap();
        let max = *temperatures.last().unwrap();
        let median = if temperatures.len() % 2 == 0 {
            let mid = temperatures.len() / 2;
            ((temperatures[mid - 1] + temperatures[mid]) / 2.0).round()
        } else {
            temperatures[temperatures.len() / 2].round()
        };

        stats.insert(station, (min.round(), median, max.round()));
    }

    Ok(stats)
}

fn save_results_to_file(stats: &HashMap<String, (f64, f64, f64)>, file_name: &str) -> io::Result<()> {
    let mut file = File::create(file_name)?;

    let mut sorted_stations: Vec<_> = stats.keys().collect();
    sorted_stations.sort();

    for station in sorted_stations {
        let (min, median, max) = stats[station];
        writeln!(file, "{} = {:.1}/ {:.1}/ {:.1}", station, min, median, max)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_chunk() {
        let chunk = b"station1;23.5\nstation2;19.8\nstation1;25.0\n";
        let result = process_chunk(chunk);
        assert_eq!(result["station1"], vec![23.5, 25.0]);
        assert_eq!(result["station2"], vec![19.8]);
    }

    #[test]
    fn test_merge_results() {
        let mut result1 = HashMap::new();
        result1.insert("station1".to_string(), vec![23.5, 25.0]);
        let mut result2 = HashMap::new();
        result2.insert("station1".to_string(), vec![22.0]);
        result2.insert("station2".to_string(), vec![19.8]);
        let results = vec![result1, result2];
        let merged = merge_results(results);
        assert_eq!(merged["station1"], vec![23.5, 25.0, 22.0]);
        assert_eq!(merged["station2"], vec![19.8]);
    }

    #[test]
    fn test_calculate_statistics() {
        let mut global_data = HashMap::new();
        global_data.insert("station1".to_string(), vec![23.5, 25.0, 22.0]);
        global_data.insert("station2".to_string(), vec![19.8, 20.2, 18.5]);
        let stats = calculate_statistics(global_data).unwrap();
        assert_eq!(stats["station1"], (22.0, 23.5, 25.0));
        assert_eq!(stats["station2"], (18.5, 19.8, 20.2));
    }

    #[test]
    fn test_save_results_to_file() {
        let mut stats = HashMap::new();
        stats.insert("station1".to_string(), (22.0, 23.5, 25.0));
        stats.insert("station2".to_string(), (18.5, 19.8, 20.2));
        let file_name = "test_results.txt";
        save_results_to_file(&stats, file_name).unwrap();
        let content = std::fs::read_to_string(file_name).unwrap();
        assert!(content.contains("station1 = 22.0/ 23.5/ 25.0"));
        assert!(content.contains("station2 = 18.5/ 19.8/ 20.2"));
        std::fs::remove_file(file_name).unwrap();
    }

    #[test]
    fn test_adjust_chunk_boundary() {
        let data = b"station1;23.5\nstation2;19.8\nst";
        let mmap = unsafe { Mmap::map(&File::create("test_boundary.txt").unwrap()).unwrap() };
        let adjusted = adjust_chunk_boundary(&mmap, 0, 8);
        assert_eq!(adjusted, 8); // Adjusted boundary ends on a newline
    }
}
