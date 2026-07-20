//! A small, honest timer: warm up, then measure each operation individually and
//! report the median with spread (median absolute deviation)
use std::time::Instant;

pub struct Timing {
    pub median_us: f64,
    pub spread_us: f64,
}

pub fn time(iterations: usize, mut op: impl FnMut(usize)) -> Timing {
    let warmup = (iterations / 10).max(5);
    let mut call = 0;

    for _ in 0..warmup {
        op(call);
        call += 1;
    }

    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        op(call);
        call += 1;
        samples.push(start.elapsed().as_nanos() as f64 / 1_000.0);
    }

    summarise(samples)
}

fn summarise(mut samples: Vec<f64>) -> Timing {
    assert!(!samples.is_empty(), "a timing needs at least one sample");
    samples.sort_by(|a, b| a.partial_cmp(b).expect("timings are never NaN"));

    let median = median_of_sorted(&samples);

    let mut deviations: Vec<_> = samples.iter().map(|s| (s - median).abs()).collect();
    deviations.sort_by(|a, b| a.partial_cmp(b).expect("deviations are never NaN"));

    Timing {
        median_us: median,
        spread_us: median_of_sorted(&deviations),
    }
}

fn median_of_sorted(sorted: &[f64]) -> f64 {
    let mid = sorted.len() / 2;
    match sorted.len() % 2 {
        0 => (sorted[mid - 1] + sorted[mid]) / 2.0,
        _ => sorted[mid],
    }
}
