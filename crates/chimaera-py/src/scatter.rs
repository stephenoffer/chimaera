//! The shuffle's scatter loop (`_shuffle._split`) for its common combines:
//! split (key, value) pairs into a dict per partition they touch, folding
//! each key's values as they arrive; a partition no pair goes to gets none
//! (a scatter into a million partitions may touch a few). It makes the same
//! dicts as the Python loop, and a key goes to the bucket
//! `_shuffle.bucket_of` gives it, bit for bit: `int`, `str` and `float` keys
//! are hashed here as it hashes them, and any other key is handed to it.
//!
//! The combine is a code: 0 makes each value list (no combine), 1 is
//! `operator.add`, 2 `operator.mul`, 3 `max` and 4 `min`, each with the
//! semantics of the Python call it stands for.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

const GROUP: u8 = 0;
const ADD: u8 = 1;
const MUL: u8 = 2;
const MAX: u8 = 3;
const MIN: u8 = 4;
/// Pairs between letting other threads run and checking for a signal.
const YIELD_EVERY: usize = 1024;
/// Up to this many partitions, buckets are found by number in a vector (8
/// bytes a partition); past it, in a map of the ones touched. The map's
/// lookup cost the integer benchmark (64 partitions) 9% and 4,096 partitions 30%.
const DENSE_MAX: usize = 1 << 16;

/// Hashes a bucket number with one multiply (Fibonacci hashing): the map of
/// touched buckets is looked up once per pair.
#[derive(Default)]
struct Spread(u64);

impl Hasher for Spread {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(b)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    fn write_usize(&mut self, n: usize) {
        self.0 = (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// The buckets made so far, by number.
enum Buckets<'py> {
    Dense(Vec<Option<Bound<'py, PyDict>>>),
    Sparse(HashMap<usize, Bound<'py, PyDict>, BuildHasherDefault<Spread>>),
}

impl<'py> Buckets<'py> {
    fn new(partitions: usize) -> Self {
        if partitions <= DENSE_MAX {
            Buckets::Dense((0..partitions).map(|_| None).collect())
        } else {
            Buckets::Sparse(HashMap::default())
        }
    }

    /// Bucket `at`, made empty the first time.
    fn get(&mut self, py: Python<'py>, at: usize) -> &Bound<'py, PyDict> {
        match self {
            Buckets::Dense(v) => v[at].get_or_insert_with(|| PyDict::new(py)),
            Buckets::Sparse(m) => m.entry(at).or_insert_with(|| PyDict::new(py)),
        }
    }

    fn into_dict(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        match self {
            Buckets::Dense(v) => {
                for (at, bucket) in v.into_iter().enumerate() {
                    if let Some(bucket) = bucket {
                        out.set_item(at, bucket)?;
                    }
                }
            }
            Buckets::Sparse(m) => {
                for (at, bucket) in m {
                    out.set_item(at, bucket)?;
                }
            }
        }
        Ok(out)
    }
}

/// `_shuffle._split(pairs, partitions, combine)` for the combine coded `op`:
/// bucket number to bucket, for the buckets some pair went to.
#[pyfunction]
fn scatter<'py>(
    py: Python<'py>,
    pairs: &Bound<'py, PyAny>,
    partitions: usize,
    op: u8,
    bucket_of: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    if partitions == 0 || op > MIN {
        return Err(PyValueError::new_err(format!("scatter: {partitions} partitions, combine code {op}")));
    }
    let mut buckets = Buckets::new(partitions);
    for (n, item) in pairs.try_iter()?.enumerate() {
        if n % YIELD_EVERY == YIELD_EVERY - 1 {
            // What the interpreter does between Python steps: let other
            // threads have the GIL (the executor's reader, which delivers a
            // cancel), then run signal handlers (a cancel's SIGUSR2 raises
            // ch.Cancelled here).
            py.detach(|| ());
            py.check_signals()?;
        }
        let (key, value) = unpack(&item?)?;
        let at = bucket(&key, partitions, bucket_of)?;
        if at >= partitions {
            return Err(PyValueError::new_err(format!("bucket {at} of {partitions}")));
        }
        let bucket = buckets.get(py, at);
        let old = bucket.get_item(&key)?;
        match (op, old) {
            (GROUP, Some(values)) => values.cast_into::<PyList>()?.append(value)?,
            (GROUP, None) => bucket.set_item(&key, PyList::new(py, [value])?)?,
            (_, None) => bucket.set_item(&key, value)?,
            (ADD, Some(old)) => bucket.set_item(&key, old.add(value)?)?,
            (MUL, Some(old)) => bucket.set_item(&key, old.mul(value)?)?,
            // max(old, new) and min(old, new) keep the first unless the new one is past it.
            (MAX, Some(old)) => {
                if value.gt(&old)? {
                    bucket.set_item(&key, value)?
                }
            }
            // MIN, the last code (checked above).
            (_, Some(old)) => {
                if value.lt(&old)? {
                    bucket.set_item(&key, value)?
                }
            }
        }
    }
    buckets.into_dict(py)
}

/// `key, value = item`.
fn unpack<'py>(item: &Bound<'py, PyAny>) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
    if let Ok(t) = item.cast_exact::<PyTuple>()
        && t.len() == 2
    {
        return Ok((t.get_item(0)?, t.get_item(1)?));
    }
    let short = |got: u8| PyValueError::new_err(format!("not enough values to unpack (expected 2, got {got})"));
    let mut it = item.try_iter()?;
    let Some(key) = it.next().transpose()? else { return Err(short(0)) };
    let Some(value) = it.next().transpose()? else { return Err(short(1)) };
    if it.next().transpose()?.is_some() {
        return Err(PyValueError::new_err("too many values to unpack (expected 2)"));
    }
    Ok((key, value))
}

/// `_shuffle.bucket_of(key, partitions)`.
fn bucket(key: &Bound<'_, PyAny>, partitions: usize, bucket_of: &Bound<'_, PyAny>) -> PyResult<usize> {
    if key.is_exact_instance_of::<PyInt>() {
        return Ok(key.hash()?.rem_euclid(partitions as isize) as usize);
    }
    if key.is_exact_instance_of::<PyFloat>() {
        let nan = key.cast_exact::<PyFloat>()?.value().is_nan();
        return Ok(if nan { 0 } else { key.hash()?.rem_euclid(partitions as isize) as usize });
    }
    // A str with lone surrogates does not encode: bucket_of raises as Python does.
    if let Ok(s) = key.cast_exact::<PyString>()
        && let Ok(text) = s.to_str()
    {
        let digest = blake3::hash(text.as_bytes());
        let head = u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("8 bytes"));
        return Ok((head % partitions as u64) as usize);
    }
    bucket_of.call1((key, partitions))?.extract()
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scatter, m)?)
}
