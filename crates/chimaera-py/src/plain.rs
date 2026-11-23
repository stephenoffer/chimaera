//! A codec for plain data: dicts, lists and tuples of `str`, `bytes`, `int`,
//! `float`, `bool` and `None`, with no container reachable twice. Encoding
//! checks and writes in one pass. Anything else is declined, and the caller
//! pickles the value instead: an `int` past 64 bits, a `str` with lone
//! surrogates, a shared or very deep container, a subclass or any other type.
//! On such data it skips what makes pickle slow there: the memo (or the
//! separate scan that proves the memo unneeded) and opcode dispatch.
//!
//! Format, version 1, little endian: a version byte, then one value. Each
//! value is a tag byte, then: an `int` as i8, i32 or i64 (the smallest that
//! holds it), a `float` as f64 bits, a `str` (UTF-8) as a u8 length (under
//! 256 bytes) or u32 length and the bytes, `bytes` as a u32 length and the
//! bytes, a list or tuple as a u32 count and its items, a dict as a u32 count
//! and its key, value pairs.
//!
//! Version 2 (`plain_encode_column`) is a list of such values with no
//! containers, where a `str` object met again is written as a u32 index into
//! the strings so far: reading gives back one object, as pickle's memo would.
//! A pandas column of repeated strings relies on that for its memory.

use pyo3::buffer::PyUntypedBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const VERSION: u8 = 1;
const VERSION_SHARED: u8 = 2;
const NONE: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const INT: u8 = 3;
const FLOAT: u8 = 4;
const STR: u8 = 5;
const BYTES: u8 = 6;
const LIST: u8 = 7;
const TUPLE: u8 = 8;
const DICT: u8 = 9;
const INT8: u8 = 10;
const INT32: u8 = 11;
const STR8: u8 = 12;
const STRREF: u8 = 13;

/// Deeper nesting is declined (encoding) or refused (decoding).
const MAX_DEPTH: usize = 200;
/// Containers tracked before declining (memory for the seen set).
const MAX_CONTAINERS: usize = 1 << 22;

struct Types {
    none: *mut ffi::PyObject,
    t_true: *mut ffi::PyObject,
    dict: *mut ffi::PyTypeObject,
    list: *mut ffi::PyTypeObject,
    tuple: *mut ffi::PyTypeObject,
    str: *mut ffi::PyTypeObject,
    int: *mut ffi::PyTypeObject,
    float: *mut ffi::PyTypeObject,
    bytes: *mut ffi::PyTypeObject,
    bool: *mut ffi::PyTypeObject,
}

impl Types {
    fn get() -> Types {
        // SAFETY: addresses of the interpreter's static type objects and singletons.
        unsafe {
            Types {
                none: ffi::Py_None(),
                t_true: ffi::Py_True(),
                dict: &raw mut ffi::PyDict_Type,
                list: &raw mut ffi::PyList_Type,
                tuple: &raw mut ffi::PyTuple_Type,
                str: &raw mut ffi::PyUnicode_Type,
                int: &raw mut ffi::PyLong_Type,
                float: &raw mut ffi::PyFloat_Type,
                bytes: &raw mut ffi::PyBytes_Type,
                bool: &raw mut ffi::PyBool_Type,
            }
        }
    }
}

struct Encoder {
    out: Vec<u8>,
    seen: std::collections::HashSet<usize>,
    ty: Types,
    /// Column mode: each str object's index, by address, and no containers
    /// below the top-level list.
    strings: Option<std::collections::HashMap<usize, u32>>,
}

impl Encoder {
    /// Append `o`; false when it is not plain data (a Python error raised on
    /// the way is cleared).
    ///
    /// SAFETY (for this and the functions it calls): the GIL is held and `o`
    /// is a live object. No Python code runs while encoding (exact built-in
    /// types only, no hashing or comparison), so the borrowed references from
    /// PyDict_Next and the list and tuple accessors stay valid, and no
    /// container changes under the walk.
    unsafe fn value(&mut self, o: *mut ffi::PyObject, depth: usize) -> bool {
        // SAFETY: `o` is live (above); reading its type pointer touches nothing else.
        let t = unsafe { ffi::Py_TYPE(o) };
        let ty = &self.ty;
        if t == ty.dict || t == ty.list || t == ty.tuple {
            if self.strings.is_some() && depth > 0 {
                return false; // a column holds atoms only
            }
            // SAFETY: as for this function.
            return unsafe { self.container(o, t, depth) };
        }
        // SAFETY: as for this function.
        unsafe { self.atom(o, t) }
    }

    /// A `None`, `str`, `float`, `int`, `bool` or `bytes` of exactly that type.
    ///
    /// # Safety
    /// As for `value`: the GIL is held, `o` is live, and `t` is its type.
    unsafe fn atom(&mut self, o: *mut ffi::PyObject, t: *mut ffi::PyTypeObject) -> bool {
        let ty = &self.ty;
        if o == ty.none {
            self.out.push(NONE);
        } else if t == ty.str {
            if let Some(strings) = self.strings.as_mut() {
                let next = strings.len() as u32;
                if let Some(&i) = strings.get(&(o as usize)) {
                    self.out.push(STRREF);
                    self.out.extend_from_slice(&i.to_le_bytes());
                    return true;
                }
                strings.insert(o as usize, next);
            }
            let mut n: ffi::Py_ssize_t = 0;
            // SAFETY: `o` is an exact str. Its UTF-8 is its own data when ASCII;
            // another str keeps the UTF-8 made here (CPython's cache).
            let p = unsafe { ffi::PyUnicode_AsUTF8AndSize(o, &mut n) };
            if p.is_null() {
                // SAFETY: the error PyUnicode_AsUTF8AndSize just set (lone surrogates).
                unsafe { ffi::PyErr_Clear() };
                return false; // pickled instead
            }
            return match u8::try_from(n) {
                Ok(short) => {
                    self.out.extend_from_slice(&[STR8, short]);
                    // SAFETY: `p` points at the `n` bytes of the str's UTF-8, alive with `o`.
                    self.out.extend_from_slice(unsafe { std::slice::from_raw_parts(p.cast::<u8>(), n as usize) });
                    true
                }
                Err(_) => self.sized(STR, p.cast(), n),
            };
        } else if t == ty.float {
            // SAFETY: `o` is an exact float; reading it cannot fail or run code.
            let v = unsafe { ffi::PyFloat_AsDouble(o) };
            self.out.push(FLOAT);
            self.out.extend_from_slice(&v.to_bits().to_le_bytes());
        } else if t == ty.int {
            // SAFETY: `o` is an exact int; an overflow sets an error, checked next.
            let v = unsafe { ffi::PyLong_AsLongLong(o) };
            // SAFETY: reads (and then clears) only this thread's error indicator.
            if v == -1 && unsafe { !ffi::PyErr_Occurred().is_null() } {
                // SAFETY: clears the overflow error PyLong_AsLongLong just set.
                unsafe { ffi::PyErr_Clear() };
                return false; // past 64 bits: pickled instead
            }
            self.int(v);
        } else if t == ty.bool {
            self.out.push(if o == ty.t_true { TRUE } else { FALSE });
        } else if t == ty.bytes {
            // SAFETY: `o` is an exact bytes object; its buffer lives with it.
            let (p, n) = unsafe { (ffi::PyBytes_AsString(o), ffi::PyBytes_Size(o)) };
            return self.sized(BYTES, p.cast(), n);
        } else {
            return false; // any other type, or a subclass: pickled instead
        }
        true
    }

    /// An exact dict, list or tuple, reachable once, not too deep.
    ///
    /// # Safety
    /// As for `value`: the GIL is held, `o` is live, and `t` (dict, list or
    /// tuple) is its type.
    unsafe fn container(&mut self, o: *mut ffi::PyObject, t: *mut ffi::PyTypeObject, depth: usize) -> bool {
        if depth >= MAX_DEPTH || !self.seen.insert(o as usize) || self.seen.len() > MAX_CONTAINERS {
            return false; // too deep, or reachable twice (a memo would keep that)
        }
        if t == self.ty.dict {
            // SAFETY: `o` is an exact dict that cannot change during the walk.
            if !self.count(DICT, unsafe { ffi::PyDict_Size(o) }) {
                return false;
            }
            let (mut pos, mut k, mut v) = (0, std::ptr::null_mut(), std::ptr::null_mut());
            // SAFETY: iterating that dict; `k` and `v` are borrowed while it lives.
            while unsafe { ffi::PyDict_Next(o, &mut pos, &mut k, &mut v) } != 0 {
                // SAFETY: live entries of the dict, under the same conditions.
                if !unsafe { self.value(k, depth + 1) && self.value(v, depth + 1) } {
                    return false;
                }
            }
            return true;
        }
        let is_list = t == self.ty.list;
        // SAFETY: `o` is an exact list or tuple that cannot change during the walk.
        let n = unsafe { if is_list { ffi::PyList_Size(o) } else { ffi::PyTuple_Size(o) } };
        if !self.count(if is_list { LIST } else { TUPLE }, n) {
            return false;
        }
        (0..n).all(|i| {
            // SAFETY: `i` is in bounds; the item is borrowed while the sequence lives.
            let item = unsafe { if is_list { ffi::PyList_GetItem(o, i) } else { ffi::PyTuple_GetItem(o, i) } };
            // SAFETY: a live item, under the same conditions.
            unsafe { self.value(item, depth + 1) }
        })
    }

    /// An `int` in the fewest bytes that hold it.
    fn int(&mut self, v: i64) {
        if let Ok(b) = i8::try_from(v) {
            self.out.extend_from_slice(&[INT8, b as u8]);
        } else if let Ok(w) = i32::try_from(v) {
            self.out.push(INT32);
            self.out.extend_from_slice(&w.to_le_bytes());
        } else {
            self.out.push(INT);
            self.out.extend_from_slice(&v.to_le_bytes());
        }
    }

    fn sized(&mut self, tag: u8, p: *const u8, n: ffi::Py_ssize_t) -> bool {
        let Ok(len) = u32::try_from(n) else { return false };
        self.out.push(tag);
        self.out.extend_from_slice(&len.to_le_bytes());
        // SAFETY: `p` points at the `n` bytes of a live str's UTF-8 or bytes' data.
        self.out.extend_from_slice(unsafe { std::slice::from_raw_parts(p, len as usize) });
        true
    }

    fn count(&mut self, tag: u8, n: ffi::Py_ssize_t) -> bool {
        let Ok(n) = u32::try_from(n) else { return false };
        self.out.push(tag);
        self.out.extend_from_slice(&n.to_le_bytes());
        true
    }
}

/// `obj` encoded (see the module), or None when it is not plain data.
#[pyfunction]
pub fn plain_encode<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> Option<Bound<'py, PyBytes>> {
    encode(py, obj, None)
}

/// `items` (an exact list of `str`, `bytes`, `int`, `float`, `bool` and
/// `None`) encoded with each repeated str object written once (version 2),
/// or None when it holds anything else.
#[pyfunction]
pub fn plain_encode_column<'py>(py: Python<'py>, items: &Bound<'py, PyAny>) -> Option<Bound<'py, PyBytes>> {
    if !items.is_exact_instance_of::<pyo3::types::PyList>() {
        return None;
    }
    encode(py, items, Some(std::collections::HashMap::new()))
}

fn encode<'py>(
    py: Python<'py>,
    obj: &Bound<'py, PyAny>,
    strings: Option<std::collections::HashMap<usize, u32>>,
) -> Option<Bound<'py, PyBytes>> {
    let version = if strings.is_some() { VERSION_SHARED } else { VERSION };
    let mut enc =
        Encoder { out: Vec::with_capacity(1 << 16), seen: std::collections::HashSet::new(), ty: Types::get(), strings };
    enc.out.push(version);
    // SAFETY: the GIL is held (`py`), and `obj` is alive for the call.
    if !unsafe { enc.value(obj.as_ptr(), 0) } {
        return None;
    }
    Some(PyBytes::new(py, &enc.out))
}

struct Decoder<'a, 'py> {
    b: &'a [u8],
    at: usize,
    /// Version 2: every str decoded so far, for STRREF. Borrowed: each one
    /// is owned by a slot of the top-level list, which outlives the decode
    /// (a column holds no containers, so no str is ever dropped midway).
    strings: Option<Vec<*mut ffi::PyObject>>,
    py: std::marker::PhantomData<Bound<'py, PyAny>>,
}

fn corrupt(what: &str) -> PyErr {
    PyValueError::new_err(format!("corrupt plain-data value: {what}"))
}

impl<'py> Decoder<'_, 'py> {
    fn take(&mut self, n: usize) -> PyResult<&[u8]> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.b.len()).ok_or_else(|| corrupt("it ends early"))?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }

    fn u32(&mut self) -> PyResult<usize> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")) as usize)
    }

    fn u64(&mut self) -> PyResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    /// A count of items each at least `min` bytes long, checked against
    /// what is left, so corrupt input cannot ask for a huge allocation.
    fn items(&mut self, min: usize) -> PyResult<usize> {
        let n = self.u32()?;
        if n.saturating_mul(min) > self.b.len() - self.at {
            return Err(corrupt("a count is larger than the data"));
        }
        Ok(n)
    }

    fn value(&mut self, py: Python<'py>, depth: usize) -> PyResult<Bound<'py, PyAny>> {
        if depth > MAX_DEPTH {
            return Err(corrupt("it nests too deep"));
        }
        let tag = self.take(1)?[0];
        if self.strings.is_some() {
            // A column (version 2) is a list of atoms, and nothing else.
            if depth == 0 && tag != LIST {
                return Err(corrupt("a column that is not a list"));
            }
            if depth > 0 && matches!(tag, LIST | TUPLE | DICT) {
                return Err(corrupt("a container inside a column"));
            }
        }
        // SAFETY (each block): C-API constructors called with the GIL held;
        // every returned pointer is a new reference (or NULL with an error
        // set), owned at once by a Bound. The SetItem calls steal or copy
        // references as documented, into containers created right here.
        Ok(match tag {
            NONE => py.None().into_bound(py),
            FALSE => false.into_pyobject(py)?.to_owned().into_any(),
            TRUE => true.into_pyobject(py)?.to_owned().into_any(),
            INT | INT8 | INT32 => {
                let v = match tag {
                    INT8 => self.take(1)?[0] as i8 as i64,
                    INT32 => i32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")) as i64,
                    _ => self.u64()? as i64,
                };
                unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyLong_FromLongLong(v))? }
            }
            FLOAT => {
                let v = f64::from_bits(self.u64()?);
                unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyFloat_FromDouble(v))? }
            }
            STR | STR8 => {
                let n = if tag == STR8 { self.take(1)?[0] as usize } else { self.u32()? };
                let s = self.take(n)?;
                let v = unsafe {
                    Bound::from_owned_ptr_or_err(
                        py,
                        ffi::PyUnicode_DecodeUTF8(s.as_ptr().cast(), n as ffi::Py_ssize_t, c"strict".as_ptr()),
                    )?
                };
                if let Some(strings) = self.strings.as_mut() {
                    strings.push(v.as_ptr());
                }
                v
            }
            STRREF => {
                let i = self.u32()?;
                let strings = self.strings.as_ref().ok_or_else(|| corrupt("a string reference in version 1"))?;
                let p = *strings.get(i).ok_or_else(|| corrupt("a reference to a string not yet read"))?;
                // SAFETY: `p` is a str owned by the top-level list (see `strings`);
                // this takes a new reference to it.
                unsafe { Bound::from_borrowed_ptr(py, p) }
            }
            BYTES => {
                let n = self.u32()?;
                let s = self.take(n)?;
                unsafe {
                    Bound::from_owned_ptr_or_err(
                        py,
                        ffi::PyBytes_FromStringAndSize(s.as_ptr().cast(), n as ffi::Py_ssize_t),
                    )?
                }
            }
            LIST | TUPLE => {
                let n = self.items(1)?;
                let (new, set): (unsafe extern "C" fn(ffi::Py_ssize_t) -> *mut ffi::PyObject, _) = if tag == LIST {
                    (ffi::PyList_New, ffi::PyList_SetItem as unsafe extern "C" fn(_, _, _) -> _)
                } else {
                    (ffi::PyTuple_New, ffi::PyTuple_SetItem as unsafe extern "C" fn(_, _, _) -> _)
                };
                let seq = unsafe { Bound::from_owned_ptr_or_err(py, new(n as ffi::Py_ssize_t))? };
                for i in 0..n {
                    let item = self.value(py, depth + 1)?;
                    // Steals `item`; a slot left empty by an error is freed with the sequence.
                    if unsafe { set(seq.as_ptr(), i as ffi::Py_ssize_t, item.into_ptr()) } != 0 {
                        return Err(PyErr::fetch(py));
                    }
                }
                seq
            }
            DICT => {
                let n = self.items(2)?;
                let d = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyDict_New())? };
                for _ in 0..n {
                    let (k, v) = (self.value(py, depth + 1)?, self.value(py, depth + 1)?);
                    if unsafe { ffi::PyDict_SetItem(d.as_ptr(), k.as_ptr(), v.as_ptr()) } != 0 {
                        return Err(PyErr::fetch(py)); // e.g. an unhashable key in corrupt input
                    }
                }
                d
            }
            _ => return Err(corrupt("an unknown tag")),
        })
    }
}

/// The value `buf` (bytes or any contiguous buffer) encodes (see `plain_encode`).
#[pyfunction]
pub fn plain_decode<'py>(py: Python<'py>, buf: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let view = PyUntypedBuffer::get(buf)?;
    if !view.is_c_contiguous() {
        return Err(PyValueError::new_err("plain-data values are read from contiguous buffers"));
    }
    // SAFETY: `view` keeps the exported buffer alive and unmoved while `b` is used.
    let b = unsafe { std::slice::from_raw_parts(view.buf_ptr().cast::<u8>(), view.len_bytes()) };
    let mut dec = Decoder { b, at: 0, strings: None, py: std::marker::PhantomData };
    match dec.take(1)?[0] {
        VERSION => {}
        VERSION_SHARED => dec.strings = Some(Vec::new()),
        _ => return Err(corrupt("an unknown version")),
    }
    let v = dec.value(py, 0)?;
    if dec.at != b.len() {
        return Err(corrupt("bytes follow the value"));
    }
    Ok(v)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(plain_encode, m)?)?;
    m.add_function(wrap_pyfunction!(plain_encode_column, m)?)?;
    m.add_function(wrap_pyfunction!(plain_decode, m)?)?;
    Ok(())
}
