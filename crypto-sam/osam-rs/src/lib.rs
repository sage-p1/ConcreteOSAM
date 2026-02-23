use oram::{
    path_oram::{
        DEFAULT_BLOCKS_PER_BUCKET, DEFAULT_POSITIONS_PER_BLOCK, DEFAULT_STASH_OVERFLOW_SIZE,
    },
    Address, BlockSize, BlockValue, BucketSize, PathOram, RecursionCutoff, StashSize,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rand::rngs::OsRng;
use std::sync::Mutex;

const BLOCK_SIZE: BlockSize = 64;
const DB_SIZE: Address = 262144;
const BUCKET_SIZE: BucketSize = DEFAULT_BLOCKS_PER_BUCKET;
const POSITIONS_PER_BLOCK: BlockSize = DEFAULT_POSITIONS_PER_BLOCK;
const INITIAL_STASH_OVERFLOW_SIZE: StashSize = DEFAULT_STASH_OVERFLOW_SIZE;
const RECURSION_CUTOFF: RecursionCutoff = u64::MAX;

type OsamOram = PathOram<BlockValue<BLOCK_SIZE>, BUCKET_SIZE, POSITIONS_PER_BLOCK>;

#[pyclass(name = "RustBackend")]
struct RustStorageServer {
    oram: Mutex<OsamOram>,
    eviction_counter: Mutex<u64>,
}

#[pymethods]
impl RustStorageServer {
    #[new]
    fn new() -> PyResult<Self> {
        let mut rng = OsRng;
        let oram_instance = PathOram::new_with_parameters(
            DB_SIZE,
            &mut rng,
            INITIAL_STASH_OVERFLOW_SIZE,
            RECURSION_CUTOFF,
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        Ok(RustStorageServer {
            oram: Mutex::new(oram_instance),
            eviction_counter: Mutex::new(0),
        })
    }

    fn write(&self, address: Address, value: Vec<u8>) -> PyResult<()> {
        let mut oram = self.oram.lock().unwrap();
        let mut counter = self.eviction_counter.lock().unwrap();
        let mut rng = OsRng;

        // Standard block preparation
        let mut block_value_bytes = [0u8; BLOCK_SIZE as usize];
        if value.len() <= BLOCK_SIZE as usize {
            block_value_bytes[..value.len()].copy_from_slice(&value);
        }
        let block_value = BlockValue::new(block_value_bytes);

        // Calculate deterministic path
        let height = oram.height();
        let num_leaves = 1 << height;
        let rev_idx = (*counter).reverse_bits() >> (64 - height);
        let leaf_id = (1 << height) + rev_idx;

        // Use leaf_id for both assignment and eviction
        let result = oram.deterministic_access(address, |_| block_value, leaf_id, &mut rng);

        *counter = (*counter + 1) % num_leaves;

        result
            .map(|_| ())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        address: Address,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut oram = self.oram.lock().unwrap();
        let mut counter = self.eviction_counter.lock().unwrap();
        let mut rng = OsRng;

        let height = oram.height();
        let num_leaves = 1 << height;
        let rev_idx = (*counter).reverse_bits() >> (64 - height);
        let leaf_id = (1 << height) + rev_idx;

        let block = oram
            .deterministic_access(address, |x| *x, leaf_id, &mut rng)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        *counter = (*counter + 1) % num_leaves;

        if block.data.iter().any(|&x| x != 0) {
            Ok(Some(PyBytes::new_bound(py, &block.data)))
        } else {
            Ok(None)
        }
    }
}

#[pymodule]
fn osam_rust_backend(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustStorageServer>()?;
    Ok(())
}
