fn main() {
    // Python symbols resolve at load time (needed on macOS when built by cargo).
    pyo3_build_config::add_extension_module_link_args();
}
