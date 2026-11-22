use std::path::PathBuf;

fn main() {
    let parsers = pkg_config::Config::new()
        .atleast_version("1.22")
        .probe("gstreamer-codecparsers-1.0")
        .expect("GStreamer codec parser development files >= 1.22 are required");
    let output = PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("h264.rs");
    bindgen::Builder::default()
        .header_contents(
            "h264.h",
            "#define GST_USE_UNSTABLE_API\n#include <gst/codecparsers/gsth264bitwriter.h>",
        )
        .allowlist_function("gst_h264_(parse_sps|bit_writer_sps|bit_writer_convert_to_nal)")
        .prepend_enum_name(false)
        .clang_args(
            parsers
                .include_paths
                .iter()
                .map(|path| format!("-I{}", path.display())),
        )
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Could not generate GStreamer H.264 bindings")
        .write_to_file(output)
        .unwrap();

    println!("cargo:rerun-if-changed=src/dave_identity.cpp");
    println!("cargo:rerun-if-env-changed=RELAY_DAVE_DIR");
    let mut dave = cc::Build::new();
    if let Some(directory) = std::env::var_os("RELAY_DAVE_DIR") {
        dave.include(std::path::Path::new(&directory).join("include"));
    }
    dave.cpp(true)
        .std("c++17")
        .opt_level(2)
        .file("src/dave_identity.cpp")
        .compile("relay_dave_identity");
}
