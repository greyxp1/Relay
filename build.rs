fn main() {
    // Native libraries follow the static archives that use them for --as-needed linking.
    for (source, library, version) in [
        ("src/share_audio.c", "libpipewire-0.3", "0.3"),
        ("src/h264.c", "gstreamer-codecparsers-1.0", "1.22"),
    ] {
        println!("cargo:rerun-if-changed={source}");
        let native = pkg_config::Config::new()
            .cargo_metadata(false)
            .atleast_version(version)
            .probe(library)
            .unwrap_or_else(|_| panic!("{library} >= {version} development files are required"));
        cc::Build::new()
            .opt_level(2)
            .includes(native.include_paths)
            .file(source)
            .compile(library);
        pkg_config::probe_library(library).unwrap();
    }
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
