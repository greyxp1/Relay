fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rerun-if-changed=src/share_audio.c");
        let pipewire = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe("libpipewire-0.3")
            .expect("PipeWire development files are required");
        cc::Build::new()
            .opt_level(2)
            .includes(pipewire.include_paths)
            .file("src/share_audio.c")
            .compile("relay_share_audio");
        // PipeWire must follow the static audio archive for --as-needed linking.
        pkg_config::probe_library("libpipewire-0.3")
            .expect("PipeWire development files are required");
    }
    println!("cargo:rerun-if-changed=src/h264.c");
    let parser = pkg_config::Config::new()
        .cargo_metadata(false)
        .atleast_version("1.22")
        .probe("gstreamer-codecparsers-1.0")
        .expect("GStreamer codecparsers >= 1.22 is required");
    cc::Build::new()
        .opt_level(2)
        .includes(parser.include_paths)
        .file("src/h264.c")
        .compile("relay_h264");
    pkg_config::probe_library("gstreamer-codecparsers-1.0")
        .expect("GStreamer codecparsers is required");
    println!("cargo:rerun-if-changed=src/dave_identity.cpp");
    println!("cargo:rerun-if-env-changed=RELAY_DAVE_DIR");
    let mut build = cc::Build::new();
    if let Some(directory) = std::env::var_os("RELAY_DAVE_DIR") {
        build.include(std::path::PathBuf::from(directory).join("include"));
    }
    build
        .cpp(true)
        .std("c++17")
        .opt_level(2)
        .file("src/dave_identity.cpp")
        .compile("relay_dave_identity");
}
