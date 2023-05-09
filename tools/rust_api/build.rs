use cmake::Config;
use std::env;
use std::path::Path;

enum ArrowMode {
    System,
    Path(String),
    Bundled,
}

fn arrow_mode() -> ArrowMode {
    // FIXME: Bundled mode is very slow at the moment. It seems to always run a clean build for
    // some reason
    if env::var("KUZU_USE_SYSTEM_ARROW").is_ok() {
        ArrowMode::System
    } else if let Ok(path) = env::var("ARROW_INSTALL") {
        ArrowMode::Path(path)
    } else {
        ArrowMode::Bundled
    }
}

fn link_mode() -> &'static str {
    if env::var("KUZU_SHARED").is_ok() {
        "dylib"
    } else {
        "static"
    }
}

fn main() -> Result<(), std::io::Error> {
    let kuzu_root = Path::new("../..");
    let mut kuzu_cmake = Config::new(kuzu_root);
    // TODO: Document environment variables better
    // TODO: Auto detection of system arrow?

    match arrow_mode() {
        ArrowMode::Bundled => {
            let arrow_cmake_root = Config::new(kuzu_root.join("external")).build();
            kuzu_cmake.define("ARROW_INSTALL", &arrow_cmake_root);
        }
        ArrowMode::Path(arrow_install) => {
            let arrow_install = Path::new(&arrow_install);
            println!(
                "cargo:rustc-link-search=native={}",
                arrow_install.join("lib").display()
            );
            println!(
                "cargo:rustc-link-search=native={}",
                arrow_install.join("lib64").display()
            );
        }
        ArrowMode::System => {
            kuzu_cmake.define("USE_SYSTEM_ARROW", "ON");
        }
    }
    let kuzu_cmake_root = kuzu_cmake.build();
    let kuzu_build_root = kuzu_cmake_root.join("build");
    let kuzu_lib_path = kuzu_build_root.join("src");

    println!("cargo:rustc-link-search=native={}", kuzu_lib_path.display());

    let include_paths = vec![
        Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("include"),
        kuzu_root.join("src/include"),
        kuzu_root.join("third_party/nlohmann_json"),
        kuzu_root.join("third_party/spdlog"),
    ];
    for dir in ["utf8proc", "antlr4_cypher", "antlr4_runtime", "re2"] {
        let lib_path = kuzu_build_root
            .join(format!("third_party/{dir}"))
            .canonicalize()
            .expect(&format!(
                "Could not find {}/third_party/{dir}",
                kuzu_build_root.display()
            ));
        println!("cargo:rustc-link-search=native={}", lib_path.display());
    }

    println!("cargo:rustc-link-lib={}=kuzu", link_mode());
    if link_mode() == "static" {
        println!("cargo:rustc-link-lib=dylib=stdc++");

        println!("cargo:rustc-link-lib=static=utf8proc");
        println!("cargo:rustc-link-lib=static=antlr4_cypher");
        println!("cargo:rustc-link-lib=static=antlr4_runtime");
        println!("cargo:rustc-link-lib=static=re2");

        match arrow_mode() {
            ArrowMode::Bundled | ArrowMode::Path(_) => {
                println!("cargo:rustc-link-lib=static=arrow_bundled_dependencies");
                println!("cargo:rustc-link-lib=static=parquet");
                println!("cargo:rustc-link-lib=static=arrow");
            }
            ArrowMode::System => {
                println!("cargo:rustc-link-lib=dylib=zstd");
                println!("cargo:rustc-link-lib=dylib=z");
                println!("cargo:rustc-link-lib=dylib=snappy");
                println!("cargo:rustc-link-lib=dylib=lz4");
                println!("cargo:rustc-link-lib=dylib=thrift");
                println!("cargo:rustc-link-lib=dylib=parquet");
                println!("cargo:rustc-link-lib=dylib=arrow");
            }
        }
    }
    println!("cargo:rerun-if-env-changed=KUZU_SHARED");

    println!("cargo:rerun-if-changed=include/kuzu_rs.h");
    println!("cargo:rerun-if-changed=include/kuzu_rs.cpp");

    cxx_build::bridge("src/ffi.rs")
        .file("src/kuzu_rs.cpp")
        .flag_if_supported("-std=c++20")
        .includes(include_paths)
        .compile("kuzu_rs");

    Ok(())
}
