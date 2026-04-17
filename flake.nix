{
  description = "";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustNightly = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" ];
        });
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            rustNightly
            pkgs.pkg-config
            (pkgs.python3.withPackages (ps: with ps; [
              pandas
              matplotlib
            ]))
          ];

          shellHook = ''
            echo "Loaded Rust Nightly: $(rustc --version)"
          '';
        };
      }
    );
}
