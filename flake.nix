{
  description = "Relay — native media for Discord";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = {nixpkgs, ...}: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
    libdave = pkgs.stdenv.mkDerivation {
      pname = "libdave";
      version = "1.2.2";
      src = pkgs.fetchurl {
        url = "https://github.com/discord/libdave/releases/download/v1.2.2/cpp/libdave-Linux-X64-boringssl.zip";
        hash = "sha256-6WiLo6GwkI5eeqA39ACi9ZxpyYZ3qDqFftDyhJwzgxI=";
      };
      nativeBuildInputs = [ pkgs.unzip pkgs.autoPatchelfHook ];
      buildInputs = [ pkgs.stdenv.cc.cc.lib ];
      sourceRoot = ".";
      dontBuild = true;
      installPhase = ''
        runHook preInstall
        mkdir -p $out
        cp -r lib include $out/
        mkdir -p $out/share/licenses/libdave
        cp -r licenses/* $out/share/licenses/libdave/
        runHook postInstall
      '';
      meta.platforms = [ system ];
    };
    # PipeWire 1.6.9 predates the merged keepalive fix in !3096.
    pipewire = pkgs.pipewire.overrideAttrs (old: {
      patches = (old.patches or [ ]) ++ [ (pkgs.fetchpatch {
        url = "https://gitlab.freedesktop.org/pipewire/pipewire/-/commit/2604cfebeabae8589132e6b1d406575c22e6abac.patch";
        hash = "sha256-LvnhZD7lUJVQ5WSCWkIzzqRk5VWMdr2S/2AtCUgD+lI=";
      }) ./pipewire-keepalive.patch ];
    });
    runtimeLibraries = with pkgs; [dbus gst_all_1.gstreamer gst_all_1.gst-plugins-base gst_all_1.gst-plugins-bad libdave pkgs.pipewire];
    pluginPath = pkgs.lib.makeSearchPath "lib/gstreamer-1.0" (with pkgs.gst_all_1; [
      (pkgs.lib.getLib gstreamer) gst-plugins-base gst-plugins-good gst-plugins-ugly gst-plugins-bad pipewire
    ]);
    mediaEnv = {
      CPLUS_INCLUDE_PATH = "${libdave}/include";
      GST_PLUGIN_SYSTEM_PATH_1_0 = pluginPath;
      LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath runtimeLibraries;
    };
    companion = pkgs.rustPlatform.buildRustPackage {
      pname = "relay";
      version = "0.1.0";
      src = pkgs.lib.fileset.toSource {
        root = ./.;
        fileset = pkgs.lib.fileset.unions [./Cargo.toml ./Cargo.lock ./build.rs ./src];
      };
      cargoLock.lockFile = ./Cargo.lock;
      nativeBuildInputs = [pkgs.pkg-config pkgs.makeWrapper];
      buildInputs = runtimeLibraries;
      env = mediaEnv;
      doCheck = false;
      postInstall = ''
        mkdir -p $out/share/licenses/relay
        cp -r ${libdave}/share/licenses/libdave $out/share/licenses/relay/
      '';
      postFixup = ''
        wrapProgram $out/bin/relay \
          --prefix LD_LIBRARY_PATH : ${mediaEnv.LD_LIBRARY_PATH} \
          --prefix GST_PLUGIN_SYSTEM_PATH_1_0 : ${mediaEnv.GST_PLUGIN_SYSTEM_PATH_1_0}
      '';
      meta.mainProgram = "relay";
    };
    plugin = pkgs.runCommand "relay-plugin" {nativeBuildInputs = [pkgs.typescript];} ''
      mkdir -p $out
      cp ${./plugin/index.ts} $out/index.ts
      cp ${./plugin/share.tsx} $out/share.tsx
      cp ${./plugin/share.css} $out/share.css
      cp ${./plugin}/playback.ts ${./plugin}/audio-bridge.ts ${./plugin}/video-worker.ts .
      tsc --declaration --strict false --skipLibCheck --target ES2024 --module ESNext --lib ES2024,DOM,DOM.Iterable \
        --outDir $out playback.ts
      tsc --target ES2024 --module ESNext --lib ES2024,DOM,DOM.Iterable \
        --outDir $out audio-bridge.ts video-worker.ts
    '';
  in {
    packages.${system} = {default = companion; inherit companion plugin;};
    devShells.${system}.default = pkgs.mkShell (mediaEnv // {
      packages = [pkgs.cargo pkgs.rustc pkgs.rustfmt pkgs.clippy pkgs.pkg-config pkgs.nodejs pkgs.typescript];
      buildInputs = runtimeLibraries;
    });
    nixosModules.default = {config, lib, ...}: let
      cfg = config.services.relay;
    in {
      options.services.relay = {
        enable = lib.mkEnableOption "Relay's native media companion";
        package = lib.mkOption {type = lib.types.package; default = companion; description = "Companion package";};
      };
      config = lib.mkIf cfg.enable {
        environment.systemPackages = [cfg.package];
        systemd.user.services.relay = {
          description = "Relay media companion";
          wantedBy = ["graphical-session.target"];
          after = ["graphical-session.target" "pipewire.service"];
          partOf = ["graphical-session.target"];
          serviceConfig = {
            ExecStart = lib.getExe cfg.package;
            Restart = "on-failure";
            RestartSec = 2;
            UMask = "0077";
          };
        };
      };
    };
  };
}
