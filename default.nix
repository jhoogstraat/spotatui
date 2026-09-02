{ pkgs ? import <nixpkgs> {} }:

  pkgs.rustPlatform.buildRustPackage rec {
    pname = "spotatui";
    version = "0.34.3";

  src = pkgs.lib.cleanSource ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  nativeBuildInputs = with pkgs; [
    pkg-config
    patchelf
    llvmPackages.clang
    llvmPackages.libclang
  ];

  buildInputs = with pkgs; [
    openssl
    alsa-lib
    dbus
  ];

  postFixup = ''
  patchelf \
    --set-rpath "${pkgs.lib.makeLibraryPath [
      pkgs.openssl
      pkgs.alsa-lib
      pkgs.dbus
    ]}" \
    $out/bin/spotatui
  '';

  meta = with pkgs.lib; {
    description = "Terminal UI Spotify client";
    homepage = "https://github.com/LargeModGames/spotatui";
    license = licenses.mit;
    mainProgram = "spotatui";
    platforms = platforms.linux;
  };
}
